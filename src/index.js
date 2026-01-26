const express = require('express');
const compression = require('compression');
const path = require('path');
const fs = require('fs');
const geoip = require('geoip-lite');
const h3 = require('h3-js');
require('dotenv').config();

const ClickHouseClient = require('./clickhouse-client');
const RegionService = require('./region-service');

// =============================================================================
// H3 Swarm Configuration
// =============================================================================
// H3 resolution 6 = ~36km² hexagons (~10km edge-to-edge)
// Good for temperature/humidity where conditions are similar over this distance
const H3_RESOLUTION = 6;

// Minimum sensors needed for peer verification
const MIN_SWARM_SIZE = 5;

// Freshness thresholds per data source (in milliseconds)
// Only sensors reporting within these thresholds are considered "active" for swarm membership
const FRESHNESS_THRESHOLDS = {
    'WESENSE': 10 * 60 * 1000,              // 10 minutes
    'MESHTASTIC_PUBLIC': 61 * 60 * 1000,    // 61 minutes
    'MESHTASTIC_COMMUNITY': 61 * 60 * 1000, // 61 minutes
    'default': 10 * 60 * 1000               // Conservative default
};

/**
 * Check if a sensor reading is fresh based on data source
 */
function isSensorFresh(timestamp, dataSource) {
    if (!timestamp) return false;
    const threshold = FRESHNESS_THRESHOLDS[dataSource] || FRESHNESS_THRESHOLDS.default;
    const readingTime = new Date(timestamp).getTime();
    const now = Date.now();
    return (now - readingTime) <= threshold;
}

// =============================================================================
// Swarm Statistics Helpers
// =============================================================================

/**
 * Calculate median of an array of numbers
 */
function calculateMedian(values) {
    if (!values || values.length === 0) return null;
    const sorted = [...values].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 !== 0
        ? sorted[mid]
        : (sorted[mid - 1] + sorted[mid]) / 2;
}

/**
 * Calculate IQR (Interquartile Range) and identify outliers
 * Returns { q1, q3, iqr, lowerBound, upperBound, outlierIndices }
 */
function calculateIQROutliers(values) {
    if (!values || values.length < 4) {
        return { q1: null, q3: null, iqr: null, lowerBound: null, upperBound: null, outlierIndices: [] };
    }

    const sorted = [...values].sort((a, b) => a - b);
    const n = sorted.length;

    // Calculate Q1 (25th percentile) and Q3 (75th percentile)
    const q1Index = Math.floor(n * 0.25);
    const q3Index = Math.floor(n * 0.75);
    const q1 = sorted[q1Index];
    const q3 = sorted[q3Index];
    const iqr = q3 - q1;

    // Outlier bounds: 1.5 * IQR
    const lowerBound = q1 - 1.5 * iqr;
    const upperBound = q3 + 1.5 * iqr;

    // Find outlier indices (in original array)
    const outlierIndices = [];
    values.forEach((val, idx) => {
        if (val < lowerBound || val > upperBound) {
            outlierIndices.push(idx);
        }
    });

    return { q1, q3, iqr, lowerBound, upperBound, outlierIndices };
}

/**
 * Calculate swarm statistics for a group of sensors
 * @param {Array} sensors - Array of sensor objects with readings
 * @param {string} metric - The metric to analyze (e.g., 'temperature')
 * @returns {Object} Swarm statistics including median, outliers, etc.
 */
function calculateSwarmStats(sensors, metric) {
    if (!sensors || sensors.length < MIN_SWARM_SIZE) {
        return null;
    }

    // Extract values for this metric
    const valuesWithIndex = sensors
        .map((s, idx) => ({
            idx,
            deviceId: s.deviceId,
            value: s.readings?.[metric]?.value
        }))
        .filter(v => v.value != null);

    if (valuesWithIndex.length < MIN_SWARM_SIZE) {
        return null;
    }

    const values = valuesWithIndex.map(v => v.value);
    const median = calculateMedian(values);
    const iqrData = calculateIQROutliers(values);

    // Map outlier indices back to device IDs
    const outlierDeviceIds = iqrData.outlierIndices.map(idx => valuesWithIndex[idx].deviceId);

    return {
        metric,
        sensor_count: valuesWithIndex.length,
        median: median,
        q1: iqrData.q1,
        q3: iqrData.q3,
        iqr: iqrData.iqr,
        lower_bound: iqrData.lowerBound,
        upper_bound: iqrData.upperBound,
        outlier_count: outlierDeviceIds.length,
        outlier_device_ids: outlierDeviceIds
    };
}

// Initialise ClickHouse client
const clickHouseClient = new ClickHouseClient();
clickHouseClient.connect();

// Initialise Region service
const regionService = new RegionService(clickHouseClient);

// =============================================================================
// Pre-computed Regional Data Cache
// =============================================================================
// Instead of computing on each request, we pre-compute all regional aggregates
// in the background and serve instantly to all users.
//
// Structure: precomputedRegions[adminLevel][metric][deploymentFilter][timeWindow] = { regions: {...}, timestamp, ... }
// Time windows: Configured via PRECOMPUTE_TIME_WINDOWS (default: 30m, 1h, 2h, 4h, 24h)
// Deployment types: OUTDOOR, INDOOR, PORTABLE, MIXED (from ESP32 sensorarray or inferred from Meshtastic node names)
// Pre-computed filters: 'outdoor', 'indoor', 'all' (portable/mixed are rare, computed on-demand)
// =============================================================================
const precomputedRegions = {
    0: {},  // ADM0 (countries)
    1: {},  // ADM1 (states/regions)
    2: {},  // ADM2 (districts)
    3: {},  // ADM3 (sub-districts) - 81 countries
    4: {}   // ADM4 (localities) - 21 countries
};

// Pre-computed Sensor Leaderboards (ADM2, 24h)
let precomputedLeaderboard = {
    byNodes: null,      // Towns with most sensor nodes (devices)
    bySensors: null,    // Towns with most sensors (device + reading_type pairs)
    byTypes: null,      // Towns with most sensor types
    total_regions: 0,
    timestamp: null
};

// Pre-computed Environmental Leaderboards (ADM2, 30d)
let precomputedEnvLeaderboard = {
    cleanestAir: [],
    bestWeather: [],
    mostStable: [],
    hottest: [],
    timestamp: null
};

const PRECOMPUTE_INTERVAL = 5 * 60 * 1000; // Refresh every 5 minutes

// =============================================================================
// Active Viewer Tracking
// =============================================================================
// Track active viewers via heartbeat. Each client sends a ping every 30s.
// A viewer is considered "online" if they've pinged in the last 60s.
// =============================================================================
const VIEWER_TIMEOUT = 60 * 1000; // 60 seconds
const activeViewers = new Map(); // viewerId -> lastSeen timestamp
const METRICS = ['temperature', 'humidity', 'pressure', 'co2', 'pm2_5', 'pm10', 'voc_index', 'nox_index'];
const ADMIN_LEVELS = [0, 1, 2, 3, 4];  // ADM3: 81 countries, ADM4: 21 countries
const DEPLOYMENT_FILTERS = ['outdoor', 'all', 'indoor'];  // outdoor first as it's the default
const DEFAULT_TIME_RANGE = '1h';  // Default for API requests

// Configurable time windows to precompute (env var allows disabling for resource-constrained deployments)
// Default: precompute all common windows for instant responses
// Set to empty string to disable precompute (all queries on-demand)
// Set to "24h" for minimal precompute (just 24h)
const PRECOMPUTE_TIME_WINDOWS = (process.env.PRECOMPUTE_TIME_WINDOWS || '30m,1h,2h,4h,24h')
    .split(',')
    .map(w => w.trim())
    .filter(w => w);

const CACHE_FILE_PATH = process.env.CACHE_PATH || path.join(__dirname, '../data/region-cache.json');

// Prioritized task order: most common queries first (outdoor temperature/humidity at ADM0/ADM1)
// Each task now includes deployment filter (outdoor is default and prioritized first)
// ADM3 (81 countries) and ADM4 (21 countries) are lower priority as they're only used at high zoom
const PRIORITIZED_TASKS = [
    // High priority - outdoor (default view) for most common metrics
    { adminLevel: 0, metric: 'temperature', deployment: 'outdoor' },
    { adminLevel: 1, metric: 'temperature', deployment: 'outdoor' },
    { adminLevel: 0, metric: 'humidity', deployment: 'outdoor' },
    { adminLevel: 1, metric: 'humidity', deployment: 'outdoor' },
    { adminLevel: 2, metric: 'temperature', deployment: 'outdoor' },
    { adminLevel: 2, metric: 'humidity', deployment: 'outdoor' },
    // Then 'all' filter for comparison
    { adminLevel: 0, metric: 'temperature', deployment: 'all' },
    { adminLevel: 1, metric: 'temperature', deployment: 'all' },
    { adminLevel: 2, metric: 'temperature', deployment: 'all' },
    { adminLevel: 0, metric: 'humidity', deployment: 'all' },
    { adminLevel: 1, metric: 'humidity', deployment: 'all' },
    { adminLevel: 2, metric: 'humidity', deployment: 'all' },
    // ADM3/ADM4 for primary metrics (used at high zoom, lower priority)
    { adminLevel: 3, metric: 'temperature', deployment: 'outdoor' },
    { adminLevel: 3, metric: 'humidity', deployment: 'outdoor' },
    { adminLevel: 4, metric: 'temperature', deployment: 'outdoor' },
    { adminLevel: 4, metric: 'humidity', deployment: 'outdoor' },
    // Outdoor pressure
    { adminLevel: 0, metric: 'pressure', deployment: 'outdoor' },
    { adminLevel: 1, metric: 'pressure', deployment: 'outdoor' },
    { adminLevel: 2, metric: 'pressure', deployment: 'outdoor' },
    // Less common metrics - outdoor only (indoor rarely queried for these)
    { adminLevel: 0, metric: 'co2', deployment: 'outdoor' },
    { adminLevel: 1, metric: 'co2', deployment: 'outdoor' },
    { adminLevel: 2, metric: 'co2', deployment: 'outdoor' },
    { adminLevel: 0, metric: 'pm2_5', deployment: 'outdoor' },
    { adminLevel: 1, metric: 'pm2_5', deployment: 'outdoor' },
    { adminLevel: 2, metric: 'pm2_5', deployment: 'outdoor' },
    // Indoor filter (least common, computed last)
    { adminLevel: 0, metric: 'temperature', deployment: 'indoor' },
    { adminLevel: 1, metric: 'temperature', deployment: 'indoor' },
    { adminLevel: 2, metric: 'temperature', deployment: 'indoor' },
    { adminLevel: 0, metric: 'humidity', deployment: 'indoor' },
    { adminLevel: 1, metric: 'humidity', deployment: 'indoor' },
    { adminLevel: 2, metric: 'humidity', deployment: 'indoor' },
    // Fill in remaining outdoor metrics
    { adminLevel: 0, metric: 'pm10', deployment: 'outdoor' },
    { adminLevel: 1, metric: 'pm10', deployment: 'outdoor' },
    { adminLevel: 2, metric: 'pm10', deployment: 'outdoor' },
    { adminLevel: 0, metric: 'voc_index', deployment: 'outdoor' },
    { adminLevel: 1, metric: 'voc_index', deployment: 'outdoor' },
    { adminLevel: 2, metric: 'voc_index', deployment: 'outdoor' },
    { adminLevel: 0, metric: 'nox_index', deployment: 'outdoor' },
    { adminLevel: 1, metric: 'nox_index', deployment: 'outdoor' },
    { adminLevel: 2, metric: 'nox_index', deployment: 'outdoor' },
    // ADM3/ADM4 for secondary metrics
    { adminLevel: 3, metric: 'pressure', deployment: 'outdoor' },
    { adminLevel: 4, metric: 'pressure', deployment: 'outdoor' },
    { adminLevel: 3, metric: 'co2', deployment: 'outdoor' },
    { adminLevel: 4, metric: 'co2', deployment: 'outdoor' },
    { adminLevel: 3, metric: 'pm2_5', deployment: 'outdoor' },
    { adminLevel: 4, metric: 'pm2_5', deployment: 'outdoor' },
    // Remaining 'all' filter for other metrics
    { adminLevel: 0, metric: 'pressure', deployment: 'all' },
    { adminLevel: 1, metric: 'pressure', deployment: 'all' },
    { adminLevel: 2, metric: 'pressure', deployment: 'all' },
    { adminLevel: 0, metric: 'co2', deployment: 'all' },
    { adminLevel: 1, metric: 'co2', deployment: 'all' },
    { adminLevel: 2, metric: 'co2', deployment: 'all' },
    // ADM3/ADM4 'all' filter
    { adminLevel: 3, metric: 'temperature', deployment: 'all' },
    { adminLevel: 3, metric: 'humidity', deployment: 'all' },
    { adminLevel: 4, metric: 'temperature', deployment: 'all' },
    { adminLevel: 4, metric: 'humidity', deployment: 'all' },
];

let lastRefreshTime = null;
let refreshInProgress = false;

/**
 * Load pre-computed data from disk cache (survives restarts)
 */
function loadCacheFromDisk() {
    try {
        if (fs.existsSync(CACHE_FILE_PATH)) {
            const data = JSON.parse(fs.readFileSync(CACHE_FILE_PATH, 'utf8'));

            // Restore the cache structure
            for (const adminLevel of ADMIN_LEVELS) {
                if (data[adminLevel]) {
                    precomputedRegions[adminLevel] = data[adminLevel];
                }
            }

            // Restore leaderboard cache
            if (data._leaderboard) {
                precomputedLeaderboard = data._leaderboard;
            }

            lastRefreshTime = data._meta?.lastRefreshTime || null;
            const age = lastRefreshTime ? Math.round((Date.now() - lastRefreshTime) / 1000 / 60) : '?';
            const totalRegions = Object.values(precomputedRegions)
                .flatMap(level => Object.values(level))
                .reduce((sum, m) => sum + Object.keys(m.regions || {}).length, 0);

            console.log(`Loaded region cache from disk (${totalRegions} regions, ${age} minutes old)`);
            return true;
        }
    } catch (error) {
        console.error('Failed to load region cache from disk:', error.message);
    }
    return false;
}

/**
 * Save pre-computed data to disk cache
 */
function saveCacheToDisk() {
    try {
        // Ensure data directory exists
        const dataDir = path.dirname(CACHE_FILE_PATH);
        if (!fs.existsSync(dataDir)) {
            fs.mkdirSync(dataDir, { recursive: true });
        }

        const data = {
            ...precomputedRegions,
            _leaderboard: precomputedLeaderboard,
            _meta: {
                lastRefreshTime,
                savedAt: Date.now()
            }
        };

        fs.writeFileSync(CACHE_FILE_PATH, JSON.stringify(data));
        console.log('Region cache saved to disk');
    } catch (error) {
        console.error('Failed to save region cache to disk:', error.message);
    }
}

/**
 * Generate prioritized tasks for precomputation
 * Includes time window dimension when PRECOMPUTE_TIME_WINDOWS is configured
 */
function generatePrecomputeTasks() {
    const tasks = [];

    // If no time windows configured, skip precompute entirely
    if (PRECOMPUTE_TIME_WINDOWS.length === 0) {
        console.log('[PRECOMPUTE] No time windows configured - all queries will be on-demand');
        return tasks;
    }

    // Prioritized combinations (most common queries first)
    const prioritizedCombos = [
        // High priority - outdoor temp/humidity at all levels
        { adminLevel: 0, metric: 'temperature', deployment: 'outdoor' },
        { adminLevel: 1, metric: 'temperature', deployment: 'outdoor' },
        { adminLevel: 2, metric: 'temperature', deployment: 'outdoor' },
        { adminLevel: 0, metric: 'humidity', deployment: 'outdoor' },
        { adminLevel: 1, metric: 'humidity', deployment: 'outdoor' },
        { adminLevel: 2, metric: 'humidity', deployment: 'outdoor' },
        // All filter for comparison
        { adminLevel: 0, metric: 'temperature', deployment: 'all' },
        { adminLevel: 1, metric: 'temperature', deployment: 'all' },
        { adminLevel: 2, metric: 'temperature', deployment: 'all' },
        // ADM3/4 for primary metrics
        { adminLevel: 3, metric: 'temperature', deployment: 'outdoor' },
        { adminLevel: 3, metric: 'humidity', deployment: 'outdoor' },
        { adminLevel: 4, metric: 'temperature', deployment: 'outdoor' },
        // Other outdoor metrics
        { adminLevel: 0, metric: 'pressure', deployment: 'outdoor' },
        { adminLevel: 1, metric: 'pressure', deployment: 'outdoor' },
        { adminLevel: 2, metric: 'pressure', deployment: 'outdoor' },
        { adminLevel: 0, metric: 'co2', deployment: 'outdoor' },
        { adminLevel: 1, metric: 'co2', deployment: 'outdoor' },
        { adminLevel: 2, metric: 'co2', deployment: 'outdoor' },
        { adminLevel: 0, metric: 'pm2_5', deployment: 'outdoor' },
        { adminLevel: 1, metric: 'pm2_5', deployment: 'outdoor' },
        { adminLevel: 2, metric: 'pm2_5', deployment: 'outdoor' },
        // Indoor temp/humidity
        { adminLevel: 0, metric: 'temperature', deployment: 'indoor' },
        { adminLevel: 1, metric: 'temperature', deployment: 'indoor' },
        { adminLevel: 2, metric: 'temperature', deployment: 'indoor' },
    ];

    // Generate tasks for each time window × combo
    // Time windows ordered by priority (shorter = more common)
    const windowPriority = ['1h', '30m', '2h', '4h', '24h'];
    const orderedWindows = PRECOMPUTE_TIME_WINDOWS.sort((a, b) => {
        const aIdx = windowPriority.indexOf(a);
        const bIdx = windowPriority.indexOf(b);
        return (aIdx === -1 ? 99 : aIdx) - (bIdx === -1 ? 99 : bIdx);
    });

    for (const timeWindow of orderedWindows) {
        for (const combo of prioritizedCombos) {
            tasks.push({ ...combo, timeWindow });
        }
    }

    return tasks;
}

/**
 * Pre-compute all regional aggregates for all metrics, admin levels, and time windows
 * Tasks are prioritized: temperature/humidity at ADM0/ADM1 first (most common)
 */
async function refreshAllRegions() {
    if (refreshInProgress) {
        console.log('Region refresh already in progress, skipping...');
        return;
    }

    refreshInProgress = true;
    const startTime = Date.now();

    // Generate tasks dynamically based on configured time windows
    const tasks = generatePrecomputeTasks();
    const totalTasks = tasks.length;

    if (totalTasks === 0) {
        console.log('[PRECOMPUTE] Skipping - no tasks configured');
        refreshInProgress = false;
        return;
    }

    let completedTasks = 0;
    let successCount = 0;
    let failCount = 0;

    // Step 1: Refresh device-to-region cache (only processes new/moved devices)
    // This must run before precomputation since ADM1/ADM2 queries depend on it
    try {
        // Auto-detect and fix corrupted cache (caused by table schema column order mismatch)
        await regionService.detectAndFixCorruptedCache();

        console.log('\n--- Device Region Cache ---');
        const cacheResult = await regionService.refreshDeviceRegionCache();
        if (cacheResult.updated > 0) {
            console.log(`Updated ${cacheResult.updated} devices in ${cacheResult.duration_ms}ms`);
        }
    } catch (error) {
        console.error('Failed to refresh device region cache:', error.message);
        // Continue anyway - will use stale cache data
    }

    // Step 2: Pre-compute regional aggregates
    console.log(`\n--- Pre-computing Regions (${totalTasks} tasks) ---\n`);

    // Progress bar settings
    const barWidth = 30;
    const spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
    let spinnerIndex = 0;
    let spinnerInterval;

    // Function to render progress bar
    const renderProgressBar = (current, total, taskLabel, elapsed) => {
        const percent = Math.round((current / total) * 100);
        const filled = Math.round((current / total) * barWidth);
        const empty = barWidth - filled;
        const bar = '█'.repeat(filled) + '░'.repeat(empty);
        return `${spinner[spinnerIndex]} [${bar}] ${percent}% (${current}/${total}) ${taskLabel} (${elapsed}s)`;
    };

    try {
        for (const { adminLevel, metric, deployment, timeWindow } of tasks) {
            const taskLabel = `ADM${adminLevel} ${metric} (${deployment}) [${timeWindow}]`;

            // Start spinner for this task
            spinnerInterval = setInterval(() => {
                const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
                process.stdout.write(`\r${renderProgressBar(completedTasks, totalTasks, taskLabel, elapsed)}   `);
                spinnerIndex = (spinnerIndex + 1) % spinner.length;
            }, 100);

            try {
                const data = await regionService.precomputeRegions(metric, adminLevel, timeWindow, deployment);
                // Initialize nested structure if needed: [adminLevel][metric][deployment][timeWindow]
                if (!precomputedRegions[adminLevel][metric]) {
                    precomputedRegions[adminLevel][metric] = {};
                }
                if (!precomputedRegions[adminLevel][metric][deployment]) {
                    precomputedRegions[adminLevel][metric][deployment] = {};
                }
                precomputedRegions[adminLevel][metric][deployment][timeWindow] = {
                    ...data,
                    timestamp: Date.now()
                };
                clearInterval(spinnerInterval);
                successCount++;
            } catch (error) {
                clearInterval(spinnerInterval);
                // Log error on new line, then continue
                process.stdout.write(`\r${'░'.repeat(barWidth + 40)}\r`); // Clear line
                console.log(`✗ ${taskLabel}: ${error.message}`);
                failCount++;
            }
            completedTasks++;
        }

        // Final progress bar at 100%
        const finalElapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        const finalBar = '█'.repeat(barWidth);
        process.stdout.write(`\r✓ [${finalBar}] 100% (${totalTasks}/${totalTasks}) Complete (${finalElapsed}s)   \n`);

        // Also compute the Sensor Leaderboards (ADM2, 24h, top 5 each)
        try {
            console.log('Computing Sensor Leaderboards...');
            const [byNodes, bySensors, byTypes] = await Promise.all([
                regionService.getTopRegions(2, '24h', 5),
                regionService.getTopRegionsBySensors(2, '24h', 5),
                regionService.getTopRegionsBySensorTypes(2, '24h', 5)
            ]);
            precomputedLeaderboard = {
                byNodes: byNodes.regions,
                bySensors: bySensors.regions,
                byTypes: byTypes.regions,
                total_regions: byNodes.total_regions,
                timestamp: Date.now()
            };
            console.log(`Sensor Leaderboards computed: ${byNodes.total_regions} total regions`);
        } catch (error) {
            console.error('Failed to compute Sensor Leaderboards:', error.message);
        }

        // Compute Environmental Leaderboards (30-day data)
        try {
            console.log('Computing Environmental Leaderboards...');
            const envData = await regionService.getEnvironmentalLeaderboards(5);
            precomputedEnvLeaderboard = {
                ...envData,
                timestamp: Date.now()
            };
            const total = (envData.outdoorAir?.length || 0) + (envData.indoorAir?.length || 0) +
                         (envData.bestWeather?.length || 0) + (envData.mostStable?.length || 0) +
                         (envData.hottest?.length || 0);
            console.log(`Environmental Leaderboards computed: ${total} entries`);
        } catch (error) {
            console.error('Failed to compute Environmental Leaderboards:', error.message);
        }

        lastRefreshTime = Date.now();
        console.log(`\nPre-computation complete: ${successCount} succeeded, ${failCount} failed\n`);

        // Save to disk for fast startup next time
        if (successCount > 0) {
            saveCacheToDisk();
        }

    } catch (error) {
        if (spinnerInterval) clearInterval(spinnerInterval);
        console.error('\nError during region pre-computation:', error);
    } finally {
        refreshInProgress = false;
    }
}

/**
 * Get pre-computed data for a specific admin level, metric, deployment filter, and time window
 */
function getPrecomputedData(adminLevel, metric, deployment = 'outdoor', timeWindow = '1h') {
    return precomputedRegions[adminLevel]?.[metric]?.[deployment]?.[timeWindow] || null;
}

/**
 * Start the background refresh loop
 */
function startRegionRefreshLoop() {
    // Try to load cached data from disk first (instant availability on restart)
    const loadedFromDisk = loadCacheFromDisk();

    if (loadedFromDisk) {
        // Data loaded - still refresh in background to get fresh data
        console.log('Serving cached data while refreshing in background...');
        setTimeout(() => {
            refreshAllRegions();
        }, 1000);
    } else {
        // No cache - refresh immediately
        console.log('No disk cache found, computing fresh data...');
        setTimeout(() => {
            refreshAllRegions();
        }, 2000);
    }

    // Schedule periodic refreshes
    setInterval(() => {
        refreshAllRegions();
    }, PRECOMPUTE_INTERVAL);

    console.log(`Region pre-computation scheduled every ${PRECOMPUTE_INTERVAL / 1000}s`);
}

// Create Express app
const app = express();

// Middleware
// Exclude PMTiles from compression - it breaks HTTP Range requests required by PMTiles
app.use(compression({
    filter: (req, res) => {
        // Don't compress PMTiles - they need proper Range request support
        if (req.url.endsWith('.pmtiles')) {
            return false;
        }
        // Use default compression filter for everything else
        return compression.filter(req, res);
    }
}));
app.use(express.json());

// Serve static files with appropriate caching (per architecture Section 8.2)
// PMTiles uses HTTP range requests, so browser caches tile chunks efficiently
app.use(express.static(path.join(__dirname, '../public'), {
    setHeaders: (res, filePath) => {
        if (filePath.endsWith('.pmtiles')) {
            // PMTiles: aggressive caching (immutable, long TTL)
            res.set('Accept-Ranges', 'bytes');
            res.set('Access-Control-Allow-Origin', '*');
            res.set('Cache-Control', 'public, max-age=31536000, immutable');
        } else if (filePath.endsWith('.html')) {
            // HTML: no cache (may change frequently)
            res.set('Cache-Control', 'no-cache');
        } else if (filePath.endsWith('.js') || filePath.endsWith('.css')) {
            // JS/CSS: no cache during development for easier iteration
            res.set('Cache-Control', 'no-cache');
        }
    }
}));

// Routes
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, '../public/index.html'));
});

app.get('/api/sensors', async (req, res) => {
    try {
        // Respect the requested time range - only show sensors with data in that period
        // The dashboard has separate logic for showing stale sensors
        const timeRange = req.query.range || '24h';
        const rawData = await clickHouseClient.queryAverageData(timeRange);

        // rawData: { deviceId: { latitude, longitude, geo_country, geo_subdivision, node_name, readings: {...} }, ... }

        // Helper to get the latest timestamp from a sensor's readings
        const getLatestTimestamp = (readings) => {
            if (!readings) return null;
            let latest = null;
            for (const reading of Object.values(readings)) {
                if (reading.timestamp) {
                    const ts = new Date(reading.timestamp).getTime();
                    if (!latest || ts > latest) latest = ts;
                }
            }
            return latest ? new Date(latest).toISOString() : null;
        };

        // Step 1: Calculate H3 cell for each sensor and group FRESH sensors by cell
        // Separate groupings for different metrics:
        // - Temperature/Humidity: OUTDOOR sensors only
        // - Pressure: Indoor + Outdoor OK (pressure is uniform regardless of building envelope)
        const h3CellOutdoorSensors = {};  // h3Index -> array of FRESH OUTDOOR sensors (for temp/humidity)
        const h3CellAllFreshSensors = {}; // h3Index -> array of ALL FRESH sensors (for pressure)
        const h3CellAllSensors = {}; // h3Index -> array of ALL sensors (for display purposes)
        const sensorH3Cells = {}; // deviceId -> h3Index
        const sensorFreshness = {}; // deviceId -> { isFresh, timestamp }

        Object.entries(rawData).forEach(([deviceId, data]) => {
            if (data.latitude && data.longitude) {
                try {
                    const h3Index = h3.latLngToCell(data.latitude, data.longitude, H3_RESOLUTION);
                    sensorH3Cells[deviceId] = h3Index;

                    // Check freshness
                    const latestTs = getLatestTimestamp(data.readings);
                    const fresh = isSensorFresh(latestTs, data.data_source);
                    sensorFreshness[deviceId] = { isFresh: fresh, timestamp: latestTs };

                    // Treat 'mixed' same as 'outdoor' for swarm calculations
                    // Mixed sensors are typically in semi-outdoor environments (garages, covered areas, etc.)
                    const isOutdoor = data.deployment_type === 'outdoor' || data.deployment_type === 'mixed';

                    // Track all sensors in cell (for display)
                    if (!h3CellAllSensors[h3Index]) {
                        h3CellAllSensors[h3Index] = [];
                    }
                    h3CellAllSensors[h3Index].push({ deviceId, ...data, _isFresh: fresh, _latestTs: latestTs, _isOutdoor: isOutdoor });

                    // Only fresh sensors participate in swarm calculations
                    if (fresh) {
                        // All fresh sensors can contribute to pressure
                        if (!h3CellAllFreshSensors[h3Index]) {
                            h3CellAllFreshSensors[h3Index] = [];
                        }
                        h3CellAllFreshSensors[h3Index].push({ deviceId, ...data, _isOutdoor: isOutdoor });

                        // Only outdoor sensors contribute to temp/humidity
                        if (isOutdoor) {
                            if (!h3CellOutdoorSensors[h3Index]) {
                                h3CellOutdoorSensors[h3Index] = [];
                            }
                            h3CellOutdoorSensors[h3Index].push({ deviceId, ...data });
                        }
                    }
                } catch (e) {
                    // Invalid coordinates, skip H3 assignment
                }
            }
        });

        // Step 2: Calculate swarm statistics (median, outliers) for each H3 cell
        // Use different sensor pools per metric type
        const h3CellStats = {};  // h3Index -> { temperature: {...}, humidity: {...}, pressure: {...} }
        const outlierDeviceIds = new Set();  // Track all outlier device IDs

        // Get all unique H3 cells
        const allH3Cells = new Set([
            ...Object.keys(h3CellOutdoorSensors),
            ...Object.keys(h3CellAllFreshSensors)
        ]);

        allH3Cells.forEach(h3Index => {
            const outdoorSensors = h3CellOutdoorSensors[h3Index] || [];
            const allFreshSensors = h3CellAllFreshSensors[h3Index] || [];

            h3CellStats[h3Index] = {
                // Outdoor sensor counts for temp/humidity
                outdoor_count: outdoorSensors.length,
                // All fresh sensor count for pressure
                all_fresh_count: allFreshSensors.length
            };

            // Temperature: outdoor only, needs 5+ outdoor sensors
            if (outdoorSensors.length >= MIN_SWARM_SIZE) {
                const stats = calculateSwarmStats(outdoorSensors, 'temperature');
                if (stats) {
                    h3CellStats[h3Index].temperature = stats;
                    stats.outlier_device_ids.forEach(id => outlierDeviceIds.add(id));
                }
            }

            // Humidity: outdoor only, needs 5+ outdoor sensors
            if (outdoorSensors.length >= MIN_SWARM_SIZE) {
                const stats = calculateSwarmStats(outdoorSensors, 'humidity');
                if (stats) {
                    h3CellStats[h3Index].humidity = stats;
                    stats.outlier_device_ids.forEach(id => outlierDeviceIds.add(id));
                }
            }

            // Pressure: indoor + outdoor, needs 5+ total fresh sensors
            if (allFreshSensors.length >= MIN_SWARM_SIZE) {
                const stats = calculateSwarmStats(allFreshSensors, 'pressure');
                if (stats) {
                    h3CellStats[h3Index].pressure = stats;
                    stats.outlier_device_ids.forEach(id => outlierDeviceIds.add(id));
                }
            }
        });

        // Step 3: Build sensor list with swarm data and outlier flags
        const sensors = Object.entries(rawData).map(([deviceId, data]) => {
            const h3Index = sensorH3Cells[deviceId];
            const cellStats = h3Index ? h3CellStats[h3Index] : null;
            const isOutdoor = data.deployment_type === 'outdoor' || data.deployment_type === 'mixed';

            // Calculate swarm sizes per metric type
            // Temperature/Humidity: outdoor sensors only
            // Pressure: all fresh sensors (indoor + outdoor)
            const outdoorSwarmSize = cellStats?.outdoor_count || 0;
            const pressureSwarmSize = cellStats?.all_fresh_count || 0;

            // Per-metric swarm info for frontend
            const swarmSizes = {
                temperature: outdoorSwarmSize,
                humidity: outdoorSwarmSize,
                pressure: pressureSwarmSize
            };

            // Determine overall swarm status based on the BEST metric for this sensor
            // For outdoor sensors: consider temp/humidity/pressure
            // For indoor sensors: only pressure can give swarm status
            let bestSwarmSize = isOutdoor
                ? Math.max(outdoorSwarmSize, pressureSwarmSize)
                : pressureSwarmSize;

            let swarmStatus = 'shield';
            let swarmIcon = '🛡️';
            if (bestSwarmSize >= 7) {
                swarmStatus = 'super_swarm';
                swarmIcon = '⭐';
            } else if (bestSwarmSize >= 5) {
                swarmStatus = 'swarm';
                swarmIcon = '🐝';
            }

            // Check if this sensor is an outlier for any metric
            const isOutlier = outlierDeviceIds.has(deviceId);
            const outlierMetrics = [];
            if (isOutlier && cellStats) {
                ['temperature', 'humidity', 'pressure'].forEach(metric => {
                    if (cellStats[metric]?.outlier_device_ids?.includes(deviceId)) {
                        outlierMetrics.push(metric);
                    }
                });
            }

            // Build list of OTHER sensors in the same swarm (for sidebar display)
            // Show all sensors in the cell with their freshness and deployment type
            let swarmSensors = null;
            const allCellSensors = h3Index ? h3CellAllSensors[h3Index] : [];
            // Show peers if ANY metric has swarm (5+)
            const hasAnySwarm = outdoorSwarmSize >= MIN_SWARM_SIZE || pressureSwarmSize >= MIN_SWARM_SIZE;
            if (hasAnySwarm && allCellSensors.length > 0) {
                swarmSensors = allCellSensors
                    .filter(s => s.deviceId !== deviceId)  // Exclude self
                    .map(s => ({
                        deviceId: s.deviceId,
                        name: s.node_name || s.deployment_location || s.deviceId,
                        readings: {
                            temperature: s.readings?.temperature?.value,
                            humidity: s.readings?.humidity?.value,
                            pressure: s.readings?.pressure?.value
                        },
                        is_outlier: outlierDeviceIds.has(s.deviceId),
                        is_fresh: s._isFresh,
                        is_outdoor: s._isOutdoor,
                        timestamp: s._latestTs,
                        data_source: s.data_source,
                        deployment_type: s.deployment_type
                    }));
            }

            // Get this sensor's freshness info
            const freshInfo = sensorFreshness[deviceId] || { isFresh: false, timestamp: null };

            return {
                deviceId: deviceId,
                name: data.node_name || data.deployment_location || deviceId,
                latitude: data.latitude,
                longitude: data.longitude,
                country: data.geo_country,
                subdivision: data.geo_subdivision,
                locality: data.deployment_location,
                readings: data.readings || {},
                board_model: data.board_model,
                data_source: data.data_source,
                deployment_type: data.deployment_type,
                // Freshness info
                is_fresh: freshInfo.isFresh,
                latest_timestamp: freshInfo.timestamp,
                // Swarm data (overall best for this sensor)
                h3_cell: h3Index || null,
                swarm_size: bestSwarmSize,
                swarm_status: swarmStatus,
                swarm_icon: swarmIcon,
                // Per-metric swarm sizes (for frontend to show per-card status)
                // Temperature/Humidity: outdoor sensors only
                // Pressure: indoor + outdoor
                swarm_sizes: swarmSizes,
                // Outlier detection (only for sensors in 5+ swarms)
                is_outlier: isOutlier,
                outlier_metrics: outlierMetrics,
                // Swarm medians (for sensors in verified swarms, per metric)
                swarm_medians: cellStats ? {
                    temperature: cellStats.temperature?.median,
                    humidity: cellStats.humidity?.median,
                    pressure: cellStats.pressure?.median
                } : null,
                // Other sensors in the same swarm (for sidebar display)
                swarm_sensors: swarmSensors
            };
        });

        console.log(`Fetched ${sensors.length} sensors from ClickHouse`);

        // Debug: Log specific sensors if present
        const debugIds = ['!e2e5a154', '!0b9ca93f'];
        debugIds.forEach(id => {
            const sensor = sensors.find(s => s.deviceId === id);
            if (sensor) {
                console.log(`  DEBUG ${id}: h3=${sensor.h3_cell}, fresh=${sensor.is_fresh}, swarm_size=${sensor.swarm_size}, sizes=${JSON.stringify(sensor.swarm_sizes)}`);
            } else {
                // Check if it exists in rawData but wasn't included
                const rawSensor = rawData[id];
                if (rawSensor) {
                    const h3 = sensorH3Cells[id];
                    const fresh = sensorFreshness[id];
                    console.log(`  DEBUG ${id}: h3=${h3}, fresh=${fresh?.isFresh}, not in sensors list`);
                } else {
                    console.log(`  DEBUG ${id}: not found in rawData (no data in time range)`);
                }
            }
        });

        // Log swarm statistics
        const outdoorVerifiedCells = Object.values(h3CellStats).filter(s => s.outdoor_count >= MIN_SWARM_SIZE).length;
        const pressureVerifiedCells = Object.values(h3CellStats).filter(s => s.all_fresh_count >= MIN_SWARM_SIZE).length;
        const swarmStats = {
            total: sensors.length,
            shield: sensors.filter(s => s.swarm_status === 'shield').length,
            swarm: sensors.filter(s => s.swarm_status === 'swarm').length,
            super_swarm: sensors.filter(s => s.swarm_status === 'super_swarm').length,
            unique_cells: Object.keys(h3CellAllSensors).length,
            outdoor_verified_cells: outdoorVerifiedCells,
            pressure_verified_cells: pressureVerifiedCells,
            outlier_count: outlierDeviceIds.size
        };
        console.log(`  Swarm stats: ${swarmStats.shield} shield, ${swarmStats.swarm} swarm, ${swarmStats.super_swarm} super swarm`);
        console.log(`  H3 cells: ${swarmStats.unique_cells} total`);
        console.log(`  Verified cells: ${outdoorVerifiedCells} outdoor (temp/humidity), ${pressureVerifiedCells} pressure (indoor+outdoor)`);
        if (swarmStats.outlier_count > 0) {
            console.log(`  Outliers detected: ${swarmStats.outlier_count} sensors`);
        }

        return res.json({ sensors, swarm_stats: swarmStats });
    } catch (error) {
        console.error('Error fetching sensors from ClickHouse:', error);
        return res.json({ sensors: [] });
    }
});

app.get('/api/sensors/average', async (req, res) => {
    try {
        const timeRange = req.query.range || '24h';
        const rawData = await clickHouseClient.queryAverageData(timeRange);
        res.json(rawData);
    } catch (error) {
        console.error('Error fetching sensor averages:', error);
        res.status(500).json({ error: 'Failed to query sensor averages' });
    }
});

// Get all known devices (for Dashboard overview - not time-limited)
app.get('/api/devices', async (req, res) => {
    try {
        const devices = await clickHouseClient.queryAllDevices();
        res.json({ devices });
    } catch (error) {
        console.error('Error fetching devices:', error);
        res.status(500).json({ error: 'Failed to query devices' });
    }
});

app.get('/api/history', async (req, res) => {
    try {
        const timeRange = req.query.range || '24h';
        const deviceId = req.query.device_id;
        const readingType = req.query.reading_type;

        if (!deviceId || !readingType) {
            return res.status(400).json({ error: 'device_id and reading_type are required' });
        }

        const historyData = await clickHouseClient.queryHistoricalData(deviceId, readingType, timeRange);
        res.json({ historyData, timeRange });
    } catch (error) {
        console.error('Error querying history:', error);
        res.status(500).json({ error: 'Failed to query historical data' });
    }
});

// Get aggregated history for multiple devices (for sidebar chart)
app.get('/api/history/aggregate', async (req, res) => {
    try {
        const timeRange = req.query.range || '24h';
        const deviceIds = req.query.devices ? req.query.devices.split(',') : [];
        const readingType = req.query.type || 'temperature';

        if (deviceIds.length === 0) {
            return res.json({ data: [], timeRange });
        }

        const data = await clickHouseClient.queryAggregateHistory(deviceIds, readingType, timeRange);
        res.json({ data, timeRange, deviceCount: deviceIds.length });

    } catch (error) {
        console.error('Error querying aggregate history:', error);
        res.status(500).json({ error: 'Failed to query aggregate history' });
    }
});

// Get historical comparison data (yesterday, 24h change, last week)
app.get('/api/history/comparison', async (req, res) => {
    try {
        const deviceIds = req.query.devices ? req.query.devices.split(',') : [];
        const readingType = req.query.type || 'temperature';

        if (deviceIds.length === 0) {
            return res.json({ comparison: null });
        }

        const comparison = await clickHouseClient.queryHistoricalComparison(deviceIds, readingType);
        res.json({ comparison, deviceCount: deviceIds.length });

    } catch (error) {
        console.error('Error querying historical comparison:', error);
        res.status(500).json({ error: 'Failed to query historical comparison' });
    }
});

// Get temperature predictions using diurnal pattern model
app.get('/api/prediction', async (req, res) => {
    try {
        const deviceIds = req.query.devices ? req.query.devices.split(',') : [];
        const hoursAhead = parseInt(req.query.hours) || 24;

        if (deviceIds.length === 0) {
            return res.json({ prediction: null, error: 'No devices specified' });
        }

        const prediction = await clickHouseClient.queryTemperaturePrediction(deviceIds, hoursAhead);
        res.json({ prediction, deviceCount: deviceIds.length });

    } catch (error) {
        console.error('Error generating prediction:', error);
        res.status(500).json({ error: 'Failed to generate prediction' });
    }
});

app.get('/api/history/average', async (req, res) => {
    try {
        const timeRange = req.query.range || '24h';
        const rawData = await clickHouseClient.queryAverageData(timeRange);

        // Transform to expected format, preserving sparklineData and trend
        const averageData = {};
        Object.entries(rawData).forEach(([deviceId, sensorData]) => {
            averageData[deviceId] = {};
            Object.entries(sensorData.readings || {}).forEach(([readingType, reading]) => {
                averageData[deviceId][readingType] = {
                    value: reading.value,
                    type: 'latest',
                    timestamp: reading.timestamp,
                    latitude: sensorData.latitude,
                    longitude: sensorData.longitude,
                    sparklineData: reading.sparklineData,
                    trend: reading.trend
                };
            });
        });

        res.json({ averageData, timeRange });
    } catch (error) {
        console.error('Error querying average:', error);
        res.status(500).json({ error: 'Failed to query average data' });
    }
});

// Get regional aggregates for choropleth map overlay
app.get('/api/regions', async (req, res) => {
    try {
        const timeRange = req.query.range || '24h';
        const metric = req.query.metric || req.query.reading_type || 'temperature';
        const regionalData = await clickHouseClient.queryRegionalAggregates(timeRange, metric);

        // Get unit for the metric
        const units = {
            temperature: '°C',
            humidity: '%',
            pressure: 'hPa',
            co2: 'ppm',
            pm2_5: 'µg/m³',
            pm10: 'µg/m³',
            voc_index: 'index',
            nox_index: 'index'
        };

        res.json({
            metric,
            unit: units[metric] || '',
            timeRange,
            regions: regionalData
        });
    } catch (error) {
        console.error('Error fetching regional aggregates:', error);
        res.status(500).json({ error: 'Failed to query regional aggregates' });
    }
});

// Get region data for choropleth overlay
// Serves pre-computed data when available, falls back to on-demand queries
// Supports historical queries via timestamp parameter
app.get('/api/regions/data', async (req, res) => {
    const zoom = parseInt(req.query.zoom) || 5;
    const metric = req.query.metric || 'temperature';
    let deployment = req.query.deployment || 'outdoor';  // Default to outdoor sensors
    const range = req.query.range || DEFAULT_TIME_RANGE;  // Time window: 30m, 1h, 2h, 4h, 24h
    const timestamp = req.query.timestamp;  // ISO8601 for historical queries

    // Normalize deployment filter to match precomputed cache keys
    // Frontend sends uppercase comma-separated (e.g., "MIXED,OUTDOOR"), cache uses lowercase single values
    const normalizedDeployment = deployment.toUpperCase().split(',').sort().join(',');
    if (normalizedDeployment === 'OUTDOOR' || normalizedDeployment === 'MIXED,OUTDOOR') {
        deployment = 'outdoor';  // Map to precomputed 'outdoor' key
    } else if (normalizedDeployment === 'INDOOR') {
        deployment = 'indoor';
    } else if (normalizedDeployment === 'ALL' || normalizedDeployment === '') {
        deployment = 'all';
    }
    // Other combinations (e.g., "INDOOR,OUTDOOR,PORTABLE") will use on-demand queries

    // Map zoom to admin level
    let adminLevel = regionService.getAdminLevelForZoom(zoom);
    let data = null;
    let isHistorical = false;
    let isOnDemand = false;

    // Historical query: always on-demand (can't precompute all timestamps)
    if (timestamp) {
        isHistorical = true;
        isOnDemand = true;
        try {
            data = await regionService.getHistoricalRegionData(metric, adminLevel, timestamp, '1h', deployment);
            data = { ...data, timestamp: Date.now(), historical: true, query_timestamp: timestamp };
        } catch (err) {
            console.error(`Historical query failed for ${timestamp}:`, err.message);
        }
    } else {
        // Live query: try precomputed cache first
        data = getPrecomputedData(adminLevel, metric, deployment, range);

        // Fall back to lower admin levels if higher ones aren't available yet
        while (!data && adminLevel > 0) {
            adminLevel--;
            data = getPrecomputedData(adminLevel, metric, deployment, range);
        }

        // If not pre-computed, try on-demand query
        // This handles: non-precomputed time windows, custom deployment combinations
        if (!data) {
            isOnDemand = true;
            try {
                const onDemandData = await regionService.precomputeRegions(metric, adminLevel, range, deployment);
                data = { ...onDemandData, timestamp: Date.now(), on_demand: true };
            } catch (err) {
                console.error(`On-demand query failed for ${deployment} ${range}:`, err.message);
            }
        }
    }

    if (!data) {
        // Data not yet computed (server just started)
        res.set('Cache-Control', 'no-store');
        res.set('Retry-After', '5');
        return res.status(503).json({
            error: 'Region data is being computed, please retry shortly',
            admin_level: adminLevel,
            metric,
            deployment_filter: deployment,
            time_range: range,
            regions: {}
        });
    }

    // Set HTTP caching headers
    if (isHistorical || isOnDemand) {
        // Historical/on-demand queries: short cache, no stale-while-revalidate
        res.set('Cache-Control', 'public, max-age=60');
        res.set('X-Cache', isHistorical ? 'HISTORICAL' : 'ON-DEMAND');
    } else {
        // Precomputed data: cache until next refresh cycle
        const age = Math.floor((Date.now() - data.timestamp) / 1000);
        const maxAge = Math.max(0, Math.floor((PRECOMPUTE_INTERVAL - (Date.now() - data.timestamp)) / 1000));
        res.set('Cache-Control', `public, max-age=${maxAge}, stale-while-revalidate=60`);
        res.set('X-Cache', 'PRECOMPUTED');
        res.set('Age', age.toString());
    }
    res.set('Vary', 'Accept-Encoding');

    res.json(data);
});

// Get region pre-computation status
app.get('/api/regions/status', async (req, res) => {
    const status = {
        last_refresh: lastRefreshTime ? new Date(lastRefreshTime).toISOString() : null,
        refresh_in_progress: refreshInProgress,
        refresh_interval_seconds: PRECOMPUTE_INTERVAL / 1000,
        deployment_filters: DEPLOYMENT_FILTERS,
        device_cache: null,
        data: {}
    };

    // Get device region cache stats
    try {
        status.device_cache = await regionService.getDeviceRegionCacheStats();
    } catch (error) {
        status.device_cache = { error: error.message };
    }

    for (const adminLevel of ADMIN_LEVELS) {
        status.data[`adm${adminLevel}`] = {};
        for (const metric of METRICS) {
            status.data[`adm${adminLevel}`][metric] = {};
            for (const deployment of DEPLOYMENT_FILTERS) {
                const data = precomputedRegions[adminLevel]?.[metric]?.[deployment];
                status.data[`adm${adminLevel}`][metric][deployment] = data ? {
                    region_count: Object.keys(data.regions).length,
                    timestamp: new Date(data.timestamp).toISOString()
                } : null;
            }
        }
    }

    res.json(status);
});

// Get region info at a specific point (for click-to-inspect)
app.get('/api/regions/at-point', async (req, res) => {
    let { lat, lng, zoom, metric = 'temperature', deployment = 'outdoor', range = '1h', timestamp } = req.query;

    if (!lat || !lng) {
        return res.status(400).json({ error: 'lat and lng required' });
    }

    // Normalize deployment filter to match precomputed cache keys
    const normalizedDeployment = deployment.toUpperCase().split(',').sort().join(',');
    if (normalizedDeployment === 'OUTDOOR' || normalizedDeployment === 'MIXED,OUTDOOR') {
        deployment = 'outdoor';
    } else if (normalizedDeployment === 'INDOOR') {
        deployment = 'indoor';
    } else if (normalizedDeployment === 'ALL' || normalizedDeployment === '') {
        deployment = 'all';
    }

    try {
        // Get which regions contain this point
        const regions = await regionService.getRegionsAtPoint(parseFloat(lat), parseFloat(lng));

        // Determine which admin level to show based on zoom
        const adminLevel = parseInt(zoom) <= 1 ? 0 : parseInt(zoom) <= 5 ? 1 : 2;
        const admKey = `adm${adminLevel}`;
        const region = regions[admKey];

        if (!region) {
            return res.json({
                found: false,
                message: 'No region boundary at this location',
                lat: parseFloat(lat),
                lng: parseFloat(lng),
                regions  // Return all found regions for debugging
            });
        }

        // Determine time window to use
        const timeWindow = timestamp ? '1h' : range;
        const isHistorical = !!timestamp;

        // Look up pre-computed data for this region with deployment filter and time window
        const precomputed = precomputedRegions[adminLevel]?.[metric]?.[deployment]?.[timeWindow];
        let regionData = precomputed?.regions?.[region.region_id];

        // For historical queries, use fast single-region query (not the slow all-regions query)
        if (isHistorical) {
            regionData = await regionService.getHistoricalDataForSingleRegion(
                region.region_id, metric, timestamp, '1h', deployment
            );
        }

        // Get the list of devices that contributed to this region (with deployment filter)
        // Skip for historical mode - device query doesn't support historical timestamps yet
        let devices = [];
        if (!isHistorical) {
            devices = await regionService.getDevicesInRegion(region.region_id, metric, timeWindow, deployment);
        }

        res.json({
            found: true,
            region_id: region.region_id,
            name: region.name,
            country_code: region.country_code,
            admin_level: adminLevel,
            metric,
            deployment_filter: deployment,
            time_window: timeWindow,
            is_historical: isHistorical,
            data: regionData || null,
            devices,  // List of contributing devices (empty for historical)
            unit: precomputed?.unit || { temperature: '°C', humidity: '%', pressure: 'hPa', co2: 'ppm', pm2_5: 'µg/m³', voc_index: '' }[metric] || '',
            precomputed_at: precomputed?.timestamp ? new Date(precomputed.timestamp).toISOString() : null,
            all_regions: regions  // Include all admin levels for debugging
        });
    } catch (error) {
        console.error('Error getting region at point:', error);
        res.status(500).json({ error: 'Failed to get region info' });
    }
});

// Get devices in a specific region (lazy-loaded from popup)
app.get('/api/regions/devices', async (req, res) => {
    const { region, metric = 'temperature', deployment = 'outdoor', range = '1h' } = req.query;

    if (!region) {
        return res.status(400).json({ error: 'region parameter required' });
    }

    try {
        const devices = await regionService.getDevicesInRegion(region, metric, range, deployment);
        res.json({ devices });
    } catch (error) {
        console.error('Error getting devices in region:', error);
        res.status(500).json({ error: 'Failed to get devices' });
    }
});

// Get town leaderboards (ADM2 level)
// Returns: byNodes (device count), byReadings (reading count), byTypes (sensor type count)
app.get('/api/leaderboard', async (req, res) => {
    try {
        const limit = parseInt(req.query.limit) || 5;

        // Serve from pre-computed cache (instant response)
        if (precomputedLeaderboard && precomputedLeaderboard.byNodes) {
            return res.json({
                admin_level: 2,
                time_range: '24h',
                byNodes: precomputedLeaderboard.byNodes.slice(0, limit),
                bySensors: precomputedLeaderboard.bySensors.slice(0, limit),
                byTypes: precomputedLeaderboard.byTypes.slice(0, limit),
                total_regions: precomputedLeaderboard.total_regions,
                cached: true,
                cache_age_seconds: Math.round((Date.now() - precomputedLeaderboard.timestamp) / 1000)
            });
        }

        // Fallback to live query if cache not ready
        const [byNodes, bySensors, byTypes] = await Promise.all([
            regionService.getTopRegions(2, '24h', limit),
            regionService.getTopRegionsBySensors(2, '24h', limit),
            regionService.getTopRegionsBySensorTypes(2, '24h', limit)
        ]);
        res.json({
            admin_level: 2,
            time_range: '24h',
            byNodes: byNodes.regions,
            bySensors: bySensors.regions,
            byTypes: byTypes.regions,
            total_regions: byNodes.total_regions,
            cached: false
        });
    } catch (error) {
        console.error('Error fetching leaderboard:', error);
        res.status(500).json({ error: 'Failed to query leaderboard data' });
    }
});

// Get environmental leaderboards (cleanest air, best weather, etc.)
app.get('/api/env-leaderboard', async (req, res) => {
    try {
        // Serve from pre-computed cache (instant response)
        if (precomputedEnvLeaderboard && precomputedEnvLeaderboard.timestamp) {
            return res.json({
                ...precomputedEnvLeaderboard,
                cached: true,
                cache_age_seconds: Math.round((Date.now() - precomputedEnvLeaderboard.timestamp) / 1000)
            });
        }

        // Fallback to live query if cache not ready
        const data = await regionService.getEnvironmentalLeaderboards(5);
        res.json({
            ...data,
            timestamp: Date.now(),
            cached: false
        });
    } catch (error) {
        console.error('Error fetching environmental leaderboard:', error);
        res.status(500).json({ error: 'Failed to query environmental leaderboard data' });
    }
});

// Get visitor's approximate location from IP address
app.get('/api/location', (req, res) => {
    // Get IP from proxy headers or socket
    const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim()
            || req.socket.remoteAddress;

    // Handle localhost/development (returns null)
    const geo = geoip.lookup(ip);

    if (geo && geo.ll) {
        res.json({
            lat: geo.ll[0],
            lng: geo.ll[1],
            city: geo.city,
            country: geo.country
        });
    } else {
        res.json(null);
    }
});

// Viewer heartbeat - tracks active viewers
app.post('/api/viewers/heartbeat', (req, res) => {
    const viewerId = req.body.viewerId;
    if (!viewerId) {
        return res.status(400).json({ error: 'viewerId required' });
    }

    const now = Date.now();
    activeViewers.set(viewerId, now);

    // Clean up stale viewers
    const cutoff = now - VIEWER_TIMEOUT;
    for (const [id, lastSeen] of activeViewers) {
        if (lastSeen < cutoff) {
            activeViewers.delete(id);
        }
    }

    res.json({ count: activeViewers.size });
});

// Get current viewer count
app.get('/api/viewers', (req, res) => {
    const now = Date.now();
    const cutoff = now - VIEWER_TIMEOUT;

    // Clean up stale viewers and count
    let count = 0;
    for (const [id, lastSeen] of activeViewers) {
        if (lastSeen < cutoff) {
            activeViewers.delete(id);
        } else {
            count++;
        }
    }

    res.json({ count });
});

// Get comparison data for dashboard "vs yesterday" badges
// Accepts comma-separated device IDs and returns comparison metrics
app.get('/api/comparison', async (req, res) => {
    try {
        const deviceIds = req.query.devices ? req.query.devices.split(',') : [];

        if (deviceIds.length === 0) {
            return res.json({});
        }

        const comparison = await clickHouseClient.queryComparisonData(deviceIds);
        res.json(comparison);

    } catch (error) {
        console.error('Error fetching comparison data:', error);
        res.status(500).json({ error: 'Failed to fetch comparison data' });
    }
});


/**
 * Check if a file is valid PMTiles format (not MBTiles/SQLite)
 * PMTiles files start with "PMTiles" magic bytes, MBTiles start with "SQLite"
 * Also checks minimum file size to detect incomplete/corrupted files
 */
function isValidPMTiles(filePath, minSizeMB = 10) {
    try {
        if (!fs.existsSync(filePath)) return { valid: false, reason: 'file does not exist' };

        // Check file size - a valid PMTiles with boundaries should be at least minSizeMB
        const stats = fs.statSync(filePath);
        const sizeMB = stats.size / (1024 * 1024);
        if (sizeMB < minSizeMB) {
            return { valid: false, reason: `file too small (${sizeMB.toFixed(2)}MB < ${minSizeMB}MB minimum)` };
        }

        const fd = fs.openSync(filePath, 'r');
        const buffer = Buffer.alloc(7);
        fs.readSync(fd, buffer, 0, 7, 0);
        fs.closeSync(fd);
        // PMTiles v3 starts with 0x50 0x4D (PM) - check first two bytes
        // MBTiles/SQLite starts with "SQLite format 3"
        const header = buffer.toString('ascii', 0, 6);
        if (header === 'SQLite') {
            return { valid: false, reason: 'file is MBTiles/SQLite format, not PMTiles' };
        }
        // PMTiles magic: first byte is 0x50 ('P'), or could check for valid header
        if (buffer[0] === 0x50 && buffer[1] === 0x4D) {
            return { valid: true, sizeMB };
        }
        return { valid: false, reason: 'invalid PMTiles header' };
    } catch (e) {
        return { valid: false, reason: e.message };
    }
}

/**
 * Check if MBTiles file is valid (SQLite format, reasonable size, and not corrupted)
 */
function isValidMBTiles(filePath, minSizeMB = 100) {
    try {
        if (!fs.existsSync(filePath)) return { valid: false, reason: 'file does not exist' };

        // Check for journal file (indicates interrupted write)
        if (fs.existsSync(filePath + '-journal')) {
            return { valid: false, reason: 'SQLite journal file exists (interrupted write)' };
        }

        const stats = fs.statSync(filePath);
        const sizeMB = stats.size / (1024 * 1024);
        if (sizeMB < minSizeMB) {
            return { valid: false, reason: `file too small (${sizeMB.toFixed(1)}MB < ${minSizeMB}MB)` };
        }

        const fd = fs.openSync(filePath, 'r');
        const buffer = Buffer.alloc(16);
        fs.readSync(fd, buffer, 0, 16, 0);
        fs.closeSync(fd);

        // MBTiles should start with "SQLite format 3"
        const header = buffer.toString('ascii', 0, 15);
        if (!header.startsWith('SQLite format')) {
            return { valid: false, reason: 'not SQLite format' };
        }

        // Quick integrity check using sqlite3 command
        try {
            const { execSync } = require('child_process');
            const result = execSync(`sqlite3 "${filePath}" "PRAGMA integrity_check(1);"`, {
                encoding: 'utf8',
                timeout: 10000
            }).trim();
            if (result !== 'ok') {
                return { valid: false, reason: `SQLite integrity check failed: ${result}` };
            }
        } catch (e) {
            // sqlite3 command might not be available, skip integrity check
            console.log('  Note: sqlite3 not available for integrity check');
        }

        return { valid: true, sizeMB, mtime: stats.mtimeMs };
    } catch (e) {
        return { valid: false, reason: e.message };
    }
}

/**
 * Generate PMTiles from processed GeoJSON files
 * Workflow: tippecanoe → MBTiles → pmtiles convert → PMTiles
 * If skipTippecanoe is true, uses existing MBTiles (for fast restarts)
 * Returns a promise that resolves when generation is complete
 */
function generatePMTiles(pmtilesPath, boundariesDir, skipTippecanoe = false) {
    const { spawn, execSync } = require('child_process');

    return new Promise(async (resolve, reject) => {
        // Check if tippecanoe is installed
        let hasTippecanoe = false;
        try {
            execSync('which tippecanoe', { stdio: 'ignore' });
            hasTippecanoe = true;
        } catch (e) {
            console.log('\n' + '='.repeat(60));
            console.log('tippecanoe not installed - PMTiles cannot be generated');
            console.log('Install with:');
            console.log('  macOS:  brew install tippecanoe');
            console.log('  Linux:  See https://github.com/felt/tippecanoe');
            console.log('='.repeat(60) + '\n');
            return resolve(false);
        }

        // Check if pmtiles CLI is installed (needed for MBTiles → PMTiles conversion)
        let hasPmtiles = false;
        try {
            execSync('which pmtiles', { stdio: 'ignore' });
            hasPmtiles = true;
        } catch (e) {
            // Try to install pmtiles via npm (works cross-platform)
            console.log('pmtiles CLI not found, attempting to install via npm...');
            try {
                execSync('npm install -g pmtiles', { stdio: 'inherit' });
                hasPmtiles = true;
                console.log('✓ pmtiles CLI installed successfully');
            } catch (installErr) {
                console.log('\n' + '='.repeat(60));
                console.log('pmtiles CLI not installed - needed to convert MBTiles to PMTiles');
                console.log('Install with:');
                console.log('  npm install -g pmtiles');
                console.log('  or: brew install pmtiles');
                console.log('='.repeat(60) + '\n');
                return resolve(false);
            }
        }

        // Build layer args for available files with per-layer zoom ranges
        // This ensures each ADM level only appears at appropriate zoom levels
        const layerZoomRanges = {
            0: { minzoom: 0, maxzoom: 1 },   // ADM0: countries at zoom 0-1
            1: { minzoom: 2, maxzoom: 4 },   // ADM1: states at zoom 2-4
            2: { minzoom: 5, maxzoom: 7 },   // ADM2: districts at zoom 5-7
            3: { minzoom: 8, maxzoom: 10 },  // ADM3: sub-districts at zoom 8-10
            4: { minzoom: 11, maxzoom: 14 }  // ADM4: localities at zoom 11-14
        };

        const layers = [];
        const availableLevels = [];
        for (let level = 0; level <= 4; level++) {
            const file = path.join(boundariesDir, `processed_adm${level}.geojson`);
            if (fs.existsSync(file)) {
                const zoomRange = layerZoomRanges[level];
                // Use -L with JSON config for per-layer zoom control
                const layerConfig = JSON.stringify({
                    file: file,
                    layer: `adm${level}`,
                    minzoom: zoomRange.minzoom,
                    maxzoom: zoomRange.maxzoom
                });
                layers.push('-L', layerConfig);
                availableLevels.push(level);
            }
        }

        if (layers.length === 0) {
            console.log('No processed GeoJSON files found');
            return resolve(false);
        }

        const startTime = Date.now();
        let lastProgress = '';
        let lastReadProgress = '';
        let lastRetryLog = 0;
        const mbtilesPath = path.join(boundariesDir, 'regions.mbtiles');

        // Check if we can skip tippecanoe (use cached MBTiles)
        if (skipTippecanoe) {
            console.log('\n' + '='.repeat(60));
            console.log('GENERATING PMTILES (using cached MBTiles)');
            console.log('Step 1/2: SKIPPED (MBTiles already exists)');
            console.log('='.repeat(60) + '\n');
        } else {
            console.log('\n' + '='.repeat(60));
            console.log('GENERATING PMTILES (ADM' + availableLevels.join(', ADM') + ')');
            console.log('Step 1/2: tippecanoe → MBTiles (this takes several minutes)');
            console.log('='.repeat(60) + '\n');
        }

        // Estimate total features based on available layers
        // ADM0: ~250, ADM1: ~4500, ADM2: ~45000, ADM3: ~150000, ADM4: ~52000
        const estimatedFeatures = {
            0: 250, 1: 4500, 2: 45000, 3: 150000, 4: 52000
        };
        const totalEstimatedFeatures = availableLevels.reduce((sum, level) =>
            sum + (estimatedFeatures[level] || 0), 0);

        // Step 1: Generate MBTiles with tippecanoe (or skip if cached)
        if (!skipTippecanoe) {
        const tippecanoePromise = new Promise((resolveTippecanoe, rejectTippecanoe) => {
            const child = spawn('tippecanoe', [
                '-o', mbtilesPath,
                ...layers,
                '--read-parallel',
                '--minimum-zoom=0',
                '--maximum-zoom=14',
                '--simplification=10',
                '--drop-densest-as-needed',
                '--extend-zooms-if-still-dropping',
                '--force'
            ], { cwd: boundariesDir });

            // Stream stderr (tippecanoe outputs progress to stderr)
            child.stderr.on('data', (data) => {
                const text = data.toString();

                // Check for feature reading progress
                const readMatch = text.match(/Read (\d+\.\d+) million features/);
                if (readMatch) {
                    const [, millions] = readMatch;
                    const featuresRead = parseFloat(millions) * 1000000;
                    const percent = Math.min(100, (featuresRead / totalEstimatedFeatures) * 100);
                    const barWidth = 30;
                    const filled = Math.round((percent / 100) * barWidth);
                    const bar = '█'.repeat(filled) + '░'.repeat(barWidth - filled);
                    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
                    const progress = `${Math.floor(percent / 10) * 10}%`;
                    if (progress !== lastReadProgress) {
                        console.log(`[tippecanoe] Reading: [${bar}] ${percent.toFixed(0)}% (${millions}M features, ${elapsed}s)`);
                        lastReadProgress = progress;
                    }
                    return;
                }

                // Check for tile generation progress - log every 5%
                const progressMatch = text.match(/(\d+\.\d+)%\s+(\d+\/\d+\/\d+)/);
                if (progressMatch) {
                    const [, percent, tile] = progressMatch;
                    const percentInt = Math.floor(parseFloat(percent) / 5) * 5;
                    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
                    const progress = `${percentInt}%`;
                    if (progress !== lastProgress) {
                        console.log(`[tippecanoe] ${percent}% - tile ${tile} (${elapsed}s)`);
                        lastProgress = progress;
                    }
                } else if (text.includes('tile') && text.includes('size is')) {
                    // Log tile retries occasionally so user knows work is happening
                    const now = Date.now();
                    if (now - lastRetryLog > 30000) {
                        const elapsed = ((now - startTime) / 1000).toFixed(0);
                        console.log(`[tippecanoe] Optimizing dense tiles... (${elapsed}s)`);
                        lastRetryLog = now;
                    }
                } else if (text.includes('Reordering geometry')) {
                    const reorderMatch = text.match(/Reordering geometry: (\d+)%/);
                    if (reorderMatch && reorderMatch[1] === '50') {
                        console.log(`[tippecanoe] Reordering geometry: 50%`);
                    }
                } else if (text.includes('features,') && text.includes('bytes of geometry')) {
                    console.log(`[tippecanoe] Processing complete, generating tiles...`);
                } else {
                    // Catch-all: log any unmatched output (trimmed) so we see activity
                    const trimmed = text.trim().replace(/\s+/g, ' ').substring(0, 100);
                    if (trimmed && !trimmed.match(/^\d+\.\d+%/) && !trimmed.includes('sparsest')) {
                        const now = Date.now();
                        if (now - lastRetryLog > 10000) {  // Rate limit to every 10s
                            console.log(`[tippecanoe] ${trimmed}`);
                            lastRetryLog = now;
                        }
                    }
                }
            });

            // Periodic heartbeat showing file size progress
            const heartbeat = setInterval(() => {
                try {
                    if (fs.existsSync(mbtilesPath)) {
                        const stats = fs.statSync(mbtilesPath);
                        const sizeMB = (stats.size / 1024 / 1024).toFixed(1);
                        const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
                        console.log(`[tippecanoe] Heartbeat: ${sizeMB}MB written (${elapsed} min)`);
                    }
                } catch (e) {}
            }, 60000);  // Every 60 seconds

            child.on('close', () => clearInterval(heartbeat));

            child.stdout.on('data', (data) => {
                const text = data.toString().trim();
                if (text) console.log(`[tippecanoe] ${text}`);
            });

            child.on('close', (code) => {
                process.stdout.write('\r' + ' '.repeat(70) + '\r');
                if (code === 0) {
                    const stats = fs.statSync(mbtilesPath);
                    const sizeMB = (stats.size / 1024 / 1024).toFixed(1);
                    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
                    console.log(`✓ MBTiles generated (${sizeMB}MB) in ${elapsed}s`);
                    resolveTippecanoe(true);
                } else {
                    console.log(`✗ tippecanoe failed (code ${code})`);
                    resolveTippecanoe(false);
                }
            });

            child.on('error', (err) => {
                console.error('Failed to start tippecanoe:', err.message);
                resolveTippecanoe(false);
            });
        });

        const tippecanoeSuccess = await tippecanoePromise;
        if (!tippecanoeSuccess) {
            return resolve(false);
        }
        } // end if (!skipTippecanoe)

        // Step 2: Convert MBTiles to PMTiles
        // Clean up any SQLite journal files from interrupted runs
        const journalPath = mbtilesPath + '-journal';
        if (fs.existsSync(journalPath)) {
            try {
                fs.unlinkSync(journalPath);
                console.log('  Cleaned up SQLite journal file from previous interrupted run');
            } catch (e) {
                // Ignore
            }
        }

        console.log('\nStep 2/2: Converting MBTiles → PMTiles...');
        const convertStart = Date.now();

        const convertPromise = new Promise((resolveConvert, rejectConvert) => {
            const child = spawn('pmtiles', ['convert', mbtilesPath, pmtilesPath], {
                cwd: boundariesDir
            });

            child.stderr.on('data', (data) => {
                const text = data.toString().trim();
                if (text) process.stdout.write(`\r[pmtiles] ${text}                    `);
            });

            child.stdout.on('data', (data) => {
                const text = data.toString().trim();
                if (text) process.stdout.write(`\r[pmtiles] ${text}                    `);
            });

            child.on('close', (code) => {
                process.stdout.write('\r' + ' '.repeat(70) + '\r');
                if (code === 0) {
                    resolveConvert(true);
                } else {
                    console.log(`✗ pmtiles convert failed (code ${code})`);
                    resolveConvert(false);
                }
            });

            child.on('error', (err) => {
                console.error('Failed to start pmtiles:', err.message);
                resolveConvert(false);
            });
        });

        const convertSuccess = await convertPromise;
        if (!convertSuccess) {
            return resolve(false);
        }

        // Verify the output is valid PMTiles
        const validation = isValidPMTiles(pmtilesPath);
        if (!validation.valid) {
            console.log(`✗ Generated file is not valid: ${validation.reason}`);
            return resolve(false);
        }

        const totalElapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        const convertElapsed = ((Date.now() - convertStart) / 1000).toFixed(1);

        console.log(`✓ PMTiles generated successfully (${validation.sizeMB.toFixed(1)}MB)`);
        console.log(`  Total time: ${totalElapsed}s (convert: ${convertElapsed}s)`);

        // Keep MBTiles cached for faster restarts (only need to re-run convert step)
        const mbStats = fs.statSync(mbtilesPath);
        console.log(`  MBTiles cached: ${(mbStats.size / 1024 / 1024).toFixed(1)}MB (for faster restarts)`);

        resolve(true);
    });
}

/**
 * Export region boundaries from ClickHouse to GeoJSON files
 * This allows a fresh deployment to regenerate PMTiles from the shared database
 * @param {string} boundariesDir - Directory to write GeoJSON files
 * @returns {Promise<boolean>} True if export was successful
 */
async function exportBoundariesFromClickHouse(boundariesDir) {
    console.log('Exporting boundaries from ClickHouse to GeoJSON...');

    // Ensure directory exists
    if (!fs.existsSync(boundariesDir)) {
        fs.mkdirSync(boundariesDir, { recursive: true });
    }

    try {
        // Check what admin levels exist in ClickHouse
        const countResult = await clickHouseClient.query({
            query: `SELECT admin_level, count() as cnt FROM wesense_respiro.region_boundaries GROUP BY admin_level ORDER BY admin_level`,
            format: 'JSONEachRow'
        });
        const counts = await countResult.json();

        if (counts.length === 0) {
            console.log('  No boundary data in ClickHouse to export');
            return false;
        }

        // Export each admin level
        for (const { admin_level, cnt } of counts) {
            const outputPath = path.join(boundariesDir, `processed_adm${admin_level}.geojson`);

            // Skip if file already exists and has content
            if (fs.existsSync(outputPath)) {
                const stats = fs.statSync(outputPath);
                if (stats.size > 1000) {
                    console.log(`  ADM${admin_level}: Already exists (${(stats.size / 1024 / 1024).toFixed(1)}MB)`);
                    continue;
                }
            }

            console.log(`  ADM${admin_level}: Exporting ${cnt} regions...`);

            // Query all regions for this admin level
            const result = await clickHouseClient.query({
                query: `
                    SELECT
                        region_id,
                        admin_level,
                        name,
                        country_code,
                        original_id,
                        polygon,
                        bbox_min_lon,
                        bbox_max_lon,
                        bbox_min_lat,
                        bbox_max_lat
                    FROM wesense_respiro.region_boundaries
                    WHERE admin_level = {admin_level:UInt8}
                `,
                query_params: { admin_level },
                format: 'JSONEachRow'
            });
            const rows = await result.json();

            // Convert to GeoJSON FeatureCollection
            const features = rows.map(row => {
                // Convert polygon array back to GeoJSON coordinates
                // ClickHouse stores as Array(Array(Tuple(Float64, Float64)))
                // GeoJSON needs [[[lon, lat], ...]]
                const coordinates = row.polygon.map(ring =>
                    ring.map(point => [point[0], point[1]])
                );

                return {
                    type: 'Feature',
                    properties: {
                        region_id: row.region_id,
                        admin_level: row.admin_level,
                        name: row.name,
                        country_code: row.country_code,
                        original_id: row.original_id
                    },
                    geometry: {
                        type: 'Polygon',
                        coordinates: coordinates
                    }
                };
            });

            const geojson = {
                type: 'FeatureCollection',
                features: features
            };

            fs.writeFileSync(outputPath, JSON.stringify(geojson));
            const sizeMB = (fs.statSync(outputPath).size / 1024 / 1024).toFixed(1);
            console.log(`  ADM${admin_level}: Exported ${features.length} features (${sizeMB}MB)`);
        }

        console.log('Boundary export complete!');
        return true;
    } catch (error) {
        console.error('Failed to export boundaries from ClickHouse:', error.message);
        return false;
    }
}

/**
 * Check if boundary data exists and auto-download if missing
 * Also checks for PMTiles and regenerates if missing
 */
async function checkAndDownloadBoundaryData() {
    const { spawn, execSync } = require('child_process');

    // First check if PMTiles exists
    const pmtilesPath = path.join(__dirname, '../public/regions.pmtiles');
    const boundariesDir = path.join(__dirname, '../data/boundaries');

    // Check if processed files exist (need at least ADM0-2 for basic functionality)
    const hasProcessedFiles = [0, 1, 2].every(level =>
        fs.existsSync(path.join(boundariesDir, `processed_adm${level}.geojson`))
    );

    // Determine if we need to generate/regenerate PMTiles
    let needsRegeneration = false;
    let regenerationReason = '';

    if (!fs.existsSync(pmtilesPath)) {
        needsRegeneration = true;
        regenerationReason = 'PMTiles file missing';
    } else {
        const validation = isValidPMTiles(pmtilesPath);
        if (!validation.valid) {
            // File exists but is invalid (wrong format, too small, etc.)
            needsRegeneration = true;
            regenerationReason = `PMTiles invalid: ${validation.reason}`;
            // Remove the invalid file
            try {
                fs.unlinkSync(pmtilesPath);
            } catch (e) {
                // Ignore
            }
        }
    }

    if (!needsRegeneration && fs.existsSync(pmtilesPath)) {
        // PMTiles exists and is valid - check if any source files are newer
        const pmtilesStats = fs.statSync(pmtilesPath);
        for (let level = 0; level <= 4; level++) {
            const file = path.join(boundariesDir, `processed_adm${level}.geojson`);
            if (fs.existsSync(file)) {
                const fileStats = fs.statSync(file);
                if (fileStats.mtimeMs > pmtilesStats.mtimeMs) {
                    needsRegeneration = true;
                    regenerationReason = `processed_adm${level}.geojson is newer than PMTiles`;
                    break;
                }
            }
        }
    }

    if (needsRegeneration) {
        console.log(`${regenerationReason} - checking if we can regenerate...`);

        // If GeoJSON files are missing, try to export from ClickHouse first
        if (!hasProcessedFiles) {
            console.log('Processed GeoJSON files missing - checking ClickHouse for boundary data...');
            const exported = await exportBoundariesFromClickHouse(boundariesDir);
            if (exported) {
                // Re-check for processed files after export
                const hasFilesNow = [0, 1, 2].every(level =>
                    fs.existsSync(path.join(boundariesDir, `processed_adm${level}.geojson`))
                );
                if (hasFilesNow) {
                    console.log('GeoJSON files exported from ClickHouse - proceeding with PMTiles generation');
                }
            }
        }

        // Re-evaluate hasProcessedFiles after potential export
        const hasProcessedFilesNow = [0, 1, 2].every(level =>
            fs.existsSync(path.join(boundariesDir, `processed_adm${level}.geojson`))
        );

        if (hasProcessedFilesNow) {
            // Check if we can use cached MBTiles (fast path - only run convert step)
            const mbtilesPath = path.join(boundariesDir, 'regions.mbtiles');
            const mbtilesValidation = isValidMBTiles(mbtilesPath);
            let skipTippecanoe = false;

            if (!mbtilesValidation.valid && fs.existsSync(mbtilesPath)) {
                // MBTiles exists but is corrupted - clean it up
                console.log(`  MBTiles invalid: ${mbtilesValidation.reason}`);
                console.log('  Removing corrupted MBTiles for fresh regeneration...');
                try {
                    fs.unlinkSync(mbtilesPath);
                    // Also remove any journal files
                    const journalPath = mbtilesPath + '-journal';
                    if (fs.existsSync(journalPath)) fs.unlinkSync(journalPath);
                } catch (e) {
                    console.log(`  Warning: Could not remove corrupted files: ${e.message}`);
                }
            }

            if (mbtilesValidation.valid) {
                // Check if MBTiles is newer than all source files
                let mbtilesIsUpToDate = true;
                for (let level = 0; level <= 4; level++) {
                    const file = path.join(boundariesDir, `processed_adm${level}.geojson`);
                    if (fs.existsSync(file)) {
                        const fileStats = fs.statSync(file);
                        if (fileStats.mtimeMs > mbtilesValidation.mtime) {
                            mbtilesIsUpToDate = false;
                            console.log(`  Source file ADM${level} is newer than cached MBTiles`);
                            break;
                        }
                    }
                }

                if (mbtilesIsUpToDate) {
                    console.log(`  Found valid cached MBTiles (${mbtilesValidation.sizeMB.toFixed(1)}MB) - skipping tippecanoe`);
                    skipTippecanoe = true;
                }
            }

            await generatePMTiles(pmtilesPath, boundariesDir, skipTippecanoe);
        } else {
            console.log('No processed boundary files found - heatmap overlay disabled');
            console.log('Run: npm run setup-boundaries');
        }
    }

    // Now check ClickHouse for boundary data
    // Wait for ClickHouse connection if not ready yet
    if (!clickHouseClient.isConnected()) {
        console.log('Boundary check: waiting for ClickHouse connection...');
        setTimeout(checkAndDownloadBoundaryData, 3000);
        return;
    }

    try {
        const result = await clickHouseClient.query({
            query: `SELECT admin_level, count() as cnt FROM wesense_respiro.region_boundaries GROUP BY admin_level ORDER BY admin_level`,
            format: 'JSONEachRow'
        });
        const rows = await result.json();
        const counts = {};
        for (const row of rows) {
            counts[row.admin_level] = parseInt(row.cnt);
        }

        const hasAdm0 = (counts[0] || 0) > 0;
        const hasAdm1 = (counts[1] || 0) > 0;
        const hasAdm2 = (counts[2] || 0) > 0;
        const hasAdm3 = (counts[3] || 0) > 0;
        const hasAdm4 = (counts[4] || 0) > 0;

        console.log(`Boundary data: ADM0=${counts[0] || 0}, ADM1=${counts[1] || 0}, ADM2=${counts[2] || 0}, ADM3=${counts[3] || 0}, ADM4=${counts[4] || 0}`);

        // Check for missing data
        const missingCore = !hasAdm0 || !hasAdm1 || !hasAdm2;
        const missingFine = !hasAdm3 || !hasAdm4;

        if (missingCore || missingFine) {
            console.log('\n' + '='.repeat(70));
            if (missingCore) {
                console.log('BOUNDARY DATA MISSING - Starting automatic download...');
            } else {
                console.log('ADM3/ADM4 missing - Starting automatic download in background...');
            }
            console.log('='.repeat(70) + '\n');

            // Spawn background process to download boundaries
            const setupScript = path.join(__dirname, '../tools/setup_boundaries.sh');

            const child = spawn('bash', [setupScript], {
                detached: true,
                stdio: ['ignore', 'pipe', 'pipe']
            });

            child.stdout.on('data', (data) => {
                const lines = data.toString().trim().split('\n');
                lines.forEach(line => {
                    if (line.trim()) console.log(`[boundary-setup] ${line}`);
                });
            });

            child.stderr.on('data', (data) => {
                const lines = data.toString().trim().split('\n');
                lines.forEach(line => {
                    if (line.trim()) console.log(`[boundary-setup] ${line}`);
                });
            });

            // For core data (ADM0-2), wait for download then generate PMTiles
            // For fine data (ADM3-4), run in background
            if (missingCore) {
                await new Promise((resolve) => {
                    child.on('close', async (code) => {
                        if (code === 0) {
                            console.log('\n[boundary-setup] Boundary data setup complete!');
                            // Now generate PMTiles since we have the data
                            console.log('[boundary-setup] Generating PMTiles from downloaded data...');
                            await generatePMTiles(pmtilesPath, boundariesDir, false);
                        } else {
                            console.log(`\n[boundary-setup] Setup failed with code ${code}`);
                            console.log('[boundary-setup] Run manually: npm run setup-boundaries\n');
                        }
                        resolve();
                    });
                });
            } else {
                // ADM3/4 only - run in background, PMTiles will regenerate on next restart
                child.on('close', (code) => {
                    if (code === 0) {
                        console.log('\n[boundary-setup] ADM3/ADM4 data setup complete!');
                        console.log('[boundary-setup] Restart server to regenerate PMTiles with new layers.\n');
                    } else {
                        console.log(`\n[boundary-setup] Setup failed with code ${code}`);
                    }
                });
                child.unref();
            }
        }
    } catch (error) {
        // Table likely doesn't exist - need to run setup
        console.log(`Boundary check: ${error.message}`);
        console.log('The wesense_respiro.region_boundaries table may not exist yet.');
        console.log('Run: npm run setup-boundaries');
    }
}

// Start server
const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0';

const server = app.listen(PORT, HOST, async () => {
    console.log(`WeSense Respiro running at http://${HOST}:${PORT}`);

    // Start background pre-computation of regional aggregates
    startRegionRefreshLoop();

    // Check for missing boundary data and auto-download if needed (runs in background)
    // Delayed slightly to let ClickHouse connection establish
    setTimeout(checkAndDownloadBoundaryData, 3000);
});

// Graceful shutdown
let shuttingDown = false;
process.on('SIGINT', () => {
    if (shuttingDown) {
        console.log('\nForce quit...');
        process.exit(1);
    }
    shuttingDown = true;
    console.log('\nShutting down gracefully...');
    
    // Force exit after 2 seconds if shutdown hangs
    const forceExit = setTimeout(() => {
        console.log('Shutdown timeout - forcing exit');
        process.exit(1);
    }, 2000);
    
    server.close(() => {
        clearTimeout(forceExit);
        console.log('Server closed');
        process.exit(0);
    });
});
