/**
 * Region Service for Respiro
 * Handles region data aggregation using ClickHouse
 * Per architecture document docs/region-overlay-architecture.md
 */

class RegionService {
    constructor(clickHouseClient) {
        this.clickhouse = clickHouseClient;

        // ISO2 to ISO3 mapping for region_id generation
        // PMTiles uses ISO3 codes, sensor_readings uses ISO2
        this.iso2ToIso3 = {
            'NZ': 'NZL', 'AU': 'AUS', 'US': 'USA', 'GB': 'GBR', 'CA': 'CAN',
            'DE': 'DEU', 'FR': 'FRA', 'IT': 'ITA', 'ES': 'ESP', 'JP': 'JPN',
            'CN': 'CHN', 'IN': 'IND', 'BR': 'BRA', 'MX': 'MEX', 'AR': 'ARG',
            'ZA': 'ZAF', 'NG': 'NGA', 'EG': 'EGY', 'KE': 'KEN', 'GH': 'GHA',
            'AF': 'AFG', 'PK': 'PAK', 'BD': 'BGD', 'ID': 'IDN', 'MY': 'MYS',
            'SG': 'SGP', 'TH': 'THA', 'VN': 'VNM', 'PH': 'PHL', 'KR': 'KOR',
            'RU': 'RUS', 'UA': 'UKR', 'PL': 'POL', 'NL': 'NLD', 'BE': 'BEL',
            'CH': 'CHE', 'AT': 'AUT', 'SE': 'SWE', 'NO': 'NOR', 'DK': 'DNK',
            'FI': 'FIN', 'IE': 'IRL', 'PT': 'PRT', 'GR': 'GRC', 'CZ': 'CZE',
            'HU': 'HUN', 'RO': 'ROU', 'BG': 'BGR', 'HR': 'HRV', 'SK': 'SVK',
            'SI': 'SVN', 'EE': 'EST', 'LV': 'LVA', 'LT': 'LTU', 'CL': 'CHL',
            'CO': 'COL', 'PE': 'PER', 'VE': 'VEN', 'EC': 'ECU', 'UY': 'URY',
            'PY': 'PRY', 'BO': 'BOL'
        };
    }

    /**
     * Map zoom level to admin level
     * @param {number} zoom - Map zoom level (0-20)
     * @returns {number} Admin level (0, 1, 2, 3, or 4)
     *
     * Zoom mapping:
     * - Zoom 0-1:  ADM0 (countries)
     * - Zoom 2-4:  ADM1 (states/regions)
     * - Zoom 5-7:  ADM2 (districts)
     * - Zoom 8-10: ADM3 (sub-districts) - 81 countries have this
     * - Zoom 11+:  ADM4 (localities) - 21 countries have this
     *
     * Note: ADM3/ADM4 may not exist for all regions. The precomputed data
     * will simply be empty for regions without finer admin levels.
     */
    getAdminLevelForZoom(zoom) {
        // PMTiles has ADM0-3
        if (zoom <= 1) return 0;
        if (zoom <= 4) return 1;
        if (zoom <= 7) return 2;
        return 3;  // ADM3 for zoom 8+
    }

    /**
     * Get aggregated region data for the viewport
     * Per architecture Section 4.3 - Uses pointInPolygon for spatial matching
     *
     * Aggregation levels:
     * - ADM0 (countries): zoom ≤ 4
     * - ADM1 (states/regions): zoom 5-7
     * - ADM2 (districts): zoom ≥ 8
     */
    async getRegionData({ zoom, bounds, metric, range = '24h' }) {
        const adminLevel = this.getAdminLevelForZoom(zoom);
        const [south, west, north, east] = bounds.split(',').map(Number);

        // Map range string to interval
        const intervalMap = {
            '30m': '30 MINUTE',
            '1h': '1 HOUR',
            '2h': '2 HOUR',
            '4h': '4 HOUR',
            '8h': '8 HOUR',
            '24h': '24 HOUR',
            '7d': '7 DAY',
            '30d': '30 DAY'
        };
        const interval = intervalMap[range] || '30 MINUTE';

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

        try {
            // Different query strategies based on admin level
            // ADM0: Use geo_country field for efficiency (sensors already have country info)
            // ADM1/2: Use pointInPolygon spatial matching
            let query;

            if (adminLevel === 0) {
                // ADM0: Aggregate by geo_country, then join with region_boundaries
                // geo_country uses ISO2 (lowercase), country_code uses ISO3 (uppercase)
                // Use transform() to map ISO2 to ISO3
                query = `
                    SELECT
                        rb.region_id,
                        rb.name,
                        sr.avg_value,
                        sr.min_value,
                        sr.max_value,
                        sr.sensor_count,
                        sr.latest_timestamp
                    FROM (
                        SELECT
                            upper(geo_country) as geo_country_upper,
                            avg(value) as avg_value,
                            min(value) as min_value,
                            max(value) as max_value,
                            count(DISTINCT device_id) as sensor_count,
                            max(timestamp) as latest_timestamp
                        FROM wesense.sensor_readings
                        WHERE reading_type = {metric:String}
                          AND timestamp > now() - INTERVAL ${interval}
                          AND latitude BETWEEN {south:Float64} AND {north:Float64}
                          AND longitude BETWEEN {west:Float64} AND {east:Float64}
                          AND latitude != 0 AND longitude != 0
                          AND geo_country IS NOT NULL AND geo_country != ''
                        GROUP BY upper(geo_country)
                    ) AS sr
                    INNER JOIN wesense_respiro.region_boundaries rb
                      ON rb.admin_level = 0
                      AND rb.country_code = transform(sr.geo_country_upper,
                        ['NZ', 'AU', 'US', 'GB', 'CA', 'DE', 'FR', 'IT', 'ES', 'JP',
                         'CN', 'IN', 'BR', 'MX', 'AR', 'ZA', 'NG', 'EG', 'KE', 'GH',
                         'AF', 'PK', 'BD', 'ID', 'MY', 'SG', 'TH', 'VN', 'PH', 'KR',
                         'RU', 'UA', 'PL', 'NL', 'BE', 'CH', 'AT', 'SE', 'NO', 'DK',
                         'FI', 'IE', 'PT', 'GR', 'CZ', 'HU', 'RO', 'BG', 'HR', 'SK',
                         'SI', 'EE', 'LV', 'LT', 'CL', 'CO', 'PE', 'VE', 'EC', 'UY',
                         'PY', 'BO', 'TW'],
                        ['NZL', 'AUS', 'USA', 'GBR', 'CAN', 'DEU', 'FRA', 'ITA', 'ESP', 'JPN',
                         'CHN', 'IND', 'BRA', 'MEX', 'ARG', 'ZAF', 'NGA', 'EGY', 'KEN', 'GHA',
                         'AFG', 'PAK', 'BGD', 'IDN', 'MYS', 'SGP', 'THA', 'VNM', 'PHL', 'KOR',
                         'RUS', 'UKR', 'POL', 'NLD', 'BEL', 'CHE', 'AUT', 'SWE', 'NOR', 'DNK',
                         'FIN', 'IRL', 'PRT', 'GRC', 'CZE', 'HUN', 'ROU', 'BGR', 'HRV', 'SVK',
                         'SVN', 'EST', 'LVA', 'LTU', 'CHL', 'COL', 'PER', 'VEN', 'ECU', 'URY',
                         'PRY', 'BOL', 'TWN'],
                        sr.geo_country_upper)
                `;
            } else {
                // ADM1/2: Use pointInPolygon spatial matching
                // Note: ClickHouse doesn't support complex expressions in JOIN ON,
                // so we use CROSS JOIN with WHERE for the spatial matching
                query = `
                    SELECT
                        rb.region_id,
                        rb.name,
                        avg(sr.value) as avg_value,
                        min(sr.value) as min_value,
                        max(sr.value) as max_value,
                        count(DISTINCT sr.device_id) as sensor_count,
                        max(sr.timestamp) as latest_timestamp
                    FROM (
                        SELECT device_id, value, timestamp, latitude, longitude
                        FROM wesense.sensor_readings
                        WHERE reading_type = {metric:String}
                          AND timestamp > now() - INTERVAL ${interval}
                          AND latitude BETWEEN {south:Float64} AND {north:Float64}
                          AND longitude BETWEEN {west:Float64} AND {east:Float64}
                          AND latitude != 0
                          AND longitude != 0
                    ) AS sr
                    CROSS JOIN (
                        SELECT region_id, name, polygon, bbox_min_lon, bbox_max_lon, bbox_min_lat, bbox_max_lat
                        FROM wesense_respiro.region_boundaries
                        WHERE admin_level = {adminLevel:UInt8}
                          AND bbox_max_lon >= {west:Float64}
                          AND bbox_min_lon <= {east:Float64}
                          AND bbox_max_lat >= {south:Float64}
                          AND bbox_min_lat <= {north:Float64}
                    ) AS rb
                    WHERE rb.bbox_min_lon <= sr.longitude
                      AND rb.bbox_max_lon >= sr.longitude
                      AND rb.bbox_min_lat <= sr.latitude
                      AND rb.bbox_max_lat >= sr.latitude
                      AND pointInPolygon((sr.longitude, sr.latitude), rb.polygon[1])
                    GROUP BY rb.region_id, rb.name
                `;
            }

            const result = await this.clickhouse.query({
                query,
                query_params: { metric, south, north, west, east, adminLevel },
                format: 'JSONEachRow'
            });

            const rows = await result.json();

            const regions = {};
            for (const row of rows) {
                regions[row.region_id] = {
                    name: row.name,
                    avg: parseFloat(row.avg_value),
                    min: parseFloat(row.min_value),
                    max: parseFloat(row.max_value),
                    sensor_count: parseInt(row.sensor_count),
                    latest_timestamp: row.latest_timestamp
                };
            }

            console.log(`RegionService: Found ${Object.keys(regions).length} regions for ${metric} at admin level ${adminLevel}`);

            return {
                admin_level: adminLevel,
                metric,
                unit: units[metric] || '',
                time_range: range,
                regions
            };
        } catch (error) {
            console.error('Error querying region data:', error);
            throw error;
        }
    }

    /**
     * Pre-compute regional aggregates for ALL regions (no bounds filtering)
     * Used by background job to populate cache for instant serving
     *
     * @param {string} metric - The metric to compute (temperature, humidity, etc.)
     * @param {number} adminLevel - Admin level (0, 1, or 2)
     * @param {string} range - Time range (24h, 7d, etc.)
     * @param {string} deploymentFilter - 'outdoor', 'indoor', or 'all' (default: 'all')
     * @returns {Object} { regions: { region_id: { avg, min, max, sensor_count } } }
     */
    async precomputeRegions(metric, adminLevel, range = '24h', deploymentFilter = 'all') {
        const intervalMap = {
            '30m': '30 MINUTE',
            '1h': '1 HOUR',
            '2h': '2 HOUR',
            '4h': '4 HOUR',
            '8h': '8 HOUR',
            '24h': '24 HOUR',
            '7d': '7 DAY',
            '30d': '30 DAY'
        };
        const interval = intervalMap[range] || '30 MINUTE';

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

        // Build deployment filter HAVING clause
        // Uses argMaxIf to get each device's most recent NON-EMPTY deployment_type
        // This fixes the issue where new readings have empty deployment_type but device was already classified
        // Deployment types: OUTDOOR, INDOOR, PORTABLE, MIXED, MOBILE, DEVICE, UNKNOWN
        // Accepts comma-separated uppercase types (e.g., "OUTDOOR,MIXED") or legacy single values
        // 'all' or empty = no filter
        // 'outdoor' (legacy) = OUTDOOR + MIXED (for environmental monitoring, MIXED sensors are similar to outdoor)
        // 'UNKNOWN' matches NULL or empty deployment_type (device never classified)
        let deploymentHavingClause = '';
        if (deploymentFilter && deploymentFilter !== 'all') {
            // Expand 'outdoor' to include MIXED sensors (they behave similarly for env monitoring)
            let expandedFilter = deploymentFilter;
            if (deploymentFilter.toLowerCase() === 'outdoor') {
                expandedFilter = 'OUTDOOR,MIXED';
            }
            // Parse comma-separated types
            const types = expandedFilter.split(',').map(t => t.trim().toUpperCase()).filter(t => t);

            if (types.length > 0) {
                const includesUnknown = types.includes('UNKNOWN');
                const knownTypes = types.filter(t => t !== 'UNKNOWN');

                const conditions = [];
                if (includesUnknown) {
                    // UNKNOWN = device was never classified (all deployment_type rows are empty)
                    conditions.push("(device_deployment_type IS NULL OR device_deployment_type = '')");
                }
                if (knownTypes.length > 0) {
                    const typeList = knownTypes.map(t => `'${t}'`).join(',');
                    conditions.push(`device_deployment_type IN (${typeList})`);
                }

                if (conditions.length > 0) {
                    deploymentHavingClause = `HAVING ${conditions.join(' OR ')}`;
                }
            }
        }

        try {
            let query;

            if (adminLevel === 0) {
                // ADM0: Aggregate by geo_country globally (no bounds)
                // For deployment filtering, we look at ALL TIME to find each device's classification,
                // then aggregate only readings from the time window. This handles the case where
                // the classifier ran days ago and new readings don't have deployment_type set.
                query = `
                    WITH
                    -- First, get each device's deployment_type from ALL TIME (not just time window)
                    device_classifications AS (
                        SELECT
                            device_id,
                            argMaxIf(deployment_type, timestamp, deployment_type != '') as device_deployment_type
                        FROM wesense.sensor_readings
                        GROUP BY device_id
                        ${deploymentHavingClause}
                    ),
                    -- Then aggregate readings only from the time window, for classified devices
                    device_data AS (
                        SELECT
                            sr.device_id,
                            upper(argMax(sr.geo_country, sr.timestamp)) as geo_country_upper,
                            avg(sr.value) as avg_value,
                            min(sr.value) as min_value,
                            max(sr.value) as max_value,
                            max(sr.timestamp) as latest_timestamp
                        FROM wesense.sensor_readings sr
                        INNER JOIN device_classifications dc ON sr.device_id = dc.device_id
                        WHERE sr.reading_type = {metric:String}
                          AND sr.timestamp > now() - INTERVAL ${interval}
                          AND sr.latitude != 0 AND sr.longitude != 0
                          AND sr.geo_country IS NOT NULL AND sr.geo_country != ''
                        GROUP BY sr.device_id
                    )
                    SELECT
                        rb.region_id,
                        rb.name,
                        avg(dd.avg_value) as avg_value,
                        min(dd.min_value) as min_value,
                        max(dd.max_value) as max_value,
                        count(*) as sensor_count,
                        max(dd.latest_timestamp) as latest_timestamp
                    FROM device_data AS dd
                    INNER JOIN wesense_respiro.region_boundaries rb
                      ON rb.admin_level = 0
                      AND rb.country_code = transform(dd.geo_country_upper,
                        ['NZ', 'AU', 'US', 'GB', 'CA', 'DE', 'FR', 'IT', 'ES', 'JP',
                         'CN', 'IN', 'BR', 'MX', 'AR', 'ZA', 'NG', 'EG', 'KE', 'GH',
                         'AF', 'PK', 'BD', 'ID', 'MY', 'SG', 'TH', 'VN', 'PH', 'KR',
                         'RU', 'UA', 'PL', 'NL', 'BE', 'CH', 'AT', 'SE', 'NO', 'DK',
                         'FI', 'IE', 'PT', 'GR', 'CZ', 'HU', 'RO', 'BG', 'HR', 'SK',
                         'SI', 'EE', 'LV', 'LT', 'CL', 'CO', 'PE', 'VE', 'EC', 'UY',
                         'PY', 'BO', 'TW'],
                        ['NZL', 'AUS', 'USA', 'GBR', 'CAN', 'DEU', 'FRA', 'ITA', 'ESP', 'JPN',
                         'CHN', 'IND', 'BRA', 'MEX', 'ARG', 'ZAF', 'NGA', 'EGY', 'KEN', 'GHA',
                         'AFG', 'PAK', 'BGD', 'IDN', 'MYS', 'SGP', 'THA', 'VNM', 'PHL', 'KOR',
                         'RUS', 'UKR', 'POL', 'NLD', 'BEL', 'CHE', 'AUT', 'SWE', 'NOR', 'DNK',
                         'FIN', 'IRL', 'PRT', 'GRC', 'CZE', 'HUN', 'ROU', 'BGR', 'HRV', 'SVK',
                         'SVN', 'EST', 'LVA', 'LTU', 'CHL', 'COL', 'PER', 'VEN', 'ECU', 'URY',
                         'PRY', 'BOL', 'TWN'],
                        dd.geo_country_upper)
                    GROUP BY rb.region_id, rb.name
                `;
            } else {
                // ADM1/2/3/4: Use device_region_cache for fast lookups (no pointInPolygon!)
                // The cache maps each device to its ADM0/ADM1/ADM2/ADM3/ADM4 regions
                // For deployment filtering, we look at ALL TIME to find each device's classification,
                // then aggregate only readings from the time window.
                const regionColumnMap = {
                    1: 'region_adm1_id',
                    2: 'region_adm2_id',
                    3: 'region_adm3_id',
                    4: 'region_adm4_id'
                };
                const regionColumn = regionColumnMap[adminLevel] || 'region_adm2_id';

                query = `
                    WITH
                    -- First, get each device's deployment_type from ALL TIME (not just time window)
                    device_classifications AS (
                        SELECT
                            device_id,
                            argMaxIf(deployment_type, timestamp, deployment_type != '') as device_deployment_type
                        FROM wesense.sensor_readings
                        GROUP BY device_id
                        ${deploymentHavingClause}
                    ),
                    -- Then aggregate readings only from the time window, for classified devices
                    device_data AS (
                        SELECT
                            sr.device_id,
                            avg(sr.value) as avg_value,
                            min(sr.value) as min_value,
                            max(sr.value) as max_value,
                            max(sr.timestamp) as latest_timestamp
                        FROM wesense.sensor_readings sr
                        INNER JOIN device_classifications dc ON sr.device_id = dc.device_id
                        WHERE sr.reading_type = {metric:String}
                          AND sr.timestamp > now() - INTERVAL ${interval}
                          AND sr.latitude != 0
                          AND sr.longitude != 0
                        GROUP BY sr.device_id
                    )
                    SELECT
                        rb.region_id,
                        rb.name,
                        avg(dev.avg_value) as avg_value,
                        min(dev.min_value) as min_value,
                        max(dev.max_value) as max_value,
                        count(*) as sensor_count,
                        max(dev.latest_timestamp) as latest_timestamp
                    FROM device_data AS dev
                    INNER JOIN wesense_respiro.device_region_cache AS cache
                        ON dev.device_id = cache.device_id
                    INNER JOIN wesense_respiro.region_boundaries AS rb
                        ON rb.region_id = cache.${regionColumn}
                        AND rb.admin_level = {adminLevel:UInt8}
                    WHERE cache.${regionColumn} != ''
                    GROUP BY rb.region_id, rb.name
                `;
            }

            const result = await this.clickhouse.query({
                query,
                query_params: { metric, adminLevel },
                format: 'JSONEachRow'
            });

            const rows = await result.json();

            const regions = {};
            for (const row of rows) {
                regions[row.region_id] = {
                    name: row.name,
                    avg: parseFloat(row.avg_value),
                    min: parseFloat(row.min_value),
                    max: parseFloat(row.max_value),
                    sensor_count: parseInt(row.sensor_count),
                    latest_timestamp: row.latest_timestamp
                };
            }

            return {
                admin_level: adminLevel,
                metric,
                deployment_filter: deploymentFilter,
                unit: units[metric] || '',
                time_range: range,
                regions
            };
        } catch (error) {
            console.error(`Error precomputing regions for ${metric} ADM${adminLevel} (${deploymentFilter}):`, error);
            throw error;
        }
    }

    /**
     * Get historical region data for a specific timestamp
     * Used for historical heatmap queries (date picker + hour slider)
     *
     * @param {string} metric - The metric to query (temperature, humidity, etc.)
     * @param {number} adminLevel - Admin level (0, 1, 2, 3, or 4)
     * @param {string} timestamp - ISO8601 timestamp (center of time window)
     * @param {string} window - Time window size (default: '1h')
     * @param {string} deploymentFilter - 'outdoor', 'indoor', or 'all' (default: 'all')
     * @returns {Object} { regions: { region_id: { avg, min, max, sensor_count } } }
     */
    async getHistoricalRegionData(metric, adminLevel, timestamp, window = '1h', deploymentFilter = 'all') {
        // Parse window to milliseconds
        const windowMs = {
            '30m': 30 * 60 * 1000,
            '1h': 60 * 60 * 1000,
            '2h': 2 * 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '24h': 24 * 60 * 60 * 1000
        }[window] || 60 * 60 * 1000;

        // Calculate start and end times (window centered on timestamp)
        const centerTime = new Date(timestamp).getTime();
        const startTime = new Date(centerTime - windowMs / 2).toISOString().replace('T', ' ').replace('Z', '');
        const endTime = new Date(centerTime + windowMs / 2).toISOString().replace('T', ' ').replace('Z', '');

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

        // Build deployment filter HAVING clause (same as precomputeRegions)
        let deploymentHavingClause = '';
        if (deploymentFilter && deploymentFilter !== 'all') {
            let expandedFilter = deploymentFilter;
            if (deploymentFilter.toLowerCase() === 'outdoor') {
                expandedFilter = 'OUTDOOR,MIXED';
            }
            const types = expandedFilter.split(',').map(t => t.trim().toUpperCase()).filter(t => t);

            if (types.length > 0) {
                const includesUnknown = types.includes('UNKNOWN');
                const knownTypes = types.filter(t => t !== 'UNKNOWN');

                const conditions = [];
                if (includesUnknown) {
                    conditions.push("(device_deployment_type IS NULL OR device_deployment_type = '')");
                }
                if (knownTypes.length > 0) {
                    const typeList = knownTypes.map(t => `'${t}'`).join(',');
                    conditions.push(`device_deployment_type IN (${typeList})`);
                }

                if (conditions.length > 0) {
                    deploymentHavingClause = `HAVING ${conditions.join(' OR ')}`;
                }
            }
        }

        try {
            let query;

            if (adminLevel === 0) {
                // ADM0: Aggregate by geo_country globally
                query = `
                    WITH
                    device_classifications AS (
                        SELECT
                            device_id,
                            argMaxIf(deployment_type, timestamp, deployment_type != '') as device_deployment_type
                        FROM wesense.sensor_readings
                        GROUP BY device_id
                        ${deploymentHavingClause}
                    ),
                    device_data AS (
                        SELECT
                            sr.device_id,
                            upper(argMax(sr.geo_country, sr.timestamp)) as geo_country_upper,
                            avg(sr.value) as avg_value,
                            min(sr.value) as min_value,
                            max(sr.value) as max_value,
                            max(sr.timestamp) as latest_timestamp
                        FROM wesense.sensor_readings sr
                        INNER JOIN device_classifications dc ON sr.device_id = dc.device_id
                        WHERE sr.reading_type = {metric:String}
                          AND sr.timestamp BETWEEN {startTime:String} AND {endTime:String}
                          AND sr.latitude != 0 AND sr.longitude != 0
                          AND sr.geo_country IS NOT NULL AND sr.geo_country != ''
                        GROUP BY sr.device_id
                    )
                    SELECT
                        rb.region_id,
                        rb.name,
                        avg(dd.avg_value) as avg_value,
                        min(dd.min_value) as min_value,
                        max(dd.max_value) as max_value,
                        count(*) as sensor_count,
                        max(dd.latest_timestamp) as latest_timestamp
                    FROM device_data AS dd
                    INNER JOIN wesense_respiro.region_boundaries rb
                      ON rb.admin_level = 0
                      AND rb.country_code = transform(dd.geo_country_upper,
                        ['NZ', 'AU', 'US', 'GB', 'CA', 'DE', 'FR', 'IT', 'ES', 'JP',
                         'CN', 'IN', 'BR', 'MX', 'AR', 'ZA', 'NG', 'EG', 'KE', 'GH',
                         'AF', 'PK', 'BD', 'ID', 'MY', 'SG', 'TH', 'VN', 'PH', 'KR',
                         'RU', 'UA', 'PL', 'NL', 'BE', 'CH', 'AT', 'SE', 'NO', 'DK',
                         'FI', 'IE', 'PT', 'GR', 'CZ', 'HU', 'RO', 'BG', 'HR', 'SK',
                         'SI', 'EE', 'LV', 'LT', 'CL', 'CO', 'PE', 'VE', 'EC', 'UY',
                         'PY', 'BO', 'TW'],
                        ['NZL', 'AUS', 'USA', 'GBR', 'CAN', 'DEU', 'FRA', 'ITA', 'ESP', 'JPN',
                         'CHN', 'IND', 'BRA', 'MEX', 'ARG', 'ZAF', 'NGA', 'EGY', 'KEN', 'GHA',
                         'AFG', 'PAK', 'BGD', 'IDN', 'MYS', 'SGP', 'THA', 'VNM', 'PHL', 'KOR',
                         'RUS', 'UKR', 'POL', 'NLD', 'BEL', 'CHE', 'AUT', 'SWE', 'NOR', 'DNK',
                         'FIN', 'IRL', 'PRT', 'GRC', 'CZE', 'HUN', 'ROU', 'BGR', 'HRV', 'SVK',
                         'SVN', 'EST', 'LVA', 'LTU', 'CHL', 'COL', 'PER', 'VEN', 'ECU', 'URY',
                         'PRY', 'BOL', 'TWN'],
                        dd.geo_country_upper)
                    GROUP BY rb.region_id, rb.name
                `;
            } else {
                // ADM1/2/3/4: Use device_region_cache for fast lookups
                const regionColumnMap = {
                    1: 'region_adm1_id',
                    2: 'region_adm2_id',
                    3: 'region_adm3_id',
                    4: 'region_adm4_id'
                };
                const regionColumn = regionColumnMap[adminLevel] || 'region_adm2_id';

                query = `
                    WITH
                    device_classifications AS (
                        SELECT
                            device_id,
                            argMaxIf(deployment_type, timestamp, deployment_type != '') as device_deployment_type
                        FROM wesense.sensor_readings
                        GROUP BY device_id
                        ${deploymentHavingClause}
                    ),
                    device_data AS (
                        SELECT
                            sr.device_id,
                            avg(sr.value) as avg_value,
                            min(sr.value) as min_value,
                            max(sr.value) as max_value,
                            max(sr.timestamp) as latest_timestamp
                        FROM wesense.sensor_readings sr
                        INNER JOIN device_classifications dc ON sr.device_id = dc.device_id
                        WHERE sr.reading_type = {metric:String}
                          AND sr.timestamp BETWEEN {startTime:String} AND {endTime:String}
                          AND sr.latitude != 0
                          AND sr.longitude != 0
                        GROUP BY sr.device_id
                    )
                    SELECT
                        rb.region_id,
                        rb.name,
                        avg(dev.avg_value) as avg_value,
                        min(dev.min_value) as min_value,
                        max(dev.max_value) as max_value,
                        count(*) as sensor_count,
                        max(dev.latest_timestamp) as latest_timestamp
                    FROM device_data AS dev
                    INNER JOIN wesense_respiro.device_region_cache AS cache
                        ON dev.device_id = cache.device_id
                    INNER JOIN wesense_respiro.region_boundaries AS rb
                        ON rb.region_id = cache.${regionColumn}
                        AND rb.admin_level = {adminLevel:UInt8}
                    WHERE cache.${regionColumn} != ''
                    GROUP BY rb.region_id, rb.name
                `;
            }

            const result = await this.clickhouse.query({
                query,
                query_params: { metric, adminLevel, startTime, endTime },
                format: 'JSONEachRow'
            });

            const rows = await result.json();

            const regions = {};
            for (const row of rows) {
                regions[row.region_id] = {
                    name: row.name,
                    avg: parseFloat(row.avg_value),
                    min: parseFloat(row.min_value),
                    max: parseFloat(row.max_value),
                    sensor_count: parseInt(row.sensor_count),
                    latest_timestamp: row.latest_timestamp
                };
            }

            return {
                admin_level: adminLevel,
                metric,
                deployment_filter: deploymentFilter,
                unit: units[metric] || '',
                time_window: window,
                query_start: startTime,
                query_end: endTime,
                regions
            };
        } catch (error) {
            console.error(`Error querying historical regions for ${metric} ADM${adminLevel} at ${timestamp}:`, error);
            throw error;
        }
    }

    /**
     * Get historical data for a SINGLE region (fast - for popup queries)
     * Unlike getHistoricalRegionData which queries ALL regions, this only queries one
     */
    async getHistoricalDataForSingleRegion(regionId, metric, timestamp, window = '1h', deploymentFilter = 'all') {
        // Parse admin level from region_id: NZL_ADM1_35688438 -> 1
        const admMatch = regionId.match(/_ADM(\d)_/);
        const adminLevel = admMatch ? parseInt(admMatch[1]) : null;
        if (adminLevel === null) return null;

        const windowMs = {
            '30m': 30 * 60 * 1000,
            '1h': 60 * 60 * 1000,
            '2h': 2 * 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '24h': 24 * 60 * 60 * 1000
        }[window] || 60 * 60 * 1000;

        const centerTime = new Date(timestamp).getTime();
        const startTime = new Date(centerTime - windowMs / 2).toISOString().replace('T', ' ').replace('Z', '');
        const endTime = new Date(centerTime + windowMs / 2).toISOString().replace('T', ' ').replace('Z', '');

        // Build deployment filter
        let deploymentHavingClause = '';
        if (deploymentFilter && deploymentFilter !== 'all') {
            let expandedFilter = deploymentFilter;
            if (deploymentFilter.toLowerCase() === 'outdoor') {
                expandedFilter = 'OUTDOOR,MIXED';
            }
            const types = expandedFilter.split(',').map(t => t.trim().toUpperCase()).filter(t => t);
            if (types.length > 0) {
                const conditions = [];
                if (types.includes('UNKNOWN')) {
                    conditions.push("(device_deployment_type IS NULL OR device_deployment_type = '')");
                }
                const knownTypes = types.filter(t => t !== 'UNKNOWN');
                if (knownTypes.length > 0) {
                    conditions.push(`device_deployment_type IN (${knownTypes.map(t => `'${t}'`).join(',')})`);
                }
                if (conditions.length > 0) {
                    deploymentHavingClause = `HAVING ${conditions.join(' OR ')}`;
                }
            }
        }

        try {
            const regionColumnMap = { 1: 'region_adm1_id', 2: 'region_adm2_id', 3: 'region_adm3_id', 4: 'region_adm4_id' };
            const regionColumn = regionColumnMap[adminLevel] || 'region_adm2_id';

            const query = `
                WITH
                device_classifications AS (
                    SELECT device_id,
                           argMaxIf(deployment_type, timestamp, deployment_type != '') as device_deployment_type
                    FROM wesense.sensor_readings
                    GROUP BY device_id
                    ${deploymentHavingClause}
                ),
                device_data AS (
                    SELECT sr.device_id,
                           avg(sr.value) as avg_value,
                           min(sr.value) as min_value,
                           max(sr.value) as max_value,
                           max(sr.timestamp) as latest_timestamp
                    FROM wesense.sensor_readings sr
                    INNER JOIN device_classifications dc ON sr.device_id = dc.device_id
                    WHERE sr.reading_type = {metric:String}
                      AND sr.timestamp BETWEEN {startTime:String} AND {endTime:String}
                      AND sr.latitude != 0 AND sr.longitude != 0
                    GROUP BY sr.device_id
                )
                SELECT
                    avg(dev.avg_value) as avg_value,
                    min(dev.min_value) as min_value,
                    max(dev.max_value) as max_value,
                    count(*) as sensor_count,
                    max(dev.latest_timestamp) as latest_timestamp
                FROM device_data AS dev
                INNER JOIN wesense_respiro.device_region_cache AS cache
                    ON dev.device_id = cache.device_id
                WHERE cache.${regionColumn} = {regionId:String}
            `;

            const result = await this.clickhouse.query({
                query,
                query_params: { metric, regionId, startTime, endTime },
                format: 'JSONEachRow'
            });

            const rows = await result.json();
            if (rows.length === 0 || !rows[0].sensor_count) return null;

            const row = rows[0];
            return {
                avg: parseFloat(row.avg_value),
                min: parseFloat(row.min_value),
                max: parseFloat(row.max_value),
                sensor_count: parseInt(row.sensor_count),
                latest_timestamp: row.latest_timestamp
            };
        } catch (error) {
            console.error(`Error querying historical data for region ${regionId}:`, error);
            return null;
        }
    }

    /**
     * Refresh the device-to-region cache
     * Only processes devices that are new or have moved since last cache update
     * Uses efficient bulk INSERT instead of per-device queries
     */
    async refreshDeviceRegionCache() {
        console.log('Refreshing device region cache...');
        const startTime = Date.now();

        try {
            // Step 1: Find devices that need updating (new or moved)
            // Compare current device locations with cached locations
            const newDevicesQuery = `
                WITH current_locations AS (
                    SELECT
                        device_id,
                        argMax(latitude, timestamp) as lat,
                        argMax(longitude, timestamp) as lon
                    FROM wesense.sensor_readings
                    WHERE latitude != 0 AND longitude != 0
                      AND timestamp > now() - INTERVAL 30 DAY
                    GROUP BY device_id
                )
                SELECT
                    cl.device_id,
                    cl.lat,
                    cl.lon
                FROM current_locations cl
                LEFT JOIN wesense_respiro.device_region_cache cache
                    ON cl.device_id = cache.device_id
                WHERE cache.device_id IS NULL
                   OR abs(cl.lat - cache.latitude) > 0.001
                   OR abs(cl.lon - cache.longitude) > 0.001
            `;

            const result = await this.clickhouse.query({
                query: newDevicesQuery,
                format: 'JSONEachRow'
            });
            const devicesToUpdate = await result.json();

            if (devicesToUpdate.length === 0) {
                console.log('Device region cache: No new or moved devices to process');
                return { updated: 0, duration_ms: Date.now() - startTime };
            }

            console.log(`Device region cache: Processing ${devicesToUpdate.length} new/moved devices...`);

            // Step 2: Insert each device with region lookup
            // Process in batches for progress reporting
            const BATCH_SIZE = 50;
            let processed = 0;

            for (let i = 0; i < devicesToUpdate.length; i += BATCH_SIZE) {
                const batch = devicesToUpdate.slice(i, i + BATCH_SIZE);

                // Process batch in parallel
                await Promise.all(batch.map(async (device) => {
                    // Insert with region lookups for ADM0-4
                    // ADM3 (81 countries) and ADM4 (21 countries) may return empty for many locations
                    const insertQuery = `
                        INSERT INTO wesense_respiro.device_region_cache
                        (device_id, latitude, longitude, region_adm0_id, region_adm1_id, region_adm2_id, region_adm3_id, region_adm4_id, updated_at)
                        SELECT
                            {device_id:String} as device_id,
                            {lat:Float64} as latitude,
                            {lon:Float64} as longitude,
                            (SELECT region_id FROM wesense_respiro.region_boundaries
                             WHERE admin_level = 0
                               AND bbox_min_lon <= {lon:Float64} AND bbox_max_lon >= {lon:Float64}
                               AND bbox_min_lat <= {lat:Float64} AND bbox_max_lat >= {lat:Float64}
                               AND pointInPolygon(({lon:Float64}, {lat:Float64}), polygon[1])
                             LIMIT 1) as region_adm0_id,
                            (SELECT region_id FROM wesense_respiro.region_boundaries
                             WHERE admin_level = 1
                               AND bbox_min_lon <= {lon:Float64} AND bbox_max_lon >= {lon:Float64}
                               AND bbox_min_lat <= {lat:Float64} AND bbox_max_lat >= {lat:Float64}
                               AND pointInPolygon(({lon:Float64}, {lat:Float64}), polygon[1])
                             LIMIT 1) as region_adm1_id,
                            (SELECT region_id FROM wesense_respiro.region_boundaries
                             WHERE admin_level = 2
                               AND bbox_min_lon <= {lon:Float64} AND bbox_max_lon >= {lon:Float64}
                               AND bbox_min_lat <= {lat:Float64} AND bbox_max_lat >= {lat:Float64}
                               AND pointInPolygon(({lon:Float64}, {lat:Float64}), polygon[1])
                             LIMIT 1) as region_adm2_id,
                            (SELECT region_id FROM wesense_respiro.region_boundaries
                             WHERE admin_level = 3
                               AND bbox_min_lon <= {lon:Float64} AND bbox_max_lon >= {lon:Float64}
                               AND bbox_min_lat <= {lat:Float64} AND bbox_max_lat >= {lat:Float64}
                               AND pointInPolygon(({lon:Float64}, {lat:Float64}), polygon[1])
                             LIMIT 1) as region_adm3_id,
                            (SELECT region_id FROM wesense_respiro.region_boundaries
                             WHERE admin_level = 4
                               AND bbox_min_lon <= {lon:Float64} AND bbox_max_lon >= {lon:Float64}
                               AND bbox_min_lat <= {lat:Float64} AND bbox_max_lat >= {lat:Float64}
                               AND pointInPolygon(({lon:Float64}, {lat:Float64}), polygon[1])
                             LIMIT 1) as region_adm4_id,
                            now() as updated_at
                    `;

                    await this.clickhouse.command({
                        query: insertQuery,
                        query_params: {
                            device_id: device.device_id,
                            lat: device.lat,
                            lon: device.lon
                        }
                    });
                }));

                processed += batch.length;

                if (devicesToUpdate.length > BATCH_SIZE) {
                    console.log(`  Processed ${processed}/${devicesToUpdate.length} devices...`);
                }
            }

            const duration = Date.now() - startTime;
            console.log(`Device region cache refreshed: ${processed} devices in ${duration}ms`);
            return { updated: processed, duration_ms: duration };
        } catch (error) {
            console.error('Error refreshing device region cache:', error);
            throw error;
        }
    }

    /**
     * Get cache statistics
     */
    async getDeviceRegionCacheStats() {
        try {
            const query = `
                SELECT
                    count() as total_devices,
                    countIf(region_adm0_id != '') as with_adm0,
                    countIf(region_adm1_id != '') as with_adm1,
                    countIf(region_adm2_id != '') as with_adm2,
                    countIf(region_adm3_id != '') as with_adm3,
                    countIf(region_adm4_id != '') as with_adm4,
                    min(updated_at) as oldest_entry,
                    max(updated_at) as newest_entry
                FROM wesense_respiro.device_region_cache
            `;
            const result = await this.clickhouse.query({ query, format: 'JSONEachRow' });
            const rows = await result.json();
            return rows[0] || null;
        } catch (error) {
            console.error('Error getting cache stats:', error);
            return null;
        }
    }

    /**
     * Check if boundary data exists for each admin level
     * Returns counts and warns if ADM3/ADM4 are missing
     * @returns {Object} Counts per admin level and whether setup is needed
     */
    async checkBoundaryData() {
        try {
            const query = `
                SELECT admin_level, count() as cnt
                FROM wesense_respiro.region_boundaries
                GROUP BY admin_level
                ORDER BY admin_level
            `;
            const result = await this.clickhouse.query({ query, format: 'JSONEachRow' });
            const rows = await result.json();

            const counts = { 0: 0, 1: 0, 2: 0, 3: 0, 4: 0 };
            rows.forEach(row => {
                counts[row.admin_level] = row.cnt;
            });

            const total = Object.values(counts).reduce((a, b) => a + b, 0);

            if (total === 0) {
                console.log('\n[BOUNDARIES] ⚠️  No boundary data found in ClickHouse!');
                console.log('[BOUNDARIES] Run: npm run setup-boundaries');
                console.log('[BOUNDARIES] Region overlay will not work until boundaries are loaded.\n');
                return { counts, needsSetup: true, severity: 'critical' };
            }

            if (counts[3] === 0 && counts[4] === 0) {
                console.log('\n[BOUNDARIES] ⚠️  ADM3/ADM4 boundary data missing');
                console.log(`[BOUNDARIES] Current: ADM0=${counts[0]}, ADM1=${counts[1]}, ADM2=${counts[2]}`);
                console.log('[BOUNDARIES] Run: npm run setup-boundaries (to add ADM3/ADM4)');
                console.log('[BOUNDARIES] Zoom 8+ will fall back to ADM2 until boundaries are loaded.\n');
                return { counts, needsSetup: true, severity: 'warning' };
            }

            console.log(`[BOUNDARIES] OK: ADM0=${counts[0]}, ADM1=${counts[1]}, ADM2=${counts[2]}, ADM3=${counts[3]}, ADM4=${counts[4]}`);
            return { counts, needsSetup: false, severity: 'ok' };
        } catch (error) {
            console.error('[BOUNDARIES] Error checking boundary data:', error.message);
            return { counts: {}, needsSetup: true, severity: 'error' };
        }
    }

    /**
     * Detect and fix corrupted device_region_cache
     * The cache can become corrupted if the table schema has columns in wrong order
     * (e.g., updated_at before region_adm3_id) causing data to shift to wrong columns.
     * This detects various corruption patterns and auto-fixes.
     * @returns {boolean} True if corruption was detected and fixed
     */
    async detectAndFixCorruptedCache() {
        try {
            // Check for multiple corruption patterns:
            // 1. region_adm4_id contains timestamps (e.g., "2025-12-27 07:49:21")
            // 2. region_adm3_id contains ADM4 IDs (wrong level)
            // 3. region_adm2_id contains ADM3 IDs (wrong level)
            // 4. region_adm1_id contains ADM2 IDs (wrong level)
            const checkQuery = `
                SELECT
                    countIf(region_adm4_id LIKE '%-%-%:%:%') as timestamps_in_adm4,
                    countIf(region_adm3_id LIKE '%_ADM4_%') as adm4_in_adm3,
                    countIf(region_adm2_id LIKE '%_ADM3_%') as adm3_in_adm2,
                    countIf(region_adm1_id LIKE '%_ADM2_%') as adm2_in_adm1
                FROM wesense_respiro.device_region_cache
            `;
            const result = await this.clickhouse.query({ query: checkQuery, format: 'JSONEachRow' });
            const rows = await result.json();
            const stats = rows[0] || {};

            const corruptions = [];
            if (stats.timestamps_in_adm4 > 0) corruptions.push(`${stats.timestamps_in_adm4} timestamps in region_adm4_id`);
            if (stats.adm4_in_adm3 > 0) corruptions.push(`${stats.adm4_in_adm3} ADM4 IDs in region_adm3_id`);
            if (stats.adm3_in_adm2 > 0) corruptions.push(`${stats.adm3_in_adm2} ADM3 IDs in region_adm2_id`);
            if (stats.adm2_in_adm1 > 0) corruptions.push(`${stats.adm2_in_adm1} ADM2 IDs in region_adm1_id`);

            if (corruptions.length > 0) {
                console.log(`[CACHE FIX] Detected corrupted device_region_cache:`);
                corruptions.forEach(c => console.log(`  - ${c}`));
                console.log('[CACHE FIX] This was caused by table schema column order mismatch. Truncating and rebuilding...');

                await this.clickhouse.command({ query: 'TRUNCATE TABLE wesense_respiro.device_region_cache' });

                console.log('[CACHE FIX] Cache cleared. Will rebuild on next refresh cycle.');
                return true;
            }

            return false;
        } catch (error) {
            console.error('Error checking for cache corruption:', error.message);
            return false;
        }
    }

    /**
     * Find which regions contain a given point
     * Returns region info for all admin levels (ADM0, ADM1, ADM2, ADM3, ADM4)
     * Note: ADM3/ADM4 may not exist for all locations (81/21 countries respectively)
     * @param {number} lat - Latitude
     * @param {number} lng - Longitude
     * @returns {Object} Regions at each admin level
     */
    async getRegionsAtPoint(lat, lng) {
        try {
            const query = `
                SELECT
                    admin_level,
                    region_id,
                    name,
                    country_code
                FROM wesense_respiro.region_boundaries
                WHERE bbox_min_lon <= {lng:Float64}
                  AND bbox_max_lon >= {lng:Float64}
                  AND bbox_min_lat <= {lat:Float64}
                  AND bbox_max_lat >= {lat:Float64}
                  AND pointInPolygon(({lng:Float64}, {lat:Float64}), polygon[1])
                ORDER BY admin_level
            `;

            const result = await this.clickhouse.query({
                query,
                query_params: { lat, lng },
                format: 'JSONEachRow'
            });

            const rows = await result.json();

            const regions = {
                adm0: null,
                adm1: null,
                adm2: null
            };

            for (const row of rows) {
                const key = `adm${row.admin_level}`;
                regions[key] = {
                    region_id: row.region_id,
                    name: row.name,
                    country_code: row.country_code
                };
            }

            return regions;
        } catch (error) {
            console.error(`Error finding regions at point (${lat}, ${lng}):`, error);
            return { adm0: null, adm1: null, adm2: null };
        }
    }

    /**
     * Get top regions by sensor count at a specific admin level
     * Used for leaderboard display
     * @param {number} adminLevel - Admin level (0, 1, or 2)
     * @param {string} range - Time range (24h, 7d, etc.)
     * @param {number} limit - Max number of regions to return (default: 5)
     * @returns {Object} { regions: [...], total_regions: number }
     */
    async getTopRegions(adminLevel = 2, range = '24h', limit = 5) {
        const intervalMap = {
            '30m': '30 MINUTE',
            '1h': '1 HOUR',
            '2h': '2 HOUR',
            '4h': '4 HOUR',
            '8h': '8 HOUR',
            '24h': '24 HOUR',
            '7d': '7 DAY',
            '30d': '30 DAY'
        };
        const interval = intervalMap[range] || '24 HOUR';

        try {
            // Use device_region_cache for fast lookups (no pointInPolygon!)
            const regionColumnMap = {
                0: 'region_adm0_id', 1: 'region_adm1_id', 2: 'region_adm2_id',
                3: 'region_adm3_id', 4: 'region_adm4_id'
            };
            const regionColumn = regionColumnMap[adminLevel] || 'region_adm2_id';

            const query = `
                WITH active_devices AS (
                    SELECT DISTINCT device_id
                    FROM wesense.sensor_readings
                    WHERE timestamp > now() - INTERVAL ${interval}
                      AND latitude != 0
                      AND longitude != 0
                ),
                region_counts AS (
                    SELECT
                        rb.region_id,
                        rb.name,
                        rb.country_code,
                        (rb.bbox_min_lat + rb.bbox_max_lat) / 2 as center_lat,
                        (rb.bbox_min_lon + rb.bbox_max_lon) / 2 as center_lon,
                        count(*) as sensor_count
                    FROM active_devices AS dev
                    INNER JOIN wesense_respiro.device_region_cache AS cache
                        ON dev.device_id = cache.device_id
                    INNER JOIN wesense_respiro.region_boundaries AS rb
                        ON rb.region_id = cache.${regionColumn}
                        AND rb.admin_level = {adminLevel:UInt8}
                    WHERE cache.${regionColumn} != ''
                    GROUP BY rb.region_id, rb.name, rb.country_code, center_lat, center_lon
                )
                SELECT
                    region_id,
                    name,
                    country_code,
                    sensor_count,
                    center_lat,
                    center_lon,
                    (SELECT count() FROM region_counts) as total_regions
                FROM region_counts
                ORDER BY sensor_count DESC
                LIMIT {limit:UInt32}
            `;

            const result = await this.clickhouse.query({
                query,
                query_params: { adminLevel, limit },
                format: 'JSONEachRow'
            });

            const rows = await result.json();

            // Get total_regions from first row (same for all rows)
            const totalRegions = rows.length > 0 ? parseInt(rows[0].total_regions) : 0;

            return {
                regions: rows.map(row => ({
                    region_id: row.region_id,
                    name: row.name,
                    country_code: row.country_code,
                    sensor_count: parseInt(row.sensor_count),
                    lat: parseFloat(row.center_lat),
                    lon: parseFloat(row.center_lon)
                })),
                total_regions: totalRegions
            };
        } catch (error) {
            console.error(`Error getting top regions at ADM${adminLevel}:`, error);
            return { regions: [], total_regions: 0 };
        }
    }

    /**
     * Get top regions by unique sensor count (device + reading_type combinations)
     * A "sensor" is a physical sensor on a device (e.g., temperature sensor, humidity sensor)
     */
    async getTopRegionsBySensors(adminLevel = 2, range = '24h', limit = 5) {
        const intervalMap = {
            '30m': '30 MINUTE', '1h': '1 HOUR', '2h': '2 HOUR', '4h': '4 HOUR',
            '8h': '8 HOUR', '24h': '24 HOUR', '7d': '7 DAY', '30d': '30 DAY'
        };
        const interval = intervalMap[range] || '24 HOUR';

        try {
            const regionColumnMap = { 0: 'region_adm0_id', 1: 'region_adm1_id', 2: 'region_adm2_id', 3: 'region_adm3_id', 4: 'region_adm4_id' };
            const regionColumn = regionColumnMap[adminLevel] || 'region_adm2_id';

            // Count unique (device_id, reading_type) pairs = individual sensors
            const query = `
                WITH region_sensors AS (
                    SELECT
                        rb.region_id,
                        rb.name,
                        rb.country_code,
                        (rb.bbox_min_lat + rb.bbox_max_lat) / 2 as center_lat,
                        (rb.bbox_min_lon + rb.bbox_max_lon) / 2 as center_lon,
                        uniqExact(sr.device_id, sr.reading_type) as sensor_count
                    FROM wesense.sensor_readings AS sr
                    INNER JOIN wesense_respiro.device_region_cache AS cache
                        ON sr.device_id = cache.device_id
                    INNER JOIN wesense_respiro.region_boundaries AS rb
                        ON rb.region_id = cache.${regionColumn}
                        AND rb.admin_level = {adminLevel:UInt8}
                    WHERE sr.timestamp > now() - INTERVAL ${interval}
                      AND sr.latitude != 0 AND sr.longitude != 0
                      AND cache.${regionColumn} != ''
                    GROUP BY rb.region_id, rb.name, rb.country_code, center_lat, center_lon
                )
                SELECT * FROM region_sensors
                ORDER BY sensor_count DESC
                LIMIT {limit:UInt32}
            `;

            const result = await this.clickhouse.query({
                query,
                query_params: { adminLevel, limit },
                format: 'JSONEachRow'
            });

            const rows = await result.json();
            return {
                regions: rows.map(row => ({
                    region_id: row.region_id,
                    name: row.name,
                    country_code: row.country_code,
                    sensor_count: parseInt(row.sensor_count),
                    lat: parseFloat(row.center_lat),
                    lon: parseFloat(row.center_lon)
                }))
            };
        } catch (error) {
            console.error(`Error getting top regions by sensors at ADM${adminLevel}:`, error);
            return { regions: [] };
        }
    }

    /**
     * Get top regions by unique sensor type count
     */
    async getTopRegionsBySensorTypes(adminLevel = 2, range = '24h', limit = 5) {
        const intervalMap = {
            '30m': '30 MINUTE', '1h': '1 HOUR', '2h': '2 HOUR', '4h': '4 HOUR',
            '8h': '8 HOUR', '24h': '24 HOUR', '7d': '7 DAY', '30d': '30 DAY'
        };
        const interval = intervalMap[range] || '24 HOUR';

        try {
            const regionColumnMap = { 0: 'region_adm0_id', 1: 'region_adm1_id', 2: 'region_adm2_id', 3: 'region_adm3_id', 4: 'region_adm4_id' };
            const regionColumn = regionColumnMap[adminLevel] || 'region_adm2_id';

            const query = `
                WITH region_types AS (
                    SELECT
                        rb.region_id,
                        rb.name,
                        rb.country_code,
                        (rb.bbox_min_lat + rb.bbox_max_lat) / 2 as center_lat,
                        (rb.bbox_min_lon + rb.bbox_max_lon) / 2 as center_lon,
                        uniqExact(sr.reading_type) as type_count
                    FROM wesense.sensor_readings AS sr
                    INNER JOIN wesense_respiro.device_region_cache AS cache
                        ON sr.device_id = cache.device_id
                    INNER JOIN wesense_respiro.region_boundaries AS rb
                        ON rb.region_id = cache.${regionColumn}
                        AND rb.admin_level = {adminLevel:UInt8}
                    WHERE sr.timestamp > now() - INTERVAL ${interval}
                      AND sr.latitude != 0 AND sr.longitude != 0
                      AND cache.${regionColumn} != ''
                    GROUP BY rb.region_id, rb.name, rb.country_code, center_lat, center_lon
                )
                SELECT * FROM region_types
                ORDER BY type_count DESC
                LIMIT {limit:UInt32}
            `;

            const result = await this.clickhouse.query({
                query,
                query_params: { adminLevel, limit },
                format: 'JSONEachRow'
            });

            const rows = await result.json();
            return {
                regions: rows.map(row => ({
                    region_id: row.region_id,
                    name: row.name,
                    country_code: row.country_code,
                    type_count: parseInt(row.type_count),
                    lat: parseFloat(row.center_lat),
                    lon: parseFloat(row.center_lon)
                }))
            };
        } catch (error) {
            console.error(`Error getting top regions by sensor types at ADM${adminLevel}:`, error);
            return { regions: [] };
        }
    }

    /**
     * Get environmental leaderboards (cleanest air, best weather, etc.)
     * Uses 30-day data for more stable rankings
     */
    async getEnvironmentalLeaderboards(limit = 5) {
        try {
            // Run all queries in parallel
            const [outdoorAir, indoorAir, bestWeather, mostStable, hottest] = await Promise.all([
                this.getCleanestOutdoorAirTowns(limit),
                this.getCleanestIndoorAirTowns(limit),
                this.getBestWeatherTowns(limit),
                this.getMostStableClimateTowns(limit),
                this.getHottestTowns(limit)
            ]);

            return {
                outdoorAir,
                indoorAir,
                bestWeather,
                mostStable,
                hottest
            };
        } catch (error) {
            console.error('Error getting environmental leaderboards:', error);
            return { cleanestAir: [], bestWeather: [], mostStable: [], hottest: [] };
        }
    }

    /**
     * Calculate outdoor air quality score (0-100, higher = cleaner)
     * Based on WHO guidelines: PM2.5 15µg/m³, PM10 45µg/m³, NOx index 100 baseline
     */
    calculateOutdoorAirScore(pm25, pm10, nox) {
        let scores = [];
        // PM2.5: WHO guideline 15µg/m³ = score 0, 0µg/m³ = score 100
        if (pm25 !== null) {
            scores.push(Math.max(0, Math.min(100, 100 - (pm25 / 15 * 100))));
        }
        // PM10: WHO guideline 45µg/m³ = score 0, 0µg/m³ = score 100
        if (pm10 !== null) {
            scores.push(Math.max(0, Math.min(100, 100 - (pm10 / 45 * 100))));
        }
        // NOx: SGP sensor baseline 100 = normal (score 50), 1 = excellent (score 100), 200 = poor (score 0)
        if (nox !== null) {
            scores.push(Math.max(0, Math.min(100, 100 - (nox / 2))));
        }
        if (scores.length === 0) return null;
        // Average all available scores
        return Math.round(scores.reduce((a, b) => a + b, 0) / scores.length);
    }

    /**
     * Towns with cleanest outdoor air (PM2.5, PM10, NOx)
     * Uses 365 day (annual) data, excludes INDOOR deployment types
     */
    async getCleanestOutdoorAirTowns(limit = 5) {
        try {
            // Get PM2.5, PM10, and NOx index separately - outdoor/mixed/unknown only
            const query = `
                WITH air_quality AS (
                    SELECT
                        cache.region_adm2_id as region_id,
                        avgIf(sr.value, sr.reading_type = 'pm2_5') as avg_pm25,
                        avgIf(sr.value, sr.reading_type = 'pm10') as avg_pm10,
                        avgIf(sr.value, sr.reading_type = 'nox_index') as avg_nox,
                        countIf(sr.reading_type = 'pm2_5') as pm25_count,
                        countIf(sr.reading_type = 'pm10') as pm10_count,
                        countIf(sr.reading_type = 'nox_index') as nox_count
                    FROM wesense.sensor_readings AS sr
                    INNER JOIN wesense_respiro.device_region_cache AS cache
                        ON sr.device_id = cache.device_id
                    WHERE sr.reading_type IN ('pm2_5', 'pm10', 'nox_index')
                      AND sr.timestamp > now() - INTERVAL 365 DAY
                      AND sr.latitude != 0 AND sr.longitude != 0
                      AND cache.region_adm2_id != ''
                      AND sr.value > 0 AND sr.value < 1000
                      AND sr.deployment_type != 'INDOOR'
                    GROUP BY cache.region_adm2_id
                    HAVING (pm25_count >= 100 OR pm10_count >= 100 OR nox_count >= 100)
                )
                SELECT
                    rb.region_id,
                    rb.name,
                    rb.country_code,
                    aq.avg_pm25,
                    aq.avg_pm10,
                    aq.avg_nox,
                    (rb.bbox_min_lat + rb.bbox_max_lat) / 2 as center_lat,
                    (rb.bbox_min_lon + rb.bbox_max_lon) / 2 as center_lon
                FROM air_quality AS aq
                INNER JOIN wesense_respiro.region_boundaries AS rb
                    ON rb.region_id = aq.region_id
                ORDER BY coalesce(aq.avg_pm25, 999) ASC, coalesce(aq.avg_nox, 999) ASC
                LIMIT {limit:UInt32}
            `;

            const result = await this.clickhouse.query({
                query,
                query_params: { limit },
                format: 'JSONEachRow'
            });

            const rows = await result.json();
            return rows.map(row => {
                const pm25 = row.avg_pm25 ? parseFloat(row.avg_pm25) : null;
                const pm10 = row.avg_pm10 ? parseFloat(row.avg_pm10) : null;
                const nox = row.avg_nox ? parseFloat(row.avg_nox) : null;
                const score = this.calculateOutdoorAirScore(pm25, pm10, nox);

                return {
                    region_id: row.region_id,
                    name: row.name,
                    country_code: row.country_code,
                    pm25: pm25 ? pm25.toFixed(1) : null,
                    pm10: pm10 ? pm10.toFixed(1) : null,
                    nox: nox ? Math.round(nox) : null,
                    score,
                    lat: parseFloat(row.center_lat),
                    lon: parseFloat(row.center_lon)
                };
            }).sort((a, b) => (b.score || 0) - (a.score || 0));
        } catch (error) {
            console.error('Error getting cleanest outdoor air towns:', error);
            return [];
        }
    }

    /**
     * Calculate indoor air quality score (0-100, higher = cleaner)
     * VOC index: 100 = normal baseline (score 50), 1 = excellent (score 100)
     * CO2: 400ppm = outdoor baseline (score 100), 1000ppm = poor (score 0)
     */
    calculateIndoorAirScore(voc, co2) {
        let scores = [];
        // VOC: 1 = excellent (100), 100 = baseline (50), 200+ = poor (0)
        if (voc !== null) {
            scores.push(Math.max(0, Math.min(100, 100 - (voc / 2))));
        }
        // CO2: 400 = excellent (100), 700 = good (50), 1000+ = poor (0)
        if (co2 !== null) {
            const co2Score = Math.max(0, Math.min(100, 100 - ((co2 - 400) / 6)));
            scores.push(co2Score);
        }
        if (scores.length === 0) return null;
        return Math.round(scores.reduce((a, b) => a + b, 0) / scores.length);
    }

    /**
     * Towns with cleanest indoor air (VOC + CO2)
     * Uses 365 day (annual) data, INDOOR deployment type only
     */
    async getCleanestIndoorAirTowns(limit = 5) {
        try {
            const query = `
                WITH air_quality AS (
                    SELECT
                        cache.region_adm2_id as region_id,
                        avgIf(sr.value, sr.reading_type = 'voc_index') as avg_voc,
                        avgIf(sr.value, sr.reading_type = 'co2') as avg_co2,
                        countIf(sr.reading_type = 'voc_index') as voc_count,
                        countIf(sr.reading_type = 'co2') as co2_count
                    FROM wesense.sensor_readings AS sr
                    INNER JOIN wesense_respiro.device_region_cache AS cache
                        ON sr.device_id = cache.device_id
                    WHERE sr.reading_type IN ('voc_index', 'co2')
                      AND sr.timestamp > now() - INTERVAL 365 DAY
                      AND sr.latitude != 0 AND sr.longitude != 0
                      AND cache.region_adm2_id != ''
                      AND sr.value > 0 AND sr.value < 5000
                      AND sr.deployment_type = 'INDOOR'
                    GROUP BY cache.region_adm2_id
                    HAVING (voc_count >= 100 OR co2_count >= 100)
                )
                SELECT
                    rb.region_id,
                    rb.name,
                    rb.country_code,
                    aq.avg_voc,
                    aq.avg_co2,
                    (rb.bbox_min_lat + rb.bbox_max_lat) / 2 as center_lat,
                    (rb.bbox_min_lon + rb.bbox_max_lon) / 2 as center_lon
                FROM air_quality AS aq
                INNER JOIN wesense_respiro.region_boundaries AS rb
                    ON rb.region_id = aq.region_id
                ORDER BY coalesce(aq.avg_voc, 999) ASC, coalesce(aq.avg_co2, 9999) ASC
                LIMIT {limit:UInt32}
            `;

            const result = await this.clickhouse.query({
                query,
                query_params: { limit },
                format: 'JSONEachRow'
            });

            const rows = await result.json();
            return rows.map(row => {
                const voc = row.avg_voc ? parseFloat(row.avg_voc) : null;
                const co2 = row.avg_co2 ? parseFloat(row.avg_co2) : null;
                const score = this.calculateIndoorAirScore(voc, co2);

                return {
                    region_id: row.region_id,
                    name: row.name,
                    country_code: row.country_code,
                    voc: voc ? Math.round(voc) : null,
                    co2: co2 ? Math.round(co2) : null,
                    score,
                    lat: parseFloat(row.center_lat),
                    lon: parseFloat(row.center_lon)
                };
            }).sort((a, b) => (b.score || 0) - (a.score || 0));
        } catch (error) {
            console.error('Error getting cleanest indoor air towns:', error);
            return [];
        }
    }

    /**
     * Towns with best summer weather (comfortable temp 18-26°C + moderate humidity 40-70%)
     * Scores towns based on how close they are to ideal conditions
     */
    async getBestWeatherTowns(limit = 5) {
        try {
            const query = `
                WITH town_weather AS (
                    SELECT
                        cache.region_adm2_id as region_id,
                        avgIf(sr.value, sr.reading_type = 'temperature') as avg_temp,
                        avgIf(sr.value, sr.reading_type = 'humidity') as avg_humidity,
                        countIf(sr.reading_type = 'temperature') as temp_readings,
                        countIf(sr.reading_type = 'humidity') as humidity_readings
                    FROM wesense.sensor_readings AS sr
                    INNER JOIN wesense_respiro.device_region_cache AS cache
                        ON sr.device_id = cache.device_id
                    WHERE sr.reading_type IN ('temperature', 'humidity')
                      AND sr.timestamp > now() - INTERVAL 30 DAY
                      AND sr.latitude != 0 AND sr.longitude != 0
                      AND cache.region_adm2_id != ''
                    GROUP BY cache.region_adm2_id
                    HAVING temp_readings >= 50 AND humidity_readings >= 50
                ),
                scored AS (
                    SELECT
                        region_id,
                        avg_temp,
                        avg_humidity,
                        -- Score: 100 = perfect, lower = worse
                        -- Ideal temp: 22°C, ideal humidity: 55%
                        100 - (abs(avg_temp - 22) * 3) - (abs(avg_humidity - 55) * 0.5) as comfort_score
                    FROM town_weather
                    WHERE avg_temp BETWEEN 10 AND 35
                      AND avg_humidity BETWEEN 20 AND 90
                )
                SELECT
                    rb.region_id,
                    rb.name,
                    rb.country_code,
                    s.avg_temp,
                    s.avg_humidity,
                    s.comfort_score,
                    (rb.bbox_min_lat + rb.bbox_max_lat) / 2 as center_lat,
                    (rb.bbox_min_lon + rb.bbox_max_lon) / 2 as center_lon
                FROM scored AS s
                INNER JOIN wesense_respiro.region_boundaries AS rb
                    ON rb.region_id = s.region_id
                ORDER BY s.comfort_score DESC
                LIMIT {limit:UInt32}
            `;

            const result = await this.clickhouse.query({
                query,
                query_params: { limit },
                format: 'JSONEachRow'
            });

            const rows = await result.json();
            return rows.map(row => ({
                region_id: row.region_id,
                name: row.name,
                country_code: row.country_code,
                value: parseFloat(row.avg_temp).toFixed(1),
                humidity: parseFloat(row.avg_humidity).toFixed(0),
                score: parseFloat(row.comfort_score).toFixed(0),
                unit: '°C',
                lat: parseFloat(row.center_lat),
                lon: parseFloat(row.center_lon)
            }));
        } catch (error) {
            console.error('Error getting best weather towns:', error);
            return [];
        }
    }

    /**
     * Towns with most stable climate (lowest temperature variance)
     */
    async getMostStableClimateTowns(limit = 5) {
        try {
            const query = `
                WITH temp_variance AS (
                    SELECT
                        cache.region_adm2_id as region_id,
                        avg(sr.value) as avg_temp,
                        stddevPop(sr.value) as temp_stddev,
                        count(*) as readings
                    FROM wesense.sensor_readings AS sr
                    INNER JOIN wesense_respiro.device_region_cache AS cache
                        ON sr.device_id = cache.device_id
                    WHERE sr.reading_type = 'temperature'
                      AND sr.timestamp > now() - INTERVAL 30 DAY
                      AND sr.latitude != 0 AND sr.longitude != 0
                      AND cache.region_adm2_id != ''
                      AND sr.value BETWEEN -40 AND 60
                    GROUP BY cache.region_adm2_id
                    HAVING readings >= 100
                )
                SELECT
                    rb.region_id,
                    rb.name,
                    rb.country_code,
                    tv.avg_temp,
                    tv.temp_stddev,
                    (rb.bbox_min_lat + rb.bbox_max_lat) / 2 as center_lat,
                    (rb.bbox_min_lon + rb.bbox_max_lon) / 2 as center_lon
                FROM temp_variance AS tv
                INNER JOIN wesense_respiro.region_boundaries AS rb
                    ON rb.region_id = tv.region_id
                ORDER BY tv.temp_stddev ASC
                LIMIT {limit:UInt32}
            `;

            const result = await this.clickhouse.query({
                query,
                query_params: { limit },
                format: 'JSONEachRow'
            });

            const rows = await result.json();
            return rows.map(row => ({
                region_id: row.region_id,
                name: row.name,
                country_code: row.country_code,
                value: parseFloat(row.avg_temp).toFixed(1),
                variance: parseFloat(row.temp_stddev).toFixed(1),
                unit: '°C',
                lat: parseFloat(row.center_lat),
                lon: parseFloat(row.center_lon)
            }));
        } catch (error) {
            console.error('Error getting most stable climate towns:', error);
            return [];
        }
    }

    /**
     * Hottest towns (highest average temperature)
     */
    async getHottestTowns(limit = 5) {
        try {
            const query = `
                WITH hot_towns AS (
                    SELECT
                        cache.region_adm2_id as region_id,
                        avg(sr.value) as avg_temp,
                        count(*) as readings
                    FROM wesense.sensor_readings AS sr
                    INNER JOIN wesense_respiro.device_region_cache AS cache
                        ON sr.device_id = cache.device_id
                    WHERE sr.reading_type = 'temperature'
                      AND sr.timestamp > now() - INTERVAL 30 DAY
                      AND sr.latitude != 0 AND sr.longitude != 0
                      AND cache.region_adm2_id != ''
                      AND sr.value BETWEEN -40 AND 60
                    GROUP BY cache.region_adm2_id
                    HAVING readings >= 50
                )
                SELECT
                    rb.region_id,
                    rb.name,
                    rb.country_code,
                    ht.avg_temp,
                    (rb.bbox_min_lat + rb.bbox_max_lat) / 2 as center_lat,
                    (rb.bbox_min_lon + rb.bbox_max_lon) / 2 as center_lon
                FROM hot_towns AS ht
                INNER JOIN wesense_respiro.region_boundaries AS rb
                    ON rb.region_id = ht.region_id
                ORDER BY ht.avg_temp DESC
                LIMIT {limit:UInt32}
            `;

            const result = await this.clickhouse.query({
                query,
                query_params: { limit },
                format: 'JSONEachRow'
            });

            const rows = await result.json();
            return rows.map(row => ({
                region_id: row.region_id,
                name: row.name,
                country_code: row.country_code,
                value: parseFloat(row.avg_temp).toFixed(1),
                unit: '°C',
                lat: parseFloat(row.center_lat),
                lon: parseFloat(row.center_lon)
            }));
        } catch (error) {
            console.error('Error getting hottest towns:', error);
            return [];
        }
    }

    /**
     * Get devices that contributed to a region's data
     * @param {string} regionId - The region ID to query
     * @param {string} metric - The reading type (temperature, humidity, etc.)
     * @param {string} range - Time range (24h, 7d, etc.)
     * @param {string} deploymentFilter - 'outdoor', 'indoor', or 'all' (default: 'all')
     * @returns {Array} List of devices with their values
     */
    async getDevicesInRegion(regionId, metric, range = '24h', deploymentFilter = 'all') {
        const intervalMap = {
            '30m': '30 MINUTE',
            '1h': '1 HOUR',
            '2h': '2 HOUR',
            '4h': '4 HOUR',
            '8h': '8 HOUR',
            '24h': '24 HOUR',
            '7d': '7 DAY',
            '30d': '30 DAY'
        };
        const interval = intervalMap[range] || '30 MINUTE';

        // Build deployment filter clause - applied AFTER aggregation (in outer WHERE)
        // Uses dc.deployment_type from the device_classifications CTE (looks at ALL TIME)
        // Deployment types: OUTDOOR, INDOOR, PORTABLE, MIXED, MOBILE, DEVICE, UNKNOWN
        // Accepts comma-separated uppercase types (e.g., "OUTDOOR,MIXED") or legacy single values
        // 'all' or empty = no filter
        // 'outdoor' (legacy) = OUTDOOR + MIXED (for environmental monitoring)
        // 'UNKNOWN' matches NULL or empty deployment_type
        let deploymentClause = '';
        if (deploymentFilter && deploymentFilter !== 'all') {
            // Expand 'outdoor' to include MIXED sensors
            let expandedFilter = deploymentFilter;
            if (deploymentFilter.toLowerCase() === 'outdoor') {
                expandedFilter = 'OUTDOOR,MIXED';
            }
            // Parse comma-separated types
            const types = expandedFilter.split(',').map(t => t.trim().toUpperCase()).filter(t => t);

            if (types.length > 0) {
                const includesUnknown = types.includes('UNKNOWN');
                const knownTypes = types.filter(t => t !== 'UNKNOWN');

                const conditions = [];
                if (includesUnknown) {
                    // dc.deployment_type from device_classifications CTE
                    conditions.push("(dc.deployment_type IS NULL OR dc.deployment_type = '')");
                }
                if (knownTypes.length > 0) {
                    const typeList = knownTypes.map(t => `'${t}'`).join(',');
                    conditions.push(`dc.deployment_type IN (${typeList})`);
                }

                if (conditions.length > 0) {
                    deploymentClause = `AND (${conditions.join(' OR ')})`;
                }
            }
        }

        try {
            // Parse region_id to get admin level: ESP_ADM1_43394848 -> 1
            const admMatch = regionId.match(/_ADM(\d)_/);
            const adminLevel = admMatch ? parseInt(admMatch[1]) : null;

            if (adminLevel === null) {
                return [];
            }

            // Use device_region_cache for fast lookups (no pointInPolygon!)
            const regionColumnMap = { 0: 'region_adm0_id', 1: 'region_adm1_id', 2: 'region_adm2_id', 3: 'region_adm3_id', 4: 'region_adm4_id' };
            const regionColumn = regionColumnMap[adminLevel] || 'region_adm2_id';

            const query = `
                WITH
                -- Get each device's deployment_type from ALL TIME (not just time window)
                device_classifications AS (
                    SELECT
                        device_id,
                        argMaxIf(deployment_type, timestamp, deployment_type != '') as deployment_type
                    FROM wesense.sensor_readings
                    GROUP BY device_id
                ),
                -- Get device data from time window
                device_locations AS (
                    SELECT
                        device_id,
                        argMax(node_name, timestamp) as node_name,
                        argMax(sensor_model, timestamp) as sensor_model,
                        argMax(board_model, timestamp) as board_model,
                        avg(value) as avg_value,
                        max(timestamp) as latest_timestamp
                    FROM wesense.sensor_readings
                    WHERE reading_type = {metric:String}
                      AND timestamp > now() - INTERVAL ${interval}
                      AND latitude != 0
                      AND longitude != 0
                    GROUP BY device_id
                )
                SELECT
                    dl.device_id,
                    dl.node_name,
                    dl.sensor_model,
                    dl.board_model,
                    dc.deployment_type,
                    dl.avg_value,
                    dl.latest_timestamp
                FROM device_locations AS dl
                INNER JOIN wesense_respiro.device_region_cache AS cache
                    ON dl.device_id = cache.device_id
                LEFT JOIN device_classifications AS dc
                    ON dl.device_id = dc.device_id
                WHERE cache.${regionColumn} = {regionId:String}
                  ${deploymentClause}
                ORDER BY dl.avg_value DESC
            `;

            const result = await this.clickhouse.query({
                query,
                query_params: { regionId, metric },
                format: 'JSONEachRow'
            });

            const rows = await result.json();

            return rows.map(row => ({
                device_id: row.device_id,
                node_name: row.node_name || row.device_id,
                sensor_model: row.sensor_model || null,
                board_model: row.board_model || null,
                deployment_type: row.deployment_type || null,
                avg_value: parseFloat(row.avg_value),
                latest_timestamp: row.latest_timestamp
            }));
        } catch (error) {
            console.error(`Error getting devices in region ${regionId}:`, error);
            return [];
        }
    }
}

module.exports = RegionService;
