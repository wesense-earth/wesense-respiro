const { createClient } = require('@clickhouse/client');

class ClickHouseClient {
    constructor() {
        this.client = null;
        this.connected = false;
    }

    async connect({ quiet = false } = {}) {
        try {
            const host = process.env.CLICKHOUSE_HOST || 'localhost';
            const port = process.env.CLICKHOUSE_PORT || '8123';
            const database = process.env.CLICKHOUSE_DATABASE || 'wesense';
            const username = process.env.CLICKHOUSE_USERNAME || 'wesense';
            const password = process.env.CLICKHOUSE_PASSWORD || '';

            this.client = createClient({
                url: `http://${host}:${port}`,
                database,
                username,
                password,
                request_timeout: 300000,  // 5 minutes for background precomputation queries
            });

            // Test connection
            const result = await this.client.query({ query: 'SELECT 1' });
            await result.json();

            this.connected = true;
            console.log(`Connected to ClickHouse at ${host}:${port}/${database}`);
            return true;

        } catch (error) {
            if (!quiet) {
                console.error('Failed to connect to ClickHouse:', error.message);
            }
            this.connected = false;
            return false;
        }
    }

    async queryLatestSensors(timeRange = '24h') {
        if (!this.connected || !this.client) {
            console.log('ClickHouse not connected');
            return {};
        }

        try {
            // Convert time range to interval (null means no time filter)
            const interval = this._parseTimeRange(timeRange);
            const whereClause = interval ? `WHERE timestamp > now() - INTERVAL ${interval}` : '';

            // Get latest reading per device per reading_type, with location info
            const query = `
                SELECT
                    device_id,
                    reading_type,
                    argMax(value, timestamp) as latest_value,
                    max(timestamp) as latest_timestamp,
                    argMax(latitude, timestamp) as latitude,
                    argMax(longitude, timestamp) as longitude,
                    argMax(geo_country, timestamp) as geo_country,
                    argMax(geo_subdivision, timestamp) as geo_subdivision,
                    argMax(node_name, timestamp) as node_name,
                    argMax(board_model, timestamp) as board_model,
                    IF(
                        countIf(data_source = 'MESHTASTIC_COMMUNITY') > 0,
                        'MESHTASTIC_COMMUNITY',
                        argMax(data_source, timestamp)
                    ) as data_source,
                    argMax(sensor_model, timestamp) as sensor_model,
                    argMax(unit, timestamp) as unit,
                    argMax(deployment_location, timestamp) as deployment_location,
                    -- Prefer non-empty deployment_type (new rows may not have it set yet)
                    -- Note: This only looks within the time range, we'll backfill from historical data below
                    argMaxIf(deployment_type, timestamp, deployment_type != '') as deployment_type,
                    argMaxIf(deployment_type_source, timestamp, deployment_type_source != '') as deployment_type_source,
                    argMax(node_info, timestamp) as node_info,
                    argMax(node_info_url, timestamp) as node_info_url
                FROM sensor_readings
                ${whereClause}
                GROUP BY device_id, reading_type
                ORDER BY device_id, reading_type
            `;

            // Run both queries in parallel: main sensor data and historical deployment types
            const [sensorResult, historicalDeploymentTypes] = await Promise.all([
                this.client.query({ query, format: 'JSONEachRow' }),
                this.queryDeviceDeploymentTypes()
            ]);

            const rows = await sensorResult.json();

            // Group by device_id
            const sensorData = {};

            for (const row of rows) {
                const deviceId = row.device_id;

                if (!sensorData[deviceId]) {
                    // Use deployment_type from this time range if available,
                    // otherwise fall back to historical deployment_type
                    let deploymentType = row.deployment_type;
                    let deploymentTypeSource = row.deployment_type_source;

                    if (!deploymentType && historicalDeploymentTypes[deviceId]) {
                        deploymentType = historicalDeploymentTypes[deviceId].deployment_type;
                        deploymentTypeSource = historicalDeploymentTypes[deviceId].deployment_type_source;
                    }

                    sensorData[deviceId] = {
                        latitude: row.latitude,
                        longitude: row.longitude,
                        geo_country: row.geo_country,
                        geo_subdivision: row.geo_subdivision,
                        node_name: row.node_name,
                        board_model: row.board_model,
                        data_source: row.data_source,
                        deployment_location: row.deployment_location,
                        deployment_type: deploymentType,
                        deployment_type_source: deploymentTypeSource,
                        node_info: row.node_info,
                        node_info_url: row.node_info_url,
                        readings: {}
                    };
                }

                // Add reading (convert timestamp to ISO format for browser parsing)
                sensorData[deviceId].readings[row.reading_type] = {
                    value: row.latest_value,
                    timestamp: this._toISOString(row.latest_timestamp),
                    unit: row.unit || this._getDefaultUnit(row.reading_type),
                    sensor_model: row.sensor_model
                };
            }

            console.log(`Fetched ${Object.keys(sensorData).length} devices from ClickHouse`);
            return sensorData;

        } catch (error) {
            console.error('Failed to query latest sensors:', error.message);
            return {};
        }
    }

    async querySparklineData(timeRange = '24h', pointsPerDevice = 48) {
        if (!this.connected || !this.client) {
            return {};
        }

        try {
            const interval = this._parseTimeRange(timeRange);
            const whereClause = interval ? `WHERE timestamp > now() - INTERVAL ${interval}` : '';

            // Determine bucket size based on time range for proper aggregation
            // This ensures we get meaningful data points across the entire time range
            const bucketConfig = this._getSparklineBucketConfig(timeRange);

            let query;
            if (bucketConfig.useBuckets) {
                // Use time-bucketed aggregation for longer time ranges
                query = `
                    SELECT
                        device_id,
                        reading_type,
                        groupArray(avg_value) as values,
                        groupArray(bucket_time) as timestamps
                    FROM (
                        SELECT
                            device_id,
                            reading_type,
                            toStartOfInterval(timestamp, INTERVAL ${bucketConfig.bucketSize}) as bucket_time,
                            avg(value) as avg_value
                        FROM sensor_readings
                        ${whereClause}
                        GROUP BY device_id, reading_type, bucket_time
                        ORDER BY device_id, reading_type, bucket_time ASC
                    )
                    GROUP BY device_id, reading_type
                `;
            } else {
                // For short time ranges, get individual readings (reversed for oldest-first)
                query = `
                    SELECT
                        device_id,
                        reading_type,
                        groupArray(${pointsPerDevice})(value) as values,
                        groupArray(${pointsPerDevice})(timestamp) as timestamps
                    FROM (
                        SELECT device_id, reading_type, value, timestamp
                        FROM sensor_readings
                        ${whereClause}
                        ORDER BY device_id, reading_type, timestamp DESC
                    )
                    GROUP BY device_id, reading_type
                `;
            }

            const result = await this.client.query({ query, format: 'JSONEachRow' });
            const rows = await result.json();

            const sparklineData = {};

            for (const row of rows) {
                const deviceId = row.device_id;

                if (!sparklineData[deviceId]) {
                    sparklineData[deviceId] = {};
                }

                // For non-bucketed queries, reverse to get oldest-first for charts
                const values = bucketConfig.useBuckets ? row.values : row.values.reverse();
                const rawTimestamps = bucketConfig.useBuckets ? row.timestamps : row.timestamps.reverse();

                // Convert timestamps to ISO format with UTC indicator
                const timestamps = rawTimestamps.map(ts => this._toISOString(ts));

                sparklineData[deviceId][row.reading_type] = {
                    values,
                    timestamps
                };
            }

            return sparklineData;

        } catch (error) {
            console.error('Failed to query sparkline data:', error.message);
            return {};
        }
    }

    _getSparklineBucketConfig(timeRange) {
        // Determine appropriate bucket size based on time range
        const match = timeRange.match(/^(\d+)([hdwmy])$/);
        if (!match) {
            return { useBuckets: false, bucketSize: null };
        }

        const value = parseInt(match[1]);
        const unit = match[2];

        // Convert to hours for comparison
        const multipliers = { h: 1, d: 24, w: 168, m: 720, y: 8760 };
        const totalHours = value * (multipliers[unit] || 24);

        if (totalHours <= 24) {
            // 24h or less: use raw data points
            return { useBuckets: false, bucketSize: null };
        } else if (totalHours <= 168) {
            // Up to 1 week: 2-hour buckets (~84 points max)
            return { useBuckets: true, bucketSize: '2 HOUR' };
        } else if (totalHours <= 720) {
            // Up to 1 month: 6-hour buckets (~120 points max)
            return { useBuckets: true, bucketSize: '6 HOUR' };
        } else if (totalHours <= 2160) {
            // Up to 3 months: daily buckets (~90 points max)
            return { useBuckets: true, bucketSize: '1 DAY' };
        } else {
            // Over 3 months: weekly buckets
            return { useBuckets: true, bucketSize: '1 WEEK' };
        }
    }

    async queryHistoricalData(deviceId, readingType, timeRange = '24h') {
        if (!this.connected || !this.client) {
            return [];
        }

        try {
            const interval = this._parseTimeRange(timeRange);
            const timeFilter = interval ? `AND timestamp > now() - INTERVAL ${interval}` : '';

            // Use bucketing for longer time ranges to improve performance
            const bucketConfig = this._getSparklineBucketConfig(timeRange);

            let query;
            if (bucketConfig.useBuckets) {
                // Use aggregated buckets for longer time ranges
                query = `
                    SELECT
                        toStartOfInterval(timestamp, INTERVAL ${bucketConfig.bucketSize}) as timestamp,
                        avg(value) as value,
                        argMax(unit, timestamp) as unit
                    FROM sensor_readings
                    WHERE device_id = {deviceId:String}
                      AND reading_type = {readingType:String}
                      ${timeFilter}
                    GROUP BY timestamp
                    ORDER BY timestamp ASC
                `;
            } else {
                // Use raw data for short time ranges
                query = `
                    SELECT timestamp, value, unit
                    FROM sensor_readings
                    WHERE device_id = {deviceId:String}
                      AND reading_type = {readingType:String}
                      ${timeFilter}
                    ORDER BY timestamp ASC
                `;
            }

            const result = await this.client.query({
                query,
                format: 'JSONEachRow',
                query_params: { deviceId, readingType }
            });

            const rows = await result.json();

            // Convert timestamps to ISO format with UTC indicator
            return rows.map(row => ({
                ...row,
                timestamp: this._toISOString(row.timestamp)
            }));

        } catch (error) {
            console.error(`Failed to query history for ${deviceId}/${readingType}:`, error.message);
            return [];
        }
    }

    /**
     * Query aggregated historical data across multiple devices
     * Groups by time buckets and returns average values
     * @param {string[]} deviceIds - List of device IDs
     * @param {string} readingType - Type of reading (temperature, humidity, etc.)
     * @param {string} timeRange - Time range (1h, 24h, 7d, etc.)
     * @returns {Array} Array of {timestamp, value} objects
     */
    async queryAggregateHistory(deviceIds, readingType, timeRange = '24h') {
        if (!this.connected || !this.client) {
            return [];
        }

        if (!deviceIds || deviceIds.length === 0) {
            return [];
        }

        try {
            const interval = this._parseTimeRange(timeRange);
            const timeFilter = interval ? `AND timestamp > now() - INTERVAL ${interval}` : '';

            // Determine bucket size based on time range
            const bucketSeconds = this._getBucketSize(timeRange);
            const deviceFilter = deviceIds.map(id => `'${id}'`).join(',');

            const query = `
                SELECT
                    toStartOfInterval(timestamp, INTERVAL ${bucketSeconds} SECOND) as bucket,
                    avg(value) as avg_value,
                    min(value) as min_value,
                    max(value) as max_value,
                    count() as count
                FROM sensor_readings
                WHERE device_id IN (${deviceFilter})
                  AND reading_type = {readingType:String}
                  ${timeFilter}
                GROUP BY bucket
                ORDER BY bucket ASC
            `;

            const result = await this.client.query({
                query,
                format: 'JSONEachRow',
                query_params: { readingType }
            });

            const rows = await result.json();

            return rows.map(row => ({
                timestamp: this._toISOString(row.bucket),
                value: parseFloat(row.avg_value),
                min: parseFloat(row.min_value),
                max: parseFloat(row.max_value),
                count: parseInt(row.count)
            }));

        } catch (error) {
            console.error(`Failed to query aggregate history:`, error.message);
            return [];
        }
    }

    /**
     * Query historical comparison data for weather hero
     * Returns temperature at same time yesterday, 24h ago, and same time last week
     * @param {string[]} deviceIds - List of outdoor device IDs
     * @param {string} readingType - Type of reading (default: temperature)
     * @returns {Object} { yesterday, yesterday24hAgo, lastWeek }
     */
    async queryHistoricalComparison(deviceIds, readingType = 'temperature') {
        if (!this.connected || !this.client) {
            return null;
        }

        if (!deviceIds || deviceIds.length === 0) {
            return null;
        }

        try {
            const deviceFilter = deviceIds.map(id => `'${id}'`).join(',');

            // Query for yesterday at same time (±30 min window), 24h ago, and last week same time (±1 hour)
            const query = `
                SELECT
                    -- Yesterday at same time (±30 min window)
                    avgIf(value,
                        timestamp >= now() - INTERVAL 24 HOUR - INTERVAL 30 MINUTE
                        AND timestamp <= now() - INTERVAL 24 HOUR + INTERVAL 30 MINUTE
                    ) as yesterday_value,

                    -- 24 hours ago (for calculating change)
                    avgIf(value,
                        timestamp >= now() - INTERVAL 24 HOUR - INTERVAL 5 MINUTE
                        AND timestamp <= now() - INTERVAL 24 HOUR + INTERVAL 5 MINUTE
                    ) as ago_24h_value,

                    -- Last week same time (±1 hour window for more data)
                    avgIf(value,
                        timestamp >= now() - INTERVAL 7 DAY - INTERVAL 1 HOUR
                        AND timestamp <= now() - INTERVAL 7 DAY + INTERVAL 1 HOUR
                    ) as last_week_value,

                    -- Current value (last 10 minutes)
                    avgIf(value,
                        timestamp >= now() - INTERVAL 10 MINUTE
                    ) as current_value

                FROM sensor_readings
                WHERE device_id IN (${deviceFilter})
                  AND reading_type = {readingType:String}
                  AND timestamp > now() - INTERVAL 8 DAY
            `;

            const result = await this.client.query({
                query,
                format: 'JSONEachRow',
                query_params: { readingType }
            });

            const rows = await result.json();
            if (rows.length === 0) return null;

            const row = rows[0];
            return {
                yesterday: row.yesterday_value ? parseFloat(row.yesterday_value) : null,
                ago24h: row.ago_24h_value ? parseFloat(row.ago_24h_value) : null,
                lastWeek: row.last_week_value ? parseFloat(row.last_week_value) : null,
                current: row.current_value ? parseFloat(row.current_value) : null
            };

        } catch (error) {
            console.error(`Failed to query historical comparison:`, error.message);
            return null;
        }
    }

    /**
     * Generate temperature predictions using diurnal pattern + trend extrapolation
     * @param {string[]} deviceIds - List of outdoor device IDs
     * @param {number} hoursAhead - How many hours to forecast (default: 24)
     * @returns {Object} Predictions with hourly forecasts, tonight/tomorrow summaries
     */
    async queryTemperaturePrediction(deviceIds, hoursAhead = 24) {
        if (!this.connected || !this.client) {
            return null;
        }

        if (!deviceIds || deviceIds.length === 0) {
            return null;
        }

        try {
            const deviceFilter = deviceIds.map(id => `'${id}'`).join(',');

            // Query 1: Build diurnal curve (average temp for each hour over past 14 days)
            // Query 2: Get current temperature and recent trend
            const query = `
                WITH
                    -- Diurnal curve: average temperature for each hour of day (past 14 days)
                    diurnal AS (
                        SELECT
                            toHour(timestamp) as hour_of_day,
                            avg(value) as avg_temp,
                            stddevPop(value) as std_temp,
                            count() as sample_count
                        FROM sensor_readings
                        WHERE device_id IN (${deviceFilter})
                          AND reading_type = 'temperature'
                          AND timestamp > now() - INTERVAL 14 DAY
                        GROUP BY hour_of_day
                        ORDER BY hour_of_day
                    ),
                    -- Current conditions (last 15 minutes)
                    current AS (
                        SELECT
                            avg(value) as current_temp,
                            toHour(now()) as current_hour
                        FROM sensor_readings
                        WHERE device_id IN (${deviceFilter})
                          AND reading_type = 'temperature'
                          AND timestamp > now() - INTERVAL 15 MINUTE
                    ),
                    -- Recent trend (compare last hour to 3 hours ago)
                    trend AS (
                        SELECT
                            avgIf(value, timestamp > now() - INTERVAL 1 HOUR) as recent_temp,
                            avgIf(value, timestamp > now() - INTERVAL 4 HOUR AND timestamp < now() - INTERVAL 3 HOUR) as earlier_temp
                        FROM sensor_readings
                        WHERE device_id IN (${deviceFilter})
                          AND reading_type = 'temperature'
                          AND timestamp > now() - INTERVAL 4 HOUR
                    )
                SELECT
                    d.hour_of_day,
                    d.avg_temp,
                    d.std_temp,
                    d.sample_count,
                    c.current_temp,
                    c.current_hour,
                    t.recent_temp,
                    t.earlier_temp,
                    -- Calculate trend per hour
                    CASE
                        WHEN t.recent_temp IS NOT NULL AND t.earlier_temp IS NOT NULL
                        THEN (t.recent_temp - t.earlier_temp) / 3.0
                        ELSE 0
                    END as trend_per_hour
                FROM diurnal d
                CROSS JOIN current c
                CROSS JOIN trend t
                ORDER BY d.hour_of_day
            `;

            const result = await this.client.query({
                query,
                format: 'JSONEachRow'
            });

            const rows = await result.json();
            if (rows.length === 0) return null;

            // Extract diurnal curve and current conditions
            const diurnalCurve = {};
            let currentTemp = null;
            let currentHour = new Date().getHours();
            let trendPerHour = 0;

            for (const row of rows) {
                diurnalCurve[row.hour_of_day] = {
                    avg: parseFloat(row.avg_temp),
                    std: parseFloat(row.std_temp) || 2, // Default 2°C std if not available
                    samples: parseInt(row.sample_count)
                };

                // These are the same for all rows (CROSS JOIN)
                if (row.current_temp != null) {
                    currentTemp = parseFloat(row.current_temp);
                }
                if (row.current_hour != null) {
                    currentHour = parseInt(row.current_hour);
                }
                if (row.trend_per_hour != null) {
                    trendPerHour = parseFloat(row.trend_per_hour);
                }
            }

            // Fill in any missing hours with interpolation
            for (let h = 0; h < 24; h++) {
                if (!diurnalCurve[h]) {
                    // Simple interpolation from neighbors
                    const prev = diurnalCurve[(h + 23) % 24];
                    const next = diurnalCurve[(h + 1) % 24];
                    if (prev && next) {
                        diurnalCurve[h] = {
                            avg: (prev.avg + next.avg) / 2,
                            std: (prev.std + next.std) / 2,
                            samples: 0
                        };
                    }
                }
            }

            // Calculate offset from expected value
            const expectedNow = diurnalCurve[currentHour]?.avg || currentTemp;
            const offset = currentTemp != null ? currentTemp - expectedNow : 0;

            // Generate hourly predictions
            const predictions = [];
            const now = new Date();
            const trendDecay = 0.85; // Trend weakens over time

            for (let h = 0; h <= hoursAhead; h++) {
                const futureHour = (currentHour + h) % 24;
                const futureDate = new Date(now.getTime() + h * 3600 * 1000);

                // Base prediction from diurnal curve + offset
                const basePrediction = (diurnalCurve[futureHour]?.avg || 20) + offset;

                // Add trend adjustment (decays exponentially)
                const trendAdjustment = trendPerHour * h * Math.pow(trendDecay, h);

                // Final prediction
                const prediction = basePrediction + trendAdjustment;

                // Confidence decreases with time
                const confidence = Math.max(0.3, 1 - (h * 0.03));

                // Uncertainty range (widens with time)
                const baseStd = diurnalCurve[futureHour]?.std || 2;
                const uncertainty = baseStd * (1 + h * 0.1);

                predictions.push({
                    hour: h,
                    time: futureDate.toISOString(),
                    hourOfDay: futureHour,
                    temp: Math.round(prediction * 10) / 10,
                    low: Math.round((prediction - uncertainty) * 10) / 10,
                    high: Math.round((prediction + uncertainty) * 10) / 10,
                    confidence: Math.round(confidence * 100) / 100
                });
            }

            // Calculate tonight and tomorrow summaries
            const tonight = this._calculatePeriodSummary(predictions, now, 'tonight');
            const tomorrow = this._calculatePeriodSummary(predictions, now, 'tomorrow');

            // Determine overall trend description
            let trendDescription = 'steady';
            if (trendPerHour > 0.3) trendDescription = 'warming';
            else if (trendPerHour < -0.3) trendDescription = 'cooling';

            return {
                predictions,
                current: currentTemp,
                trendPerHour,
                trendDescription,
                tonight,
                tomorrow,
                generatedAt: new Date().toISOString(),
                dataPoints: rows[0]?.sample_count || 0
            };

        } catch (error) {
            console.error(`Failed to generate temperature prediction:`, error.message);
            return null;
        }
    }

    /**
     * Calculate summary for a time period (tonight or tomorrow)
     */
    _calculatePeriodSummary(predictions, now, period) {
        const currentHour = now.getHours();
        let startHour, endHour;

        if (period === 'tonight') {
            // Tonight: from now until 6am tomorrow
            startHour = 0;
            endHour = Math.min(predictions.length, (24 - currentHour) + 6);
        } else if (period === 'tomorrow') {
            // Tomorrow: 6am to 9pm tomorrow
            const hoursUntilTomorrow6am = (24 - currentHour) + 6;
            startHour = hoursUntilTomorrow6am;
            endHour = hoursUntilTomorrow6am + 15; // Until 9pm
        }

        const periodPredictions = predictions.slice(startHour, endHour);
        if (periodPredictions.length === 0) return null;

        const temps = periodPredictions.map(p => p.temp);
        return {
            low: Math.round(Math.min(...temps)),
            high: Math.round(Math.max(...temps)),
            avg: Math.round(temps.reduce((a, b) => a + b, 0) / temps.length)
        };
    }

    /**
     * Get appropriate bucket size in seconds based on time range
     */
    _getBucketSize(timeRange) {
        const buckets = {
            '1h': 60,        // 1 minute buckets
            '2h': 120,       // 2 minute buckets
            '4h': 300,       // 5 minute buckets
            '8h': 600,       // 10 minute buckets
            '24h': 1800,     // 30 minute buckets
            '48h': 3600,     // 1 hour buckets
            '7d': 7200,      // 2 hour buckets
            '30d': 21600,    // 6 hour buckets
            '90d': 86400,    // 1 day buckets
            '1y': 259200,    // 3 day buckets
            'all': 604800    // 1 week buckets
        };
        return buckets[timeRange] || 1800;
    }

    async queryAverageData(timeRange = '24h') {
        if (!this.connected || !this.client) {
            console.log('ClickHouse not connected');
            return {};
        }

        try {
            // Get latest sensors with sparkline data merged in
            const sensorData = await this.queryLatestSensors(timeRange);
            const sparklineData = await this.querySparklineData(timeRange);

            // Merge sparkline data and calculate trends
            for (const [deviceId, device] of Object.entries(sensorData)) {
                for (const [readingType, reading] of Object.entries(device.readings)) {
                    const sparkline = sparklineData[deviceId]?.[readingType];

                    if (sparkline && sparkline.values.length >= 2) {
                        reading.sparklineData = sparkline.values;

                        // Calculate trend
                        const latest = sparkline.values[sparkline.values.length - 1];
                        const previous = sparkline.values[0];
                        const diff = latest - previous;

                        // Pressure uses absolute threshold (hPa), others use percentage
                        if (readingType === 'pressure') {
                            // For pressure: 2 hPa change is noticeable, 5+ is significant
                            if (diff > 2) {
                                reading.trend = diff > 5 ? 'Rising Rapidly' : 'Rising';
                            } else if (diff < -2) {
                                reading.trend = diff < -5 ? 'Falling Rapidly' : 'Falling';
                            } else {
                                reading.trend = 'Steady';
                            }
                        } else {
                            // Other metrics use 1% threshold
                            if (latest > previous * 1.01) {
                                reading.trend = 'up';
                            } else if (latest < previous * 0.99) {
                                reading.trend = 'down';
                            } else {
                                reading.trend = 'flat';
                            }
                        }
                    }
                }
            }

            return sensorData;

        } catch (error) {
            console.error('Failed to query average data:', error.message);
            return {};
        }
    }

    async queryRegionalAggregates(timeRange = '24h', readingType = 'temperature') {
        if (!this.connected || !this.client) {
            console.log('ClickHouse not connected');
            return {};
        }

        try {
            const interval = this._parseTimeRange(timeRange);
            const whereClause = interval ? `WHERE timestamp > now() - INTERVAL ${interval}` : '';

            // Aggregate readings by country + subdivision
            const query = `
                SELECT
                    geo_country,
                    geo_subdivision,
                    reading_type,
                    avg(value) as avg_value,
                    min(value) as min_value,
                    max(value) as max_value,
                    count(DISTINCT device_id) as sensor_count,
                    max(timestamp) as latest_timestamp
                FROM sensor_readings
                ${whereClause}
                ${whereClause ? 'AND' : 'WHERE'} reading_type = {readingType:String}
                    AND geo_country IS NOT NULL
                    AND geo_country != ''
                    AND geo_subdivision IS NOT NULL
                    AND geo_subdivision != ''
                GROUP BY geo_country, geo_subdivision, reading_type
                ORDER BY geo_country, geo_subdivision
            `;

            const result = await this.client.query({
                query,
                format: 'JSONEachRow',
                query_params: { readingType }
            });

            const rows = await result.json();

            // Transform to ISO code format for matching PMTiles: { "NZ-AUK": { ... }, "AU-VIC": { ... } }
            const regionalData = {};

            for (const row of rows) {
                // Create ISO 3166-2 style key (e.g., "NZ-AUK", "AU-VIC", "US-CA")
                const isoCode = `${row.geo_country}-${row.geo_subdivision}`;

                regionalData[isoCode] = {
                    country: row.geo_country,
                    subdivision: row.geo_subdivision,
                    readingType: row.reading_type,
                    avg: row.avg_value,
                    min: row.min_value,
                    max: row.max_value,
                    sensorCount: row.sensor_count,
                    latestTimestamp: this._toISOString(row.latest_timestamp)
                };
            }

            console.log(`Fetched regional aggregates for ${Object.keys(regionalData).length} regions`);
            return regionalData;

        } catch (error) {
            console.error('Failed to query regional aggregates:', error.message);
            return {};
        }
    }

    async queryAllDevices() {
        if (!this.connected || !this.client) {
            console.log('ClickHouse not connected');
            return [];
        }

        try {
            // Get all unique devices with their latest info and recent readings
            const query = `
                SELECT
                    device_id,
                    argMax(node_name, timestamp) as node_name,
                    argMax(deployment_location, timestamp) as deployment_location,
                    argMax(latitude, timestamp) as latitude,
                    argMax(longitude, timestamp) as longitude,
                    argMax(geo_country, timestamp) as geo_country,
                    argMax(geo_subdivision, timestamp) as geo_subdivision,
                    argMax(board_model, timestamp) as board_model,
                    argMax(data_source, timestamp) as data_source,
                    -- Prefer non-empty deployment_type (new rows may not have it set yet)
                    argMaxIf(deployment_type, timestamp, deployment_type != '') as deployment_type,
                    argMaxIf(deployment_type_source, timestamp, deployment_type_source != '') as deployment_type_source,
                    argMax(node_info, timestamp) as node_info,
                    argMax(node_info_url, timestamp) as node_info_url,
                    max(timestamp) as last_seen,
                    count() as reading_count,
                    -- Latest temperature and humidity values
                    argMaxIf(value, timestamp, reading_type = 'temperature') as temperature,
                    argMaxIf(value, timestamp, reading_type = 'humidity') as humidity,
                    maxIf(timestamp, reading_type = 'temperature') as temperature_timestamp,
                    maxIf(timestamp, reading_type = 'humidity') as humidity_timestamp
                FROM sensor_readings
                GROUP BY device_id
                ORDER BY last_seen DESC
            `;

            const result = await this.client.query({ query, format: 'JSONEachRow' });
            const rows = await result.json();

            const devices = rows.map(row => ({
                deviceId: row.device_id,
                name: row.node_name || row.deployment_location || row.device_id,
                latitude: row.latitude,
                longitude: row.longitude,
                geo_country: row.geo_country,
                geo_subdivision: row.geo_subdivision,
                board_model: row.board_model,
                data_source: row.data_source,
                deployment_type: row.deployment_type,
                deployment_type_source: row.deployment_type_source,
                node_info: row.node_info,
                node_info_url: row.node_info_url,
                last_seen: this._toISOString(row.last_seen),
                reading_count: row.reading_count,
                temperature: row.temperature !== 0 ? row.temperature : null,
                humidity: row.humidity !== 0 ? row.humidity : null,
                temperature_timestamp: row.temperature_timestamp ? this._toISOString(row.temperature_timestamp) : null,
                humidity_timestamp: row.humidity_timestamp ? this._toISOString(row.humidity_timestamp) : null
            }));

            console.log(`Found ${devices.length} total devices in ClickHouse`);
            return devices;

        } catch (error) {
            console.error('Failed to query all devices:', error.message);
            return [];
        }
    }

    _parseTimeRange(timeRange) {
        // Convert formats like "30m", "24h", "7d", "1w", "1y" to ClickHouse interval
        // "all" returns null to indicate no time filter
        if (timeRange === 'all') {
            return null;
        }

        // Handle minute format (e.g., "30m" for 30 minutes)
        const minuteMatch = timeRange.match(/^(\d+)m$/);
        if (minuteMatch) {
            return `${parseInt(minuteMatch[1])} MINUTE`;
        }

        const match = timeRange.match(/^(\d+)([hdwMy])$/);
        if (!match) {
            return '24 HOUR';
        }

        const value = parseInt(match[1]);
        const unit = match[2];

        const units = {
            'h': 'HOUR',
            'd': 'DAY',
            'w': 'WEEK',
            'M': 'MONTH',
            'y': 'YEAR'
        };

        return `${value} ${units[unit] || 'HOUR'}`;
    }

    _getDefaultUnit(readingType) {
        const units = {
            'temperature': '°C',
            'humidity': '%',
            'pressure': 'hPa',
            'co2': 'ppm',
            'pm1_0': 'µg/m³',
            'pm2_5': 'µg/m³',
            'pm10': 'µg/m³',
            'voc_index': 'index',
            'nox_index': 'index',
            'altitude': 'm',
            'battery_level': '%',
            'voltage': 'V'
        };
        return units[readingType] || '';
    }

    _toISOString(timestamp) {
        // Convert ClickHouse timestamp "2025-12-01 03:52:53.000" to ISO format "2025-12-01T03:52:53.000Z"
        if (!timestamp) return null;
        if (typeof timestamp === 'string') {
            // Replace space with T and add Z for UTC
            return timestamp.replace(' ', 'T') + 'Z';
        }
        return timestamp;
    }

    /**
     * Direct query method for RegionService and other services
     * @param {Object} options - Query options { query, query_params, format }
     * @returns {Promise<Object>} Query result object with json() method
     */
    async query(options) {
        if (!this.connected || !this.client) {
            throw new Error('ClickHouse not connected');
        }
        return this.client.query(options);
    }

    /**
     * Execute a command (INSERT, ALTER, etc.)
     * @param {Object} options - Command options { query, query_params }
     * @returns {Promise<void>}
     */
    async command(options) {
        if (!this.connected || !this.client) {
            throw new Error('ClickHouse not connected');
        }
        return this.client.command(options);
    }

    /**
     * Query comparison data for dashboard "vs yesterday" badges
     * Compares current values with 1 hour ago, yesterday same time, and week average
     * @param {string[]} deviceIds - List of device IDs to aggregate
     * @param {string[]} readingTypes - List of reading types to compare
     * @returns {Object} Comparison data per reading type
     */
    async queryComparisonData(deviceIds = [], readingTypes = ['temperature', 'humidity', 'pressure', 'co2', 'pm2_5', 'pm10', 'pm1_0', 'voc_index', 'nox_index']) {
        if (!this.connected || !this.client) {
            return {};
        }

        if (deviceIds.length === 0) {
            return {};
        }

        try {
            // Build device filter - escape single quotes in device IDs
            const deviceFilter = deviceIds.map(id => `'${id.replace(/'/g, "''")}'`).join(',');
            const comparison = {};

            // Query each reading type separately for reliability
            for (const readingType of readingTypes) {
                try {
                    const query = `
                        SELECT
                            avg(if(timestamp > now() - INTERVAL 1 HOUR, value, NULL)) as current_avg,
                            avg(if(timestamp > now() - INTERVAL 2 HOUR AND timestamp <= now() - INTERVAL 1 HOUR, value, NULL)) as hour_ago_avg,
                            avg(if(timestamp > now() - INTERVAL 25 HOUR AND timestamp <= now() - INTERVAL 23 HOUR, value, NULL)) as yesterday_avg,
                            avg(value) as week_avg,
                            min(value) as week_min,
                            max(value) as week_max,
                            min(if(timestamp > toStartOfDay(now()), value, NULL)) as today_min,
                            max(if(timestamp > toStartOfDay(now()), value, NULL)) as today_max
                        FROM sensor_readings
                        WHERE device_id IN (${deviceFilter})
                          AND reading_type = '${readingType}'
                          AND timestamp > now() - INTERVAL 7 DAY
                    `;

                    const result = await this.client.query({
                        query,
                        format: 'JSONEachRow'
                    });

                    const rows = await result.json();

                    if (rows.length > 0) {
                        const row = rows[0];

                        // Helper to parse float without treating 0 as null
                        const safeParseFloat = (val) => {
                            const parsed = parseFloat(val);
                            return isNaN(parsed) ? null : parsed;
                        };

                        const current = safeParseFloat(row.current_avg);
                        const hourAgo = safeParseFloat(row.hour_ago_avg);
                        const yesterday = safeParseFloat(row.yesterday_avg);
                        const weekAvg = safeParseFloat(row.week_avg);

                        comparison[readingType] = {
                            current: current,
                            hourAgo: hourAgo,
                            hourAgoDiff: (current !== null && hourAgo !== null) ? current - hourAgo : null,
                            yesterday: yesterday,
                            yesterdayDiff: (current !== null && yesterday !== null) ? current - yesterday : null,
                            weekAvg: weekAvg,
                            weekDiff: (current !== null && weekAvg !== null) ? current - weekAvg : null,
                            todayMin: safeParseFloat(row.today_min),
                            todayMax: safeParseFloat(row.today_max),
                            weekMin: safeParseFloat(row.week_min),
                            weekMax: safeParseFloat(row.week_max)
                        };
                    }
                } catch (innerError) {
                    console.error(`[queryComparisonData] Error querying ${readingType}:`, innerError.message);
                }
            }

            return comparison;

        } catch (error) {
            console.error('[queryComparisonData] Failed:', error.message);
            return {};
        }
    }

    /**
     * Get the most recent non-empty deployment_type for each device
     * This queries ALL time (no time filter) to find historical classifications
     * @returns {Object} Map of device_id -> { deployment_type, deployment_type_source }
     */
    async queryDeviceDeploymentTypes() {
        if (!this.connected || !this.client) {
            return {};
        }

        try {
            const query = `
                SELECT
                    device_id,
                    argMaxIf(deployment_type, timestamp, deployment_type != '') as deployment_type,
                    argMaxIf(deployment_type_source, timestamp, deployment_type_source != '') as deployment_type_source
                FROM sensor_readings
                GROUP BY device_id
                HAVING deployment_type != ''
            `;

            const result = await this.client.query({ query, format: 'JSONEachRow' });
            const rows = await result.json();

            const deploymentTypes = {};
            for (const row of rows) {
                deploymentTypes[row.device_id] = {
                    deployment_type: row.deployment_type,
                    deployment_type_source: row.deployment_type_source
                };
            }

            return deploymentTypes;

        } catch (error) {
            console.error('Failed to query device deployment types:', error.message);
            return {};
        }
    }

    /**
     * Query network-wide statistics for the Stats tab
     * Single efficient query using countDistinctIf for all metrics
     */
    async queryNetworkStats() {
        if (!this.connected || !this.client) {
            return null;
        }

        try {
            const query = `
                SELECT
                    uniqExact(device_id) as total_devices,
                    uniqExactIf(device_id, timestamp > now() - INTERVAL 1 HOUR) as active_devices_1h,
                    uniqExactIf(device_id, timestamp > now() - INTERVAL 24 HOUR) as active_devices_24h,
                    countIf(timestamp > now() - INTERVAL 1 HOUR) as readings_last_1h,
                    countIf(timestamp > now() - INTERVAL 24 HOUR) as readings_last_24h,
                    round(countIf(timestamp > now() - INTERVAL 1 HOUR) / 60.0, 1) as readings_per_minute,
                    uniqExactIf(geo_country, geo_country != '') as countries,
                    uniqExactIf(concat(geo_country, '-', geo_subdivision), geo_country != '' AND geo_subdivision != '') as regions,
                    uniqExactIf(reading_type, timestamp > now() - INTERVAL 24 HOUR) as reading_types_active,
                    max(timestamp) as latest_reading,
                    count() as total_readings_all_time
                FROM sensor_readings
            `;

            const sourceQuery = `
                SELECT
                    data_source,
                    uniqExact(device_id) as device_count
                FROM sensor_readings
                WHERE timestamp > now() - INTERVAL 24 HOUR
                  AND data_source != ''
                GROUP BY data_source
                ORDER BY device_count DESC
            `;

            const [statsResult, sourceResult] = await Promise.all([
                this.client.query({ query, format: 'JSONEachRow' }),
                this.client.query({ query: sourceQuery, format: 'JSONEachRow' })
            ]);

            const statsRows = await statsResult.json();
            const sourceRows = await sourceResult.json();

            if (statsRows.length === 0) return null;

            const row = statsRows[0];
            const dataSources = {};
            for (const src of sourceRows) {
                dataSources[src.data_source] = parseInt(src.device_count);
            }

            return {
                total_devices: parseInt(row.total_devices),
                active_devices_1h: parseInt(row.active_devices_1h),
                active_devices_24h: parseInt(row.active_devices_24h),
                readings_last_1h: parseInt(row.readings_last_1h),
                readings_last_24h: parseInt(row.readings_last_24h),
                readings_per_minute: parseFloat(row.readings_per_minute),
                data_sources: dataSources,
                coverage: {
                    countries: parseInt(row.countries),
                    regions: parseInt(row.regions)
                },
                reading_types_active: parseInt(row.reading_types_active),
                latest_reading: this._toISOString(row.latest_reading),
                total_readings_all_time: parseInt(row.total_readings_all_time)
            };

        } catch (error) {
            console.error('Failed to query network stats:', error.message);
            return null;
        }
    }

    /**
     * Query contribution breakdown: local ingesters vs P2P data
     * Groups by received_via and data_source
     */
    async queryContribution() {
        if (!this.connected || !this.client) {
            return null;
        }

        try {
            const query = `
                SELECT
                    received_via,
                    data_source,
                    uniqExact(device_id) as device_count,
                    count() as reading_count
                FROM sensor_readings
                WHERE timestamp > now() - INTERVAL 24 HOUR
                GROUP BY received_via, data_source
                ORDER BY received_via, device_count DESC
            `;

            const result = await this.client.query({ query, format: 'JSONEachRow' });
            const rows = await result.json();

            const contribution = { local: {}, p2p: {} };
            for (const row of rows) {
                const via = row.received_via === 'p2p' ? 'p2p' : 'local';
                contribution[via][row.data_source] = {
                    devices: parseInt(row.device_count),
                    readings: parseInt(row.reading_count)
                };
            }

            return contribution;

        } catch (error) {
            console.error('Failed to query contribution:', error.message);
            return null;
        }
    }

    isConnected() {
        return this.connected;
    }

    async disconnect() {
        if (this.client) {
            await this.client.close();
            this.connected = false;
            console.log('ClickHouse client disconnected');
        }
    }
}

module.exports = ClickHouseClient;
