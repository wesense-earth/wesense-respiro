#!/usr/bin/env node
/**
 * Diagnostic script to check sensor data in ClickHouse
 * Usage: node tools/check-sensor-data.js <device_id> [reading_type] [days]
 *
 * Examples:
 *   node tools/check-sensor-data.js "my-sensor-id"
 *   node tools/check-sensor-data.js "my-sensor-id" temperature 7
 */

require('dotenv').config();
const { createClient } = require('@clickhouse/client');

const deviceId = process.argv[2];
const readingType = process.argv[3] || 'temperature';
const days = parseInt(process.argv[4]) || 30;

if (!deviceId) {
    console.error('Usage: node tools/check-sensor-data.js <device_id> [reading_type] [days]');
    console.error('Example: node tools/check-sensor-data.js "esp32-abc123" temperature 7');
    process.exit(1);
}

async function checkSensorData() {
    const client = createClient({
        host: `http://${process.env.CLICKHOUSE_HOST || 'localhost'}:${process.env.CLICKHOUSE_PORT || 8123}`,
        database: process.env.CLICKHOUSE_DATABASE || 'wesense',
        username: process.env.CLICKHOUSE_USERNAME || 'default',
        password: process.env.CLICKHOUSE_PASSWORD || ''
    });

    console.log(`\n=== Checking sensor: ${deviceId} ===\n`);

    try {
        // 1. Check what reading types exist for this device
        console.log('1. Available reading types:');
        const typesResult = await client.query({
            query: `
                SELECT reading_type, count() as count,
                       min(timestamp) as first_reading,
                       max(timestamp) as last_reading
                FROM sensor_readings
                WHERE device_id = {deviceId:String}
                GROUP BY reading_type
                ORDER BY count DESC
            `,
            format: 'JSONEachRow',
            query_params: { deviceId }
        });
        const types = await typesResult.json();
        console.table(types);

        // 2. Check recent readings for the specified type
        console.log(`\n2. Last 20 ${readingType} readings:`);
        const recentResult = await client.query({
            query: `
                SELECT timestamp, value
                FROM sensor_readings
                WHERE device_id = {deviceId:String}
                  AND reading_type = {readingType:String}
                ORDER BY timestamp DESC
                LIMIT 20
            `,
            format: 'JSONEachRow',
            query_params: { deviceId, readingType }
        });
        const recent = await recentResult.json();
        console.table(recent);

        // 3. Check for value distribution over the past N days
        console.log(`\n3. Value distribution (last ${days} days):`);
        const distResult = await client.query({
            query: `
                SELECT
                    round(value, 1) as value_bucket,
                    count() as count
                FROM sensor_readings
                WHERE device_id = {deviceId:String}
                  AND reading_type = {readingType:String}
                  AND timestamp > now() - INTERVAL ${days} DAY
                GROUP BY value_bucket
                ORDER BY count DESC
                LIMIT 20
            `,
            format: 'JSONEachRow',
            query_params: { deviceId, readingType }
        });
        const dist = await distResult.json();
        console.table(dist);

        // 4. Check for gaps in data
        console.log(`\n4. Data gaps > 1 hour (last ${days} days):`);
        const gapsResult = await client.query({
            query: `
                SELECT
                    prev_ts as gap_start,
                    curr_ts as gap_end,
                    round(gap_minutes / 60, 1) as gap_hours
                FROM (
                    SELECT
                        timestamp as curr_ts,
                        lagInFrame(timestamp) OVER (ORDER BY timestamp) as prev_ts,
                        dateDiff('minute', lagInFrame(timestamp) OVER (ORDER BY timestamp), timestamp) as gap_minutes
                    FROM sensor_readings
                    WHERE device_id = {deviceId:String}
                      AND reading_type = {readingType:String}
                      AND timestamp > now() - INTERVAL ${days} DAY
                )
                WHERE gap_minutes > 60
                ORDER BY gap_minutes DESC
                LIMIT 10
            `,
            format: 'JSONEachRow',
            query_params: { deviceId, readingType }
        });
        const gaps = await gapsResult.json();
        if (gaps.length > 0) {
            console.table(gaps);
        } else {
            console.log('  No gaps > 1 hour found');
        }

        // 5. Check for suspicious patterns (same value repeated many times)
        console.log(`\n5. Consecutive identical values (potential stuck sensor):`);
        const stuckResult = await client.query({
            query: `
                SELECT
                    value,
                    count() as consecutive_count,
                    min(timestamp) as first_occurrence,
                    max(timestamp) as last_occurrence
                FROM (
                    SELECT
                        timestamp,
                        value,
                        value - lagInFrame(value) OVER (ORDER BY timestamp) as value_diff
                    FROM sensor_readings
                    WHERE device_id = {deviceId:String}
                      AND reading_type = {readingType:String}
                      AND timestamp > now() - INTERVAL ${days} DAY
                )
                WHERE value_diff = 0 OR value_diff IS NULL
                GROUP BY value
                HAVING count() > 5
                ORDER BY consecutive_count DESC
                LIMIT 10
            `,
            format: 'JSONEachRow',
            query_params: { deviceId, readingType }
        });
        const stuck = await stuckResult.json();
        if (stuck.length > 0) {
            console.table(stuck);
        } else {
            console.log('  No suspicious patterns found');
        }

    } catch (error) {
        console.error('Error:', error.message);
    } finally {
        await client.close();
    }
}

checkSensorData();
