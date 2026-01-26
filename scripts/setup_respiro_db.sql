-- Create wesense_respiro database for consumer-specific data
-- This keeps region mapping separate from core wesense telemetry data

CREATE DATABASE IF NOT EXISTS wesense_respiro;

-- Region boundaries table for point-in-polygon queries
CREATE TABLE IF NOT EXISTS wesense_respiro.region_boundaries (
    region_id String,
    admin_level UInt8,
    name String,
    country_code String,
    polygon Array(Array(Tuple(Float64, Float64)))
) ENGINE = MergeTree()
ORDER BY (admin_level, country_code, region_id);

-- Cache device locations to regions (updated periodically)
-- This avoids expensive point-in-polygon queries at read time
CREATE TABLE IF NOT EXISTS wesense_respiro.device_region_cache (
    device_id String,
    latitude Float64,
    longitude Float64,
    region_adm0_id String,
    region_adm1_id String,
    region_adm2_id String,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY device_id;

-- Index on lat/lng for spatial queries
ALTER TABLE wesense_respiro.device_region_cache
ADD INDEX IF NOT EXISTS idx_lat_lng (latitude, longitude) TYPE minmax GRANULARITY 1;
