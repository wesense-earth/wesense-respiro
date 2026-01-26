#!/usr/bin/env python3
"""
Load GeoJSON boundary files into ClickHouse for spatial queries.
Per architecture document docs/region-overlay-architecture.md Section 4.2-4.3
"""

import json
import os
import sys
import clickhouse_connect

# Load .env file if present
def load_dotenv():
    """Load environment variables from .env file."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(os.path.dirname(script_dir), '.env')
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ.setdefault(key.strip(), value.strip())

load_dotenv()

# ClickHouse connection settings (HTTP interface)
CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.environ.get('CLICKHOUSE_PORT', 8123))
CLICKHOUSE_USER = os.environ.get('CLICKHOUSE_USERNAME', 'default')
CLICKHOUSE_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD', '')

# GeoJSON files to load (ADM3/ADM4 are optional - only loaded if files exist)
BOUNDARY_FILES = [
    ('data/boundaries/processed_adm0.geojson', 0),
    ('data/boundaries/processed_adm1.geojson', 1),
    ('data/boundaries/processed_adm2.geojson', 2),
    ('data/boundaries/processed_adm3.geojson', 3),  # Optional: 81 countries, ~105K units
    ('data/boundaries/processed_adm4.geojson', 4),  # Optional: 21 countries, ~94K units
]

def get_base_path():
    """Get the base path of the project."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.dirname(script_dir)

def connect_clickhouse():
    """Connect to ClickHouse via HTTP."""
    print(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}...")
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    # Test connection
    result = client.query('SELECT 1')
    print("Connected successfully!")
    return client

def create_schema(client):
    """Create the region_boundaries table in the wesense_respiro database."""
    print("\nCreating region_boundaries table in wesense_respiro database...")

    # Drop existing table to recreate with correct schema
    client.command('DROP TABLE IF EXISTS wesense_respiro.region_boundaries')

    # Create region_boundaries table
    # Using Array(Tuple(Float64, Float64)) for polygon rings
    # ClickHouse pointInPolygon expects Array(Tuple(Float64, Float64))
    client.command('''
        CREATE TABLE wesense_respiro.region_boundaries (
            region_id String,
            admin_level UInt8,
            name String,
            country_code String,
            original_id String,
            -- Polygon stored as array of rings, each ring is array of (lon, lat) tuples
            polygon Array(Array(Tuple(Float64, Float64))),
            -- Bounding box for fast filtering
            bbox_min_lon Float64,
            bbox_max_lon Float64,
            bbox_min_lat Float64,
            bbox_max_lat Float64
        ) ENGINE = MergeTree()
        ORDER BY (admin_level, country_code, region_id)
    ''')

    print("Schema created successfully!")

def extract_polygon_coords(geometry):
    """
    Extract polygon coordinates from GeoJSON geometry.
    Returns list of rings, each ring is list of (lon, lat) tuples.
    Handles both Polygon and MultiPolygon types.
    For MultiPolygon, takes the largest polygon by point count.
    """
    geom_type = geometry.get('type')
    coords = geometry.get('coordinates', [])

    if geom_type == 'Polygon':
        # Polygon: coordinates is array of rings
        # Each ring is array of [lon, lat] points
        rings = []
        for ring in coords:
            ring_coords = [(float(pt[0]), float(pt[1])) for pt in ring]
            rings.append(ring_coords)
        return rings

    elif geom_type == 'MultiPolygon':
        # MultiPolygon: find the largest polygon by point count in outer ring
        if coords:
            largest_polygon = None
            largest_point_count = 0

            for polygon in coords:
                if polygon and polygon[0]:
                    point_count = len(polygon[0])  # Count points in outer ring
                    if point_count > largest_point_count:
                        largest_point_count = point_count
                        largest_polygon = polygon

            if largest_polygon:
                rings = []
                for ring in largest_polygon:
                    ring_coords = [(float(pt[0]), float(pt[1])) for pt in ring]
                    rings.append(ring_coords)
                return rings

    return []

def compute_bbox(polygon_rings):
    """Compute bounding box from polygon rings."""
    if not polygon_rings or not polygon_rings[0]:
        return (0, 0, 0, 0)

    all_coords = []
    for ring in polygon_rings:
        all_coords.extend(ring)

    lons = [c[0] for c in all_coords]
    lats = [c[1] for c in all_coords]

    return (min(lons), max(lons), min(lats), max(lats))


def render_progress_bar(current, total, width=30, label=''):
    """Render a progress bar string."""
    percent = int((current / total) * 100) if total > 0 else 0
    filled = int((current / total) * width) if total > 0 else 0
    empty = width - filled
    bar = '█' * filled + '░' * empty
    return f"  [{bar}] {percent}% ({current}/{total}) {label}"

def load_geojson_file(client, filepath, expected_admin_level):
    """Load a single GeoJSON file into ClickHouse."""
    base_path = get_base_path()
    full_path = os.path.join(base_path, filepath)

    print(f"\nLoading {filepath}...")

    if not os.path.exists(full_path):
        print(f"  WARNING: File not found: {full_path}")
        return 0

    with open(full_path, 'r') as f:
        data = json.load(f)

    features = data.get('features', [])
    total = len(features)
    print(f"  Processing {total} features...")

    # Prepare batch insert
    rows = []
    skipped = 0
    inserted = 0

    for i, feature in enumerate(features):
        props = feature.get('properties', {})
        geometry = feature.get('geometry', {})

        region_id = props.get('region_id') or ''
        admin_level = props.get('admin_level') or expected_admin_level
        name = props.get('name') or ''  # Handle None
        country_code = props.get('country_code') or ''
        original_id = props.get('original_id') or ''

        # Extract polygon coordinates
        polygon_rings = extract_polygon_coords(geometry)

        if not polygon_rings or not polygon_rings[0]:
            skipped += 1
            continue

        # Compute bounding box
        bbox = compute_bbox(polygon_rings)

        rows.append((
            region_id,
            admin_level,
            name,
            country_code,
            original_id,
            polygon_rings,
            bbox[0], bbox[1], bbox[2], bbox[3]
        ))

        # Insert in batches of 1000
        if len(rows) >= 1000:
            client.insert(
                'wesense_respiro.region_boundaries',
                rows,
                column_names=['region_id', 'admin_level', 'name', 'country_code', 'original_id',
                              'polygon', 'bbox_min_lon', 'bbox_max_lon', 'bbox_min_lat', 'bbox_max_lat']
            )
            inserted += len(rows)
            rows = []

        # Update progress bar
        if (i + 1) % 500 == 0 or i == total - 1:
            sys.stdout.write('\r' + render_progress_bar(i + 1, total, label=f'{inserted + len(rows)} inserted') + '   ')
            sys.stdout.flush()

    # Insert remaining rows
    if rows:
        client.insert(
            'wesense_respiro.region_boundaries',
            rows,
            column_names=['region_id', 'admin_level', 'name', 'country_code', 'original_id',
                          'polygon', 'bbox_min_lon', 'bbox_max_lon', 'bbox_min_lat', 'bbox_max_lat']
        )
        inserted += len(rows)

    sys.stdout.write('\r' + render_progress_bar(total, total, label='Complete') + '   \n')
    sys.stdout.flush()

    print(f"  Loaded {total - skipped} features, skipped {skipped}")
    return total - skipped

def update_device_region_cache_schema(client):
    """Add ADM3/ADM4 columns to device_region_cache if they don't exist."""
    print("\nUpdating device_region_cache schema for ADM3/ADM4...")

    # Check if columns already exist
    result = client.query('''
        SELECT name FROM system.columns
        WHERE database = 'wesense_respiro' AND table = 'device_region_cache'
    ''')
    existing_columns = {row[0] for row in result.result_rows}

    columns_to_add = []
    if 'region_adm3_id' not in existing_columns:
        columns_to_add.append(('region_adm3_id', "String DEFAULT ''"))
    if 'region_adm4_id' not in existing_columns:
        columns_to_add.append(('region_adm4_id', "String DEFAULT ''"))

    if not columns_to_add:
        print("  ADM3/ADM4 columns already exist")
        return

    for col_name, col_type in columns_to_add:
        print(f"  Adding column {col_name}...")
        client.command(f'''
            ALTER TABLE wesense_respiro.device_region_cache
            ADD COLUMN IF NOT EXISTS {col_name} {col_type}
        ''')

    print("  Schema updated successfully!")


def verify_data(client):
    """Verify the loaded data."""
    print("\nVerifying loaded data...")

    result = client.query('''
        SELECT admin_level, count() as cnt
        FROM wesense_respiro.region_boundaries
        GROUP BY admin_level
        ORDER BY admin_level
    ''')

    print("Counts by admin level:")
    for row in result.result_rows:
        print(f"  ADM{row[0]}: {row[1]} regions")

    # Test a sample query
    print("\nSample NZ regions:")
    result = client.query('''
        SELECT region_id, name, country_code
        FROM wesense_respiro.region_boundaries
        WHERE country_code = 'NZL' AND admin_level = 2
        LIMIT 5
    ''')
    for row in result.result_rows:
        print(f"  {row[0]}: {row[1]} ({row[2]})")

    # Test pointInPolygon for Auckland
    print("\nTesting pointInPolygon for Auckland (174.763, -36.848)...")
    result = client.query('''
        SELECT region_id, name, admin_level
        FROM wesense_respiro.region_boundaries
        WHERE pointInPolygon((174.763, -36.848), polygon[1])
        LIMIT 5
    ''')
    for row in result.result_rows:
        print(f"  ADM{row[2]}: {row[0]} - {row[1]}")

def main():
    """Main entry point."""
    print("=" * 60)
    print("Loading boundary data into ClickHouse")
    print("=" * 60)

    try:
        client = connect_clickhouse()
        create_schema(client)

        total_loaded = 0
        for filepath, admin_level in BOUNDARY_FILES:
            loaded = load_geojson_file(client, filepath, admin_level)
            total_loaded += loaded

        # Update device_region_cache schema for ADM3/ADM4
        update_device_region_cache_schema(client)

        verify_data(client)

        print("\n" + "=" * 60)
        print(f"Successfully loaded {total_loaded} total regions!")
        print("=" * 60)
        print("\nNOTE: After loading new ADM levels, you should:")
        print("  1. Clear the device_region_cache to re-compute ADM3/ADM4 assignments")
        print("  2. Restart the server to trigger cache refresh")
        print("  3. Regenerate PMTiles with the new layers (see tippecanoe command)")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
