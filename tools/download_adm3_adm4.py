#!/usr/bin/env python3
"""
Download ADM3 and ADM4 boundary data from geoBoundaries API.

geoBoundaries doesn't provide global CGAZ files for ADM3/ADM4, so we need to
download each country individually and merge them.

Usage:
    python3 tools/download_adm3_adm4.py

Output:
    data/boundaries/downloaded_adm3/*.geojson  (per-country files)
    data/boundaries/downloaded_adm4/*.geojson  (per-country files)
    data/boundaries/merged_adm3.geojson        (merged file)
    data/boundaries/merged_adm4.geojson        (merged file)
    data/boundaries/processed_adm3.geojson     (processed with region_id)
    data/boundaries/processed_adm4.geojson     (processed with region_id)
"""

import json
import os
import sys
import time
import re
import urllib.request
import urllib.error
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
GEOBOUNDARIES_API = "https://www.geoboundaries.org/api/current/gbOpen"
MAX_WORKERS = 5  # Parallel downloads (be nice to the API)
RETRY_DELAY = 2  # Seconds between retries
MAX_RETRIES = 3

# Get paths relative to script location
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
BOUNDARIES_DIR = PROJECT_ROOT / "data" / "boundaries"


def get_countries_with_adm_level(adm_level):
    """Fetch list of countries that have data at the specified admin level."""
    url = f"{GEOBOUNDARIES_API}/ALL/ADM{adm_level}/"
    print(f"Fetching country list for ADM{adm_level}...")

    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            data = json.loads(response.read().decode('utf-8'))

        countries = []
        for item in data:
            iso = item.get('boundaryISO', '')
            download_url = item.get('gjDownloadURL', '')
            unit_count = int(item.get('admUnitCount', 0))
            mean_area = float(item.get('meanAreaSqKM', 0))

            if iso and download_url:
                countries.append({
                    'iso': iso,
                    'url': download_url,
                    'unit_count': unit_count,
                    'mean_area_km2': mean_area
                })

        print(f"  Found {len(countries)} countries with ADM{adm_level} data")
        return countries

    except Exception as e:
        print(f"  Error fetching country list: {e}")
        return []


def download_country_geojson(country_info, adm_level, output_dir):
    """Download GeoJSON for a single country."""
    iso = country_info['iso']
    url = country_info['url']
    output_path = output_dir / f"{iso}_ADM{adm_level}.geojson"

    # Skip if already downloaded
    if output_path.exists():
        return {'iso': iso, 'status': 'cached', 'path': output_path}

    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                data = response.read()

            # Validate JSON
            json.loads(data.decode('utf-8'))

            # Save to file
            output_path.write_bytes(data)
            return {'iso': iso, 'status': 'downloaded', 'path': output_path}

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                return {'iso': iso, 'status': 'failed', 'error': str(e)}

    return {'iso': iso, 'status': 'failed', 'error': 'Max retries exceeded'}


def render_progress_bar(current, total, width=30, label=''):
    """Render a progress bar string."""
    percent = int((current / total) * 100) if total > 0 else 0
    filled = int((current / total) * width) if total > 0 else 0
    empty = width - filled
    bar = '█' * filled + '░' * empty
    return f"  [{bar}] {percent}% ({current}/{total}) {label}"


def download_all_countries(adm_level):
    """Download all countries for an admin level."""
    output_dir = BOUNDARIES_DIR / f"downloaded_adm{adm_level}"
    output_dir.mkdir(parents=True, exist_ok=True)

    countries = get_countries_with_adm_level(adm_level)
    if not countries:
        return []

    results = []
    completed = 0
    downloaded = 0
    cached = 0
    failed = 0
    total = len(countries)
    failed_countries = []

    print(f"\nDownloading ADM{adm_level} boundaries ({total} countries)...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(download_country_geojson, c, adm_level, output_dir): c
            for c in countries
        }

        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1

            if result['status'] == 'downloaded':
                downloaded += 1
                label = f"Downloaded {result['iso']}"
            elif result['status'] == 'cached':
                cached += 1
                label = f"Cached {result['iso']}"
            else:
                failed += 1
                failed_countries.append(f"{result['iso']}: {result.get('error', 'unknown')[:30]}")
                label = f"Failed {result['iso']}"

            # Update progress bar in place
            sys.stdout.write('\r' + render_progress_bar(completed, total, label=label) + '   ')
            sys.stdout.flush()

    # Final line
    sys.stdout.write('\r' + render_progress_bar(total, total, label='Complete') + '   \n')
    sys.stdout.flush()

    print(f"  Summary: {downloaded} downloaded, {cached} cached, {failed} failed")

    # Show failed countries if any
    if failed_countries:
        print(f"  Failed: {', '.join(c.split(':')[0] for c in failed_countries[:10])}" +
              (f" (+{len(failed_countries)-10} more)" if len(failed_countries) > 10 else ""))

    return results


def merge_geojson_files(adm_level):
    """Merge all downloaded country files into a single GeoJSON."""
    input_dir = BOUNDARIES_DIR / f"downloaded_adm{adm_level}"
    output_path = BOUNDARIES_DIR / f"merged_adm{adm_level}.geojson"

    if not input_dir.exists():
        print(f"No downloaded files for ADM{adm_level}")
        return None

    all_features = []
    files = list(input_dir.glob("*.geojson"))
    total = len(files)

    print(f"\nMerging ADM{adm_level} files ({total} countries)...")

    for i, filepath in enumerate(files):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)

            features = data.get('features', [])

            # Add source country to each feature (for debugging)
            iso = filepath.stem.split('_')[0]
            for feature in features:
                if 'properties' not in feature:
                    feature['properties'] = {}
                feature['properties']['_source_iso'] = iso

            all_features.extend(features)

            # Update progress bar
            sys.stdout.write('\r' + render_progress_bar(i + 1, total, label=f"{iso} ({len(all_features)} features)") + '   ')
            sys.stdout.flush()

        except Exception as e:
            sys.stdout.write('\r' + ' ' * 80 + '\r')
            print(f"  Error processing {filepath.name}: {e}")

    sys.stdout.write('\r' + render_progress_bar(total, total, label=f'Complete ({len(all_features)} features)') + '   \n')
    sys.stdout.flush()

    # Create merged GeoJSON
    merged = {
        "type": "FeatureCollection",
        "features": all_features
    }

    print(f"  Writing to {output_path.name}...")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(merged, f, ensure_ascii=False)

    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Created {output_path.name} ({file_size_mb:.1f} MB)")

    return output_path


def fix_double_encoding(text):
    """Fix double-encoded UTF-8 strings."""
    if not isinstance(text, str):
        return text
    try:
        return text.encode('latin-1').decode('utf-8')
    except (UnicodeDecodeError, UnicodeEncodeError):
        return text


def extract_english_name(text):
    """Extract English name from text that may contain both local and English names."""
    if not isinstance(text, str) or not text:
        return text

    # Check if text contains CJK characters
    has_cjk = any(0x4E00 <= ord(c) <= 0x9FFF or 0x3400 <= ord(c) <= 0x4DBF for c in text)

    if not has_cjk:
        return text

    # Try to extract English from parentheses
    match = re.search(r'\(([A-Za-z][A-Za-z\s\-\']+)\)', text)
    if match:
        return match.group(1).strip()

    # Try to find English name after CJK characters
    match = re.search(r'[^\x00-\x7F]+\s+([A-Za-z][A-Za-z\s\-\']+)$', text)
    if match:
        return match.group(1).strip()

    return text


def process_merged_file(adm_level):
    """Process merged file to add consistent region_id properties."""
    input_path = BOUNDARIES_DIR / f"merged_adm{adm_level}.geojson"
    output_path = BOUNDARIES_DIR / f"processed_adm{adm_level}.geojson"

    if not input_path.exists():
        print(f"No merged file for ADM{adm_level}")
        return None

    print(f"\nProcessing ADM{adm_level} file...")
    print(f"  Loading {input_path.name}...")

    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    features = data['features']
    total = len(features)

    print(f"  Normalizing {total} features...")

    for i, feature in enumerate(features):
        props = feature.get('properties', {})

        # Get country code - try multiple possible fields
        country_code = (
            props.get('shapeGroup') or
            props.get('_source_iso') or
            props.get('ISO_A3') or
            'UNK'
        )

        # Get shape ID for uniqueness
        shape_id = (
            props.get('shapeID') or
            props.get('shapeName', '') + str(hash(str(props)))
        )

        # Create region_id
        if shape_id:
            short_id = str(shape_id)[-12:]  # Use more chars for ADM3/4 uniqueness
            region_id = f"{country_code}_ADM{adm_level}_{short_id}"
        else:
            region_id = f"{country_code}_ADM{adm_level}_{hash(str(props))}"

        # Get and clean name
        raw_name = props.get('shapeName', props.get('name', ''))
        fixed_name = fix_double_encoding(raw_name)
        english_name = extract_english_name(fixed_name)

        # Normalize properties
        feature['properties'] = {
            'region_id': region_id,
            'name': english_name,
            'country_code': country_code,
            'admin_level': adm_level,
            'original_id': str(shape_id) if shape_id else ''
        }

        # Update progress every 1000 features
        if (i + 1) % 1000 == 0 or i == total - 1:
            sys.stdout.write('\r' + render_progress_bar(i + 1, total) + '   ')
            sys.stdout.flush()

    sys.stdout.write('\r' + render_progress_bar(total, total, label='Complete') + '   \n')
    sys.stdout.flush()

    # Write processed file
    print(f"  Writing to {output_path.name}...")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)

    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Created {output_path.name} ({file_size_mb:.1f} MB)")

    return output_path


def main():
    print("=" * 60)
    print("geoBoundaries ADM3/ADM4 Downloader")
    print("=" * 60)

    # Ensure output directory exists
    BOUNDARIES_DIR.mkdir(parents=True, exist_ok=True)

    # Process ADM3
    print("\n" + "=" * 60)
    print("PROCESSING ADM3")
    print("=" * 60)
    download_all_countries(3)
    merge_geojson_files(3)
    process_merged_file(3)

    # Process ADM4
    print("\n" + "=" * 60)
    print("PROCESSING ADM4")
    print("=" * 60)
    download_all_countries(4)
    merge_geojson_files(4)
    process_merged_file(4)

    # Print tippecanoe command
    print("\n" + "=" * 60)
    print("NEXT STEPS")
    print("=" * 60)
    print("""
1. Load boundaries into ClickHouse:
   python3 tools/load_boundaries_to_clickhouse.py

2. Regenerate PMTiles with all admin levels:
   cd data/boundaries
   tippecanoe -o regions.pmtiles \\
     --layer=adm0 --named-layer=adm0:processed_adm0.geojson \\
     --layer=adm1 --named-layer=adm1:processed_adm1.geojson \\
     --layer=adm2 --named-layer=adm2:processed_adm2.geojson \\
     --layer=adm3 --named-layer=adm3:processed_adm3.geojson \\
     --layer=adm4 --named-layer=adm4:processed_adm4.geojson \\
     --minimum-zoom=0 \\
     --maximum-zoom=14 \\
     --simplification=10 \\
     --drop-densest-as-needed \\
     --extend-zooms-if-still-dropping \\
     --force
""")


if __name__ == '__main__':
    main()
