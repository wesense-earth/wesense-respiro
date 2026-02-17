#!/usr/bin/env python3
"""
Process CGAZ GeoJSON files to add consistent region_id properties
and prepare for PMTiles generation.
"""

import json
import sys

def fix_double_encoding(text):
    """
    Fix double-encoded UTF-8 strings.
    CGAZ data sometimes has UTF-8 bytes incorrectly treated as Latin-1
    and re-encoded, producing mojibake like "Ä" instead of "ā".
    """
    if not isinstance(text, str):
        return text
    try:
        return text.encode('latin-1').decode('utf-8')
    except (UnicodeDecodeError, UnicodeEncodeError):
        return text

import re

def extract_english_name(text):
    """
    Extract English name from text that may contain both local and English names.
    Handles patterns like:
    - "大埤鄉 (Dapi)" → "Dapi"
    - "北區" → "北區" (no change if no English)
    - "Dayuan District" → "Dayuan District" (already English)
    """
    if not isinstance(text, str) or not text:
        return text

    # Check if text contains CJK characters
    has_cjk = any(0x4E00 <= ord(c) <= 0x9FFF or 0x3400 <= ord(c) <= 0x4DBF for c in text)

    if not has_cjk:
        return text  # Already in English/Latin script

    # Try to extract English from parentheses: "大埤鄉 (Dapi)" → "Dapi"
    match = re.search(r'\(([A-Za-z][A-Za-z\s\-\']+)\)', text)
    if match:
        return match.group(1).strip()

    # Try to find English name after space/dash: "北區 North" → "North"
    match = re.search(r'[^\x00-\x7F]+\s+([A-Za-z][A-Za-z\s\-\']+)$', text)
    if match:
        return match.group(1).strip()

    # No English found, return original
    return text

def process_cgaz_file(input_file, output_file, admin_level):
    """Process a CGAZ GeoJSON file and add region_id properties."""
    print(f"Processing {input_file}...")

    with open(input_file, 'r') as f:
        data = json.load(f)

    features = data['features']
    print(f"  Found {len(features)} features")

    for feature in features:
        props = feature['properties']

        # Create region_id: {ISO3}_{ADM_LEVEL}_{UNIQUE_ID}
        country_code = props.get('shapeGroup', 'UNK')
        shape_id = props.get('shapeID', '')

        if admin_level == 0:
            # For ADM0, use country code as the ID
            region_id = f"{country_code}_ADM0"
        else:
            # For ADM1/ADM2, use shapeID (or generate one)
            if shape_id:
                # Use last 8 chars of shapeID for uniqueness
                short_id = shape_id[-8:]
                region_id = f"{country_code}_ADM{admin_level}_{short_id}"
            else:
                region_id = f"{country_code}_ADM{admin_level}_{hash(props.get('shapeName', ''))}"

        # Normalize properties (fix encoding issues and extract English names)
        raw_name = props.get('shapeName', '')
        fixed_name = fix_double_encoding(raw_name)
        english_name = extract_english_name(fixed_name)

        feature['properties'] = {
            'region_id': region_id,
            'name': english_name,
            'country_code': country_code,
            'admin_level': admin_level,
            'original_id': shape_id
        }

    # Write output with proper UTF-8 encoding
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)

    print(f"  Wrote {output_file}")
    return len(features)

def main():
    total = 0

    # Process each admin level
    total += process_cgaz_file('CGAZ_ADM0.geojson', 'processed_adm0.geojson', 0)
    total += process_cgaz_file('CGAZ_ADM1.geojson', 'processed_adm1.geojson', 1)
    total += process_cgaz_file('CGAZ_ADM2.geojson', 'processed_adm2.geojson', 2)

    print(f"\nTotal features processed: {total}")
    print("\nNow run tippecanoe to generate PMTiles:")
    print("""
tippecanoe -o regions.pmtiles \\
  --layer=adm0 --named-layer=adm0:processed_adm0.geojson \\
  --layer=adm1 --named-layer=adm1:processed_adm1.geojson \\
  --layer=adm2 --named-layer=adm2:processed_adm2.geojson \\
  --minimum-zoom=0 \\
  --maximum-zoom=12 \\
  --simplification=10 \\
  --drop-densest-as-needed \\
  --extend-zooms-if-still-dropping \\
  --force
""")

if __name__ == '__main__':
    main()
