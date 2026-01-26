#!/bin/bash
#
# Setup ALL boundary data (ADM0-4) for WeSense Respiro
# Downloads from geoBoundaries, loads into ClickHouse, and regenerates PMTiles
#
# This handles both fresh installs (no data) and upgrades (adding ADM3/4)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BOUNDARIES_DIR="$PROJECT_DIR/data/boundaries"

echo "========================================"
echo "WeSense Respiro - Boundary Data Setup"
echo "========================================"
echo ""

# Ensure boundaries directory exists
mkdir -p "$BOUNDARIES_DIR"
cd "$BOUNDARIES_DIR"

# ============================================
# Step 1: Check and download CGAZ (ADM0/1/2)
# ============================================
echo "[1/5] Checking ADM0/1/2 (CGAZ) data..."

CGAZ_BASE_URL="https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/CGAZ"

download_cgaz() {
    local level=$1
    local filename="geoBoundariesCGAZ_ADM${level}.geojson"
    local output="CGAZ_ADM${level}.geojson"

    if [ -f "$output" ]; then
        echo "  ✓ $output already exists"
        return 0
    fi

    echo "  Downloading ADM${level} from CGAZ..."
    # Try the main CGAZ URL first
    if curl -L -f -o "$output" "${CGAZ_BASE_URL}/${filename}" 2>/dev/null; then
        echo "  ✓ Downloaded $output"
        return 0
    fi

    # Fallback to API lookup
    echo "  Trying API lookup for ADM${level}..."
    local api_url="https://www.geoboundaries.org/api/current/gbOpen/ALL/ADM${level}/"
    # For CGAZ, we need the composite file, not individual countries
    # The CGAZ files are hosted differently

    echo "  ✗ Could not download ADM${level} automatically."
    echo "    Please download manually from: https://www.geoboundaries.org/index.html#getdata"
    echo "    Look for 'CGAZ' (Comprehensive Global Administrative Zones)"
    return 1
}

# Check if processed files exist (skip download if so)
NEED_CGAZ=false
for level in 0 1 2; do
    if [ ! -f "processed_adm${level}.geojson" ] && [ ! -f "CGAZ_ADM${level}.geojson" ]; then
        NEED_CGAZ=true
        break
    fi
done

if [ "$NEED_CGAZ" = true ]; then
    echo "  Downloading CGAZ data (this may take a few minutes)..."

    # Download each level
    for level in 0 1 2; do
        if [ ! -f "CGAZ_ADM${level}.geojson" ] && [ ! -f "processed_adm${level}.geojson" ]; then
            download_cgaz $level || {
                echo ""
                echo "ERROR: Could not download CGAZ ADM${level} data."
                echo "Please download manually from geoBoundaries website."
                exit 1
            }
        fi
    done

    # Process CGAZ files
    echo ""
    echo "  Processing CGAZ files..."
    python3 "$SCRIPT_DIR/../data/boundaries/process_cgaz.py" || {
        # If process_cgaz.py doesn't exist in that location, try the tools dir
        if [ -f "$SCRIPT_DIR/process_cgaz.py" ]; then
            python3 "$SCRIPT_DIR/process_cgaz.py"
        else
            echo "  WARNING: process_cgaz.py not found, skipping processing"
        fi
    }
else
    echo "  ✓ ADM0/1/2 data already exists"
fi

# ============================================
# Step 2: Download ADM3/ADM4
# ============================================
echo ""
echo "[2/5] Downloading ADM3/ADM4 from geoBoundaries..."
echo "      (81 countries for ADM3, 21 countries for ADM4)"
echo "      This may take 10-15 minutes..."
echo ""

python3 "$SCRIPT_DIR/download_adm3_adm4.py"

# ============================================
# Step 3: Load into ClickHouse
# ============================================
echo ""
echo "[3/5] Loading boundaries into ClickHouse..."
python3 "$SCRIPT_DIR/load_boundaries_to_clickhouse.py"

# ============================================
# Step 4: Generate PMTiles
# ============================================
echo ""
echo "[4/5] Regenerating PMTiles..."

if ! command -v tippecanoe &> /dev/null; then
    echo "WARNING: tippecanoe is not installed."
    echo ""
    echo "Install with:"
    echo "  macOS:  brew install tippecanoe"
    echo "  Linux:  See https://github.com/felt/tippecanoe#installation"
    echo ""
    echo "After installing, run this script again or manually run:"
    echo "  cd $BOUNDARIES_DIR && tippecanoe -o regions.pmtiles \\"
    echo "    --layer=adm0 --named-layer=adm0:processed_adm0.geojson \\"
    echo "    --layer=adm1 --named-layer=adm1:processed_adm1.geojson \\"
    echo "    --layer=adm2 --named-layer=adm2:processed_adm2.geojson \\"
    echo "    --layer=adm3 --named-layer=adm3:processed_adm3.geojson \\"
    echo "    --layer=adm4 --named-layer=adm4:processed_adm4.geojson \\"
    echo "    --minimum-zoom=0 --maximum-zoom=14 --simplification=10 \\"
    echo "    --drop-densest-as-needed --extend-zooms-if-still-dropping --force"
else
    # Check which processed files exist
    LAYERS=""
    for level in 0 1 2 3 4; do
        if [ -f "processed_adm${level}.geojson" ]; then
            LAYERS="$LAYERS --layer=adm${level} --named-layer=adm${level}:processed_adm${level}.geojson"
        else
            echo "  Note: processed_adm${level}.geojson not found, skipping ADM${level}"
        fi
    done

    if [ -n "$LAYERS" ]; then
        echo "  Generating PMTiles with available layers..."
        tippecanoe -o regions.pmtiles \
            $LAYERS \
            --minimum-zoom=0 \
            --maximum-zoom=14 \
            --simplification=10 \
            --drop-densest-as-needed \
            --extend-zooms-if-still-dropping \
            --force

        # Copy to public folder
        cp regions.pmtiles "$PROJECT_DIR/public/"
        echo "  ✓ PMTiles generated and copied to public/"
    else
        echo "  ERROR: No processed boundary files found!"
    fi
fi

# ============================================
# Step 5: Clear device region cache
# ============================================
echo ""
echo "[5/5] Clearing device region cache..."
source "$PROJECT_DIR/.env" 2>/dev/null || true

# Try to clear the cache
if curl -s "http://${CLICKHOUSE_USERNAME:-default}:${CLICKHOUSE_PASSWORD}@${CLICKHOUSE_HOST:-localhost}:${CLICKHOUSE_PORT:-8123}" \
    --data "TRUNCATE TABLE wesense_respiro.device_region_cache" 2>/dev/null; then
    echo "  ✓ Cache cleared - will be rebuilt on server restart"
else
    echo "  Note: Could not clear cache (ClickHouse may not be running)"
    echo "  The cache will be rebuilt automatically when the server starts"
fi

echo ""
echo "========================================"
echo "Setup complete!"
echo "========================================"
echo ""
echo "Restart the server to apply changes:"
echo "  npm start"
echo ""
echo "The server will rebuild the device region cache on first startup"
echo "(this may take a few minutes for the first request)."
echo ""
