# Heatmap Tile Rendering — Investigation Status

## What works

- Heatmap tiles render globally (proxy/tile serving is fixed)
- ADM1 (zoom 1-4) shows Auckland with WeSense data
- ADM2 (zoom 5+) shows Wellington, Canterbury, Nelson (Meshtastic data)
- The Auckland ADM2 feature (NZL_ADM2_65097584) IS in the tile (confirmed via raw byte search)
- The API returns Auckland ADM2 data (27.8°C, 2 sensors)
- Clicking Auckland at zoom 5+ shows the correct popup with data
- The device_region_cache correctly maps WeSense devices to NZL_ADM2_65097584

## What doesn't work

- The dynamicFill function is never called for NZL_ADM2_65097584 at zoom 5+
- This ONLY affects WeSense sensors (which currently only exist in Auckland)
- Auckland shows at ADM1 (zoom 1-4) but disappears at ADM2 (zoom 5+)

## What changed during this session

- Switched from PMTiles range requests to ZXY tile API (`/api/tiles/{z}/{x}/{y}.mvt`) to fix reverse proxy corruption
- Added try/catch error boundary around dynamicFill (fixed blank tiles from silent throws)
- Added ADM4 paint rule and aligned zoom ranges with tippecanoe generation
- Added FINAL to all device_region_cache queries (ReplacingMergeTree dedup)
- Added empty region ID re-check in cache refresh cycle
- Disabled ETags on static files
- Reduced simplification from 10 to 4 (unlikely to be the fix — see below)
- Changed from --drop-densest-as-needed to --no-feature-limit --no-tile-size-limit

## Key observations

- The fill function IS called for other NZL_ADM2 regions in the same tile, but NOT for 65097584
- This is NOT a simplification issue — if the polygon were simplified to nothing, it wouldn't appear in the raw tile bytes at all. But it IS there.
- The simplification value (10) hasn't changed from when it was working
- protomaps-leaflet parses the tile and finds the feature, but does not call the fill for it — this suggests protomaps-leaflet filters it out during its own internal geometry processing (e.g., degenerate polygon, zero-area after coordinate rounding)

## What we don't know

- Whether this feature renders when using PMTiles file directly (the old approach that worked locally before the ZXY switch)
- Whether the issue is specific to ZXY tile serving or the PMTiles data itself
- Whether the Waitematā polygon geometry is degenerate at zoom 5-6 in the new PMTiles (coordinates rounded to sub-pixel, zero-area)
- Whether this is a regression from the ZXY switch or was already broken before

## Next steps to investigate

1. **Test PMTiles vs ZXY locally**: Go to localhost:3000, test the heatmap with the PMTiles file directly (revert URL to `/regions.pmtiles`). If Auckland shows at zoom 5+ with PMTiles but not ZXY, the issue is in the tile serving/decompression path.
2. **Falsify a WeSense sensor elsewhere**: Place a WeSense sensor reading in a larger ADM2 region to confirm whether the issue is WeSense-specific or Auckland-geometry-specific.
3. **Inspect the actual polygon geometry**: Parse the tile's MVT protobuf for NZL_ADM2_65097584 and check if the geometry has valid coordinates and non-zero area at zoom 5-6.
4. **Check if the old PMTiles (before ZXY switch) had working Auckland ADM2**: The original PMTiles file was 291MB (local) vs the regenerated ones (285-320MB). The boundary data or tippecanoe settings may have changed.

## Fixes applied during this session (committed)

1. **dynamicFill error boundary** — try/catch prevents silent tile render abort
2. **ZXY tile API** — bypasses reverse proxy corruption of PMTiles range requests
3. **ETags disabled** — prevents proxy ETag conflicts
4. **Compression excluded for .mvt** — prevents double-compression through proxy
5. **ADM4 paint rule** — Docker PMTiles includes ADM4 boundaries
6. **Inclusive zoom ranges** — protomaps-leaflet uses inclusive min/maxzoom
7. **FINAL on cache queries** — ClickHouse ReplacingMergeTree dedup
8. **Empty region re-check** — devices cached before boundaries loaded get re-processed
9. **Layer re-add after data refresh** — force re-render instead of unreliable rerenderTiles()
10. **Board type friendly names** — formatBoardModel() for filter and display
11. **Map jump mitigation** — deferred setView restores (partially effective)
