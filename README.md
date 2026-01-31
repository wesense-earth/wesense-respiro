# WeSense Respiro

Community-driven environmental sensor monitoring dashboard. Displays real-time sensor data on an interactive map with choropleth region overlays, powered by ClickHouse and MQTT.

> For detailed documentation, see the [Wiki](https://github.com/wesense-earth/wesense-respiro/wiki).
> Read on for a project overview and quick install instructions.

## Overview

Respiro plots contributed sensor locations on a Leaflet map and aggregates readings (temperature, humidity, pressure, CO2, PM2.5, VOC) into regional heatmaps using crowdsourced data science. Sensor data arrives from WeSense ingesters via ClickHouse and optionally via real-time MQTT subscription.

**Key features:**
- Interactive map with sensor markers showing segmented ring icons (temperature, humidity, pressure, CO2)
- Choropleth region heatmaps using PMTiles vector boundaries
- Dashboard view with sparkline charts
- Dark mode
- Deployment type filtering (Indoor/Outdoor/Mixed)
- Data-source-aware freshness thresholds
- Self-hostable — run your own instance on a home server or Raspberry Pi

WeSense is a truly open telemetry system — its data is never locked behind an API paywall. The entire database history will be freely available to download and host yourself via IPFS.

## Quick Install (Recommended)

Most users should deploy via [wesense-deploy](https://github.com/wesense-earth/wesense-deploy), which orchestrates all WeSense services using Docker Compose profiles:

```bash
# Clone the deploy repo
git clone https://github.com/wesense-earth/wesense-deploy.git
cd wesense-deploy

# Configure
cp .env.sample .env
# Edit .env with your settings

# Start as a full station (includes EMQX, ClickHouse, Ingesters, Respiro)
docker compose --profile station up -d

# Access the map at http://localhost:3000
```

For Unraid or manual deployments, use the docker-run script:

```bash
./scripts/docker-run.sh station
```

See [Deployment Personas](https://github.com/wesense-earth/wesense-deploy) for all options.

## Docker (Standalone)

For running Respiro independently (e.g. pointed at a remote ClickHouse):

```bash
docker pull ghcr.io/wesense-earth/wesense-respiro:latest

docker run -d \
  --name wesense-respiro \
  --restart unless-stopped \
  -p 3000:3000 \
  -e CLICKHOUSE_HOST=your-clickhouse-host \
  -e CLICKHOUSE_PORT=8123 \
  -e CLICKHOUSE_DATABASE=wesense \
  -e CLICKHOUSE_USERNAME=wesense \
  -e CLICKHOUSE_PASSWORD= \
  -e MQTT_BROKER_URL=mqtt://your-mqtt-broker:1883 \
  -e MQTT_TOPIC_FILTER=wesense/decoded/# \
  -e MAP_CENTER_LAT=-36.848 \
  -e MAP_CENTER_LNG=174.763 \
  -e MAP_ZOOM_LEVEL=10 \
  -v respiro-data:/app/data \
  ghcr.io/wesense-earth/wesense-respiro:latest
```

## Local Development

```bash
# Install dependencies
npm install

# Start the server
npm start

# Server runs at http://localhost:3000
```

### Prerequisites

- Node.js >= 14.0.0
- ClickHouse server (for sensor data and region boundaries)
- Python 3 (for boundary download/processing scripts, optional)
- tippecanoe (for PMTiles generation, optional)

### Environment Variables

```env
# Required
CLICKHOUSE_HOST=192.168.43.11
CLICKHOUSE_PORT=8123
CLICKHOUSE_DATABASE=wesense
CLICKHOUSE_USERNAME=wesense
CLICKHOUSE_PASSWORD=

# Optional MQTT (for real-time updates)
MQTT_BROKER_URL=mqtt://localhost:1883
MQTT_USERNAME=
MQTT_PASSWORD=
MQTT_TOPIC_FILTER=wesense/decoded/#

# Server
PORT=3000
HOST=0.0.0.0

# Map defaults (Auckland, NZ)
MAP_CENTER_LAT=-36.848
MAP_CENTER_LNG=174.763
MAP_ZOOM_LEVEL=10
```

## Architecture

```
ClickHouse (wesense.sensor_readings) ──→ Express API ──→ Browser (Leaflet map)
                                              ▲
MQTT (wesense/decoded/#) ─── real-time ───────┘
```

### Backend (Node.js / Express)

- `src/index.js` — Server entry point, API routes (`/api/sensors`, `/api/regions`, `/api/history`)
- `src/clickhouse-client.js` — Sensor data queries, sparklines, regional aggregates
- `src/region-service.js` — Spatial queries using ClickHouse `pointInPolygon()`
- `src/mqtt-manager.js` — Real-time MQTT subscription

### Frontend (Vanilla JS + Leaflet)

- `public/app.js` — Map rendering, MarkerCluster, choropleth overlays, dashboard widgets
- Segmented ring markers with temperature/humidity/pressure/CO2 quadrants
- Region heatmap using protomaps-leaflet with PMTiles boundaries
- BOM 2013 temperature colour scale

## Sensor Reading Types

temperature, humidity, pressure, co2, pm1_0, pm2_5, pm10, voc_index, nox_index

## Related

- [wesense-deploy](https://github.com/wesense-earth/wesense-deploy) — Docker Compose orchestration
- [wesense-ingester-meshtastic](https://github.com/wesense-earth/wesense-ingester-meshtastic) — Meshtastic data ingester
- [wesense-ingester-wesense](https://github.com/wesense-earth/wesense-ingester-wesense) — WeSense WiFi/LoRa ingester
- [wesense-ingester-homeassistant](https://github.com/wesense-earth/wesense-ingester-homeassistant) — Home Assistant ingester
- [wesense-ingester-core](https://github.com/wesense-earth/wesense-ingester-core) — Shared ingester library

## License

MIT
