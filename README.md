# WeSense Sensor Map

WeSense Respiro is a community oriented environmental sensor monitoring dashboard that displays real-time wesense environmental data via the wesense ingester plugin system. Notable examples in the plugin system include the premium wesense-esp32_sensorarray-automatic, wesense-ingestger-meshtastic and wesense-ingester-homeassistant. The interactive map plots contributed sensor locations as well as mapping tiles that aims to show highly accurate temperature ratings though crowdsourced data science, not usually available through cheap sensor units. It aggregates sensor readings (temperature, humidity, pressure, CO2, PM2.5, VOC with more coming) and displays them with choropleth region overlays. The map intends to be a feature rich, self hostable solution for mapping, querying and comparing your own local real-time readings against global norms as well as comparing to a rich history afforded by the WeSense distributed IPFS stored environmental telemetry record, queried through clickhouse. Wesense is the only true open telemetry system, in that it's data is not locked behind an API paywall.  WeSense Respiro is a community-driven environmental monitoring dashboard that displays real-time sensor data through the WeSense ingester plugin system. Notable plugins include wesense-esp32-sensorarray-automatic, wesense-ingester-meshtastic, and wesense-ingester-homeassistant.  The interactive map plots contributed sensor locations and uses crowdsourced data science to generate highly accurate regional temperature readings—precision typically unattainable from low-cost sensor units alone. It aggregates environmental readings (temperature, humidity, pressure, CO₂, PM2.5, VOC, with more to come) and visualizes them through choropleth region overlays.  Designed as a feature-rich, self-hostable solution, Respiro enables you to map, query, and compare your local real-time readings against global norms. You can also explore historical trends through the WeSense distributed IPFS-stored telemetry record, queried via ClickHouse. WeSense is a truly open telemetry system—its data is never locked behind an API paywall, the entire database history is freely available to download and host yourself via IPFS.

## Features

- **Interactive OpenStreetMap** with sensor location markers
- **Real-time MQTT subscription** to ESP32 sensor topics
- **Sensor readings dashboard** displaying temperature, humidity, CO2, air quality, etc.
- **Automatic colour-coding** based on temperature ranges
- **Responsive design** for desktop and mobile devices
- **Minimal dependencies** for easy deployment on home servers and educational institutions
- **Zero configuration** - works out of the box with sensible defaults

## Architecture

```
ESP32 Sensors
     |
     | MQTT Topics
     | (wesense/v1/... or skytrace/esp32/env/...)
     |
     v
  MQTT Broker
     |
     v
Node.js Server
     |
     +-- Express API (/api/sensors, /api/stats)
     +-- MQTT Client (listens to topics)
     +-- Sensor Store (in-memory data)
     |
     v
   Browser
     |
     +-- Leaflet.js Map
     +-- Real-time sensor display
     +-- Auto-refresh (10s)
```

## Prerequisites

- **Node.js** 14.0.0 or higher
- **MQTT Broker** (e.g., mosquitto, EMQ X)
- **ESP32 sensors** publishing to WeSense MQTT topics

## Quick Start

### 1. Clone/Download the Repository

```bash
cd wesense-respiro
```

### 2. Install Dependencies

```bash
npm install
```

### 3. Configure Environment

Copy `.env.example` to `.env` and update settings:

```bash
cp .env.example .env
```

Edit `.env`:

```env
# MQTT Configuration
MQTT_BROKER_URL=mqtt://localhost:1883
MQTT_USERNAME=optional_user
MQTT_PASSWORD=optional_pass
MQTT_TOPIC_FILTER=wesense/v1/#,skytrace/esp32/env/#

# Server Configuration
PORT=3000
HOST=0.0.0.0

# Map Configuration (centre point and zoom)
MAP_CENTER_LAT=-36.848
MAP_CENTER_LNG=174.763
MAP_ZOOM_LEVEL=10
```

### 4. Start the Server

```bash
npm start
```

The server will start on `http://localhost:3000` (or your configured port).

### 5. Access the Dashboard

Open your browser and navigate to:

```
http://localhost:3000
```

## Configuration

### MQTT Settings

- **MQTT_BROKER_URL**: URL to your MQTT broker (e.g., `mqtt://192.168.1.100:1883`)
- **MQTT_USERNAME**: Username (optional)
- **MQTT_PASSWORD**: Password (optional)
- **MQTT_TOPIC_FILTER**: Topic pattern to subscribe to

### Server Settings

- **PORT**: Server port (default: 3000)
- **HOST**: Bind address (default: 0.0.0.0 for all interfaces)

### Map Settings

- **MAP_CENTER_LAT**: Default map centre latitude
- **MAP_CENTER_LNG**: Default map centre longitude
- **MAP_ZOOM_LEVEL**: Default zoom level (1-19)

## Expected MQTT Topic Format

The application supports both current and target topic structures:

### Current Format

```
skytrace/esp32/env/{REGION}/{TYPE}/{LOCATION}_{DEVICE_ID}/{sensor_type}
```

Example:

```
skytrace/esp32/env/NZ/mix/Office_301274C0E8FC/temperature_sht4x
```

### Target Format (in progress)

```
wesense/v1/{country}/{subdivision}/{device_id}/{reading_type}
```

Example:

```
wesense/v1/nz/auk/office_301274c0e8fc/temperature
```

### JSON Payload Format

Each MQTT message should contain JSON:

```json
{
  "value": 23.45,
  "timestamp": "2025-11-24T10:30:00Z",
  "device_id": "Office_301274C0E8FC",
  "latitude": -36.848461,
  "longitude": 174.763336,
  "location_source": "gps",
  "deployment_region": "NZ",
  "deployment_type": "mix",
  "calibration_status": "calibrated",
  "unit": "°C"
}
```

**Required fields:**

- `value` - The sensor reading

**Optional fields:**

- `timestamp` - ISO 8601 timestamp
- `latitude`, `longitude` - Sensor location
- `location_source` - How location was determined (gps, firmware_default, mqtt_override)
- `device_id` - Device identifier
- `unit` - Measurement unit

See `wesense-general-docs/MQTT_PAYLOAD_SCHEMA.md` for full specification.

## API Endpoints

### GET /api/sensors

Returns all detected sensors with their latest readings.

**Response:**

```json
{
  "sensors": [
    {
      "deviceId": "Office_301274C0E8FC",
      "name": "Office",
      "latitude": -36.848461,
      "longitude": 174.763336,
      "region": "NZ",
      "readings": {
        "temperature_sht4x": {
          "value": 23.45,
          "unit": "°C",
          "timestamp": "2025-11-24T10:30:00Z"
        }
      },
      "lastUpdated": "2025-11-24T10:30:00Z"
    }
  ]
}
```

### GET /api/sensors/:deviceId

Returns data for a specific sensor.

### GET /api/sensors/:deviceId/history

Returns historical readings for a sensor (last 100 readings).

### GET /api/stats

Returns network statistics.

**Response:**

```json
{
  "totalSensors": 5,
  "sensorsWithLocation": 3,
  "regions": {
    "NZ": 3,
    "AU": 2
  },
  "sensorTypes": {
    "temperature_sht4x": 5,
    "co2": 3
  },
  "lastUpdated": "2025-11-24T10:30:00Z"
}
```

## Deployment

### Home Server / Local Network

For a simple home network setup:

1. Install Node.js on your server
2. Configure MQTT broker (e.g., mosquitto)
3. Follow Quick Start section above
4. Access via `http://your-server-ip:3000`

### Docker (Optional Future Enhancement)

Coming soon: Docker container for easy deployment.

### Behind Nginx (Production)

Example Nginx configuration:

```nginx
server {
    listen 80;
    server_name sensor-map.example.com;

    location / {
        proxy_pass http://localhost:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }
}
```

## Future Enhancements

- Local ClickHouse database synced from IPFS archives
- libp2p pub/sub subscription for live data
- Time series data export (CSV, JSON, Parquet)
- Dark mode
- Multi-language support
- Advanced analytics and trends
- Region comparison tools

## Troubleshooting

### Sensors not appearing on map

1. Check MQTT broker is running: `mosquitto -v` or access broker admin panel
2. Verify ESP32 sensors are publishing to correct topics
3. Check server logs for MQTT connection errors
4. Ensure latitude/longitude are included in MQTT payloads

### MQTT Connection Failed

1. Verify `MQTT_BROKER_URL` is correct
2. Check MQTT broker is accessible from the server
3. If using authentication, verify username/password
4. Check firewall settings

### Map not displaying correctly

1. Check browser console for JavaScript errors (F12 → Console)
2. Verify OpenStreetMap tiles are loading
3. Try clearing browser cache

## Related Documentation

- Architecture: `wesense-general-docs/Decentralised_Data_Commons_Architecture.md`
- Payload Schema: `wesense-general-docs/MQTT_PAYLOAD_SCHEMA.md`
- Project Summary: `wesense-general-docs/PROJECT_SUMMARY.md`

## Support

For issues or questions, please check the main project documentation or create an issue.

## License

MIT License - see LICENSE file for details.

---

**Built for distributed environmental monitoring networks** | **Easy to install on home servers and educational institutions**
