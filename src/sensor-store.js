class SensorStore {
    constructor() {
        this.sensors = new Map(); // deviceId -> sensor object
        this.history = new Map(); // deviceId -> array of historical readings
        this.maxHistorySize = 100; // Keep last 100 readings per sensor
    }

    update(deviceId, data) {
        // Get or create sensor entry
        if (!this.sensors.has(deviceId)) {
            this.sensors.set(deviceId, {
                deviceId,
                name: data.name || this._extractNameFromDeviceId(deviceId),
                readings: {},
                lastUpdated: null,
                firstSeen: new Date().toISOString(),
            });
            this.history.set(deviceId, []);
        }

        const sensor = this.sensors.get(deviceId);

        // Update name if provided in data (Meshtastic NODEINFO)
        if (data.name && data.name !== sensor.name) {
            sensor.name = data.name;
        }
        
        // Update latitude and longitude if present
        if (data.latitude !== undefined) {
            sensor.latitude = data.latitude;
        }
        if (data.longitude !== undefined) {
            sensor.longitude = data.longitude;
        }
        if (data.location_source !== undefined) {
            sensor.location_source = data.location_source;
        }

        // Store sensor-specific reading
        const sensorType = data.sensorType || 'unknown';
        console.log(`DEBUG sensor-store: Storing reading for ${deviceId}, data keys:`, Object.keys(data));
        sensor.readings[sensorType] = {
            value: data.value,
            timestamp: data.timestamp || new Date().toISOString(),
            unit: this._getUnitForSensorType(sensorType),
            raw: data,
        };
        console.log(`DEBUG sensor-store: Stored raw keys:`, Object.keys(sensor.readings[sensorType].raw));

        sensor.lastUpdated = new Date().toISOString();
        sensor.region = data.region || 'unknown';

        // Add to history
        const historyEntry = {
            timestamp: new Date().toISOString(),
            sensorType,
            value: data.value,
        };

        const sensorHistory = this.history.get(deviceId);
        sensorHistory.push(historyEntry);
        
        // Trim history if it exceeds max size
        if (sensorHistory.length > this.maxHistorySize) {
            sensorHistory.shift();
        }
    }

    get(deviceId) {
        return this.sensors.get(deviceId) || null;
    }

    getAll() {
        return Array.from(this.sensors.values());
    }

    getHistory(deviceId) {
        return this.history.get(deviceId) || [];
    }

    getStats() {
        const sensors = Array.from(this.sensors.values());
        const regions = new Map();
        const sensorTypes = new Map();

        sensors.forEach(sensor => {
            // Count by region
            if (sensor.region) {
                regions.set(sensor.region, (regions.get(sensor.region) || 0) + 1);
            }

            // Count by sensor type
            Object.keys(sensor.readings).forEach(type => {
                sensorTypes.set(type, (sensorTypes.get(type) || 0) + 1);
            });
        });

        return {
            totalSensors: sensors.length,
            sensorsWithLocation: sensors.filter(s => s.latitude && s.longitude).length,
            regions: Object.fromEntries(regions),
            sensorTypes: Object.fromEntries(sensorTypes),
            lastUpdated: new Date().toISOString(),
        };
    }

    _extractNameFromDeviceId(deviceId) {
        // Extract location name from device ID
        // Format: LOCATION_DEVICE_ID or just DEVICE_ID
        if (deviceId.includes('_')) {
            return deviceId.split('_').slice(0, -1).join('_');
        }
        return deviceId;
    }

    _getUnitForSensorType(sensorType) {
        const units = {
            'temperature': '°C',
            'humidity': '%',
            'co2': 'ppm',
            'pm1_0': 'µg/m³',
            'pm2_5': 'µg/m³',
            'pm10': 'µg/m³',
            'particles_0_3um': '#/cm³',
            'particles_0_5um': '#/cm³',
            'particles_1_0um': '#/cm³',
            'particles_2_5um': '#/cm³',
            'particles_5_0um': '#/cm³',
            'particles_10um': '#/cm³',
            'voc_raw': 'raw',
            'voc_index': 'index',
            'nox_raw': 'raw',
            'nox_index': 'index',
            'pressure': 'hPa',
            'altitude': 'm',
            'dc_bus_voltage': 'V',
            'dc_current': 'A',
            'dc_power': 'W',
        };

        // Exact match first
        if (units[sensorType]) {
            return units[sensorType];
        }

        // Partial match (case-insensitive)
        const lower = sensorType.toLowerCase();
        for (const [key, unit] of Object.entries(units)) {
            if (lower.includes(key)) {
                return unit;
            }
        }

        return 'unknown';
    }

    clear() {
        this.sensors.clear();
        this.history.clear();
    }
}

module.exports = SensorStore;
