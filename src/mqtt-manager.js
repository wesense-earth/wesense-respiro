const mqtt = require('mqtt');

class MqttManager {
    constructor(sensorStore) {
        this.sensorStore = sensorStore;
        this.client = null;
        this.connected = false;
    }

    connect() {
        const brokerUrl = process.env.MQTT_BROKER_URL || 'mqtt://localhost:1883';
        const options = {};

        if (process.env.MQTT_USERNAME && process.env.MQTT_PASSWORD) {
            options.username = process.env.MQTT_USERNAME;
            options.password = process.env.MQTT_PASSWORD;
        }

        this.client = mqtt.connect(brokerUrl, {
            ...options,
            reconnectPeriod: 5000,
            clean: true,
        });

        this.client.on('connect', () => {
            this.connected = true;
            console.log('Connected to MQTT broker');
            this.subscribe();
        });

        this.client.on('message', (topic, payload) => {
            this.handleMessage(topic, payload);
        });

        this.client.on('error', (error) => {
            console.error('MQTT Error:', error.message);
        });

        this.client.on('disconnect', () => {
            this.connected = false;
            console.log('Disconnected from MQTT broker');
        });
    }

    subscribe() {
        const topicFilter = process.env.MQTT_TOPIC_FILTER || 'wesense/decoded/#';

        console.log(`Subscribing to topic filter: ${topicFilter}`);
        this.client.subscribe(topicFilter, (err) => {
            if (err) {
                console.error(`Failed to subscribe to ${topicFilter}:`, err);
            } else {
                console.log(`Subscribed to ${topicFilter}`);
            }
        });
    }

    handleMessage(topic, payload) {
        try {
            console.log(`DEBUG: Received message on ${topic}`);
            const data = JSON.parse(payload.toString());
            console.log(`DEBUG: Parsed payload:`, data);

            // Extract device ID and sensor type from topic
            const topicParts = topic.split('/');

            let deviceLocationId, sensorType, country, subdivision, dataSource;

            if (topicParts[0] === 'wesense' && topicParts[1] === 'decoded') {
                // Format: wesense/decoded/{source}/{country}/{subdivision}/{device_id}
                // Example: wesense/decoded/meshtastic-public/us/ohio/meshtastic_abc123
                if (topicParts.length < 6) {
                    console.warn(`Invalid WeSense topic format: ${topic}`);
                    return;
                }
                dataSource = topicParts[2];         // "meshtastic-public", "meshtastic-community", "wesense"
                country = topicParts[3];            // "us"
                subdivision = topicParts[4];        // "ohio"
                deviceLocationId = topicParts[5];   // "meshtastic_abc123"
                sensorType = data.reading_type;     // from payload

                console.log(`Parsed WeSense topic: source=${dataSource}, country=${country}, subdivision=${subdivision}, device=${deviceLocationId}, sensor=${sensorType}`);
            }
            else {
                console.warn(`Unrecognised topic format: ${topic}`);
                return;
            }

            // Parse timestamp - handle both Unix timestamp (number) and ISO string
            let timestamp;
            if (typeof data.timestamp === 'number') {
                timestamp = new Date(data.timestamp * 1000).toISOString();
            } else if (typeof data.timestamp === 'string') {
                timestamp = data.timestamp;
            } else {
                timestamp = new Date().toISOString();
            }

            // Update sensor data with all available fields
            this.sensorStore.update(deviceLocationId, {
                country: country || data.country,
                subdivision: subdivision || data.subdivision,
                data_source: dataSource || data.data_source,
                sensorType,
                topic,
                value: data.value,
                timestamp,
                latitude: data.latitude,
                longitude: data.longitude,
                location_source: data.location_source,
                // node_name contains combined PREFIX_LOCATION, deployment_location is just DEVICE_LOCATION
                name: data.node_name || data.deployment_location,
                device_id: data.device_id || deviceLocationId,
                board_model: data.board_model,
                firmware_version: data.firmware_version,
                sensor_model: data.sensor_model,
                reading_type: sensorType,
                unit: data.unit,
                calibration_status: data.calibration_status,
                deployment_type: data.deployment_type,
                receivedAt: new Date().toISOString(),
                // Preserve complete MQTT payload for frontend access
                rawMqttPayload: data,
            });

        } catch (error) {
            console.error(`Error processing message from ${topic}:`, error.message);
            console.error(`Topic: ${topic}, Payload: ${payload.toString()}`);
        }
    }

    disconnect() {
        if (this.client) {
            this.client.end(true, () => {
                console.log('MQTT client disconnected');
            });
        }
    }

    isConnected() {
        return this.connected;
    }
}

module.exports = MqttManager;
