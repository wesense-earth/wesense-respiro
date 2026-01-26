#!/usr/bin/env python3
"""
Meshtastic to SkyTrace Decoder
Subscribes to Meshtastic MQTT topics, decodes protobuf messages,
correlates telemetry with position data, and publishes to SkyTrace format.

Usage:
    python meshtastic_decoder.py --broker localhost --username your-username --password your-password
"""

import sys
import os
import json
import logging
import argparse
import time
from typing import Optional, Dict, Any
from datetime import datetime
from pathlib import Path
from collections import defaultdict

import paho.mqtt.client as mqtt

# Add meshtastic protobufs to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'meshtastic' / 'protobufs'))

try:
    from meshtastic import mesh_pb2, mqtt_pb2, telemetry_pb2, portnums_pb2
except ImportError:
    print("ERROR: Cannot import Meshtastic protobufs. Please compile them first:")
    print("  cd ../meshtastic/protobufs")
    print("  protoc --python_out=. meshtastic/*.proto")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class NodeCache:
    """Cache position and node info data for nodes"""
    
    def __init__(self, max_age_seconds=86400, cache_file='meshtastic_cache.json'):  # 24 hours
        self.positions = {}  # node_id -> {lat, lon, alt, timestamp}
        self.node_names = {}  # node_id -> long_name
        self.hardware_models = {}  # node_id -> hardware_model
        self.max_age = max_age_seconds
        self.cache_file = cache_file
        self.load_cache()
    
    def update_position(self, node_id: str, latitude: float, longitude: float, altitude: int = None, timestamp: int = None):
        """Update position for a node"""
        self.positions[node_id] = {
            'latitude': latitude,
            'longitude': longitude,
            'altitude': altitude,
            'timestamp': timestamp or int(time.time()),
            'updated_at': int(time.time())
        }
        logger.info(f"Cached position for {node_id}: {latitude:.6f}, {longitude:.6f}")
        self.save_cache()
    
    def update_name(self, node_id: str, long_name: str):
        """Update node name"""
        self.node_names[node_id] = long_name
        logger.debug(f"Cached name for {node_id}: {long_name}")
        self.save_cache()
    
    def update_hardware(self, node_id: str, hw_model: int):
        """Update hardware model"""
        self.hardware_models[node_id] = hw_model
        logger.debug(f"Cached hardware for {node_id}: {hw_model}")
        self.save_cache()
    
    def get_position(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get cached position for a node if not too old"""
        if node_id not in self.positions:
            return None
        
        pos = self.positions[node_id]
        age = int(time.time()) - pos['updated_at']
        
        if age > self.max_age:
            logger.warning(f"Position for {node_id} is {age}s old, discarding")
            del self.positions[node_id]
            return None
        
        return pos
    
    def get_device_name(self, node_id: str) -> str:
        """Get formatted device name: LongName_!nodeid"""
        if node_id in self.node_names:
            # Remove spaces and special chars from long_name
            clean_name = self.node_names[node_id].replace(' ', '_')
            return f"{clean_name}_{node_id}"
        else:
            return node_id
    
    def get_hardware_model(self, node_id: str) -> Optional[str]:
        """Get hardware model name"""
        if node_id in self.hardware_models:
            try:
                return mesh_pb2.HardwareModel.Name(self.hardware_models[node_id])
            except:
                return None
        return None
    
    def cleanup_old(self):
        """Remove positions older than max_age"""
        now = int(time.time())
        to_remove = [
            node_id for node_id, pos in self.positions.items()
            if now - pos['updated_at'] > self.max_age
        ]
        for node_id in to_remove:
            del self.positions[node_id]
            logger.debug(f"Removed stale position for {node_id}")
    
    def save_cache(self):
        """Save cache to disk"""
        try:
            cache_data = {
                'positions': self.positions,
                'node_names': self.node_names,
                'hardware_models': self.hardware_models,
                'saved_at': int(time.time())
            }
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            logger.debug(f"Saved cache to {self.cache_file}")
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")
    
    def load_cache(self):
        """Load cache from disk"""
        try:
            if not os.path.exists(self.cache_file):
                logger.info("No cache file found, starting fresh")
                return
            
            with open(self.cache_file, 'r') as f:
                cache_data = json.load(f)
            
            self.positions = cache_data.get('positions', {})
            self.node_names = cache_data.get('node_names', {})
            self.hardware_models = cache_data.get('hardware_models', {})
            
            saved_at = cache_data.get('saved_at', 0)
            age = int(time.time()) - saved_at
            logger.info(f"Loaded cache from {self.cache_file} (age: {age}s, {len(self.positions)} positions, {len(self.hardware_models)} hardware models)")
            
            # Clean up old positions
            self.cleanup_old()
        except Exception as e:
            logger.error(f"Failed to load cache: {e}")


class MeshtasticDecoder:
    """Decode Meshtastic messages and publish to SkyTrace format"""
    
    def __init__(self, broker, username=None, password=None):
        self.broker = broker
        self.username = username
        self.password = password
        
        self.input_client = None
        self.output_client = None
        self.cache = NodeCache(max_age_seconds=86400)  # 24 hours
        self.message_count = 0
        
    def connect(self):
        """Connect to MQTT brokers"""
        # Input client (subscribe to Meshtastic)
        self.input_client = mqtt.Client(
            client_id='meshtastic_decoder_input',
            clean_session=True
        )
        self.input_client.on_connect = self._on_input_connect
        self.input_client.on_message = self._on_message
        
        if self.username and self.password:
            self.input_client.username_pw_set(self.username, self.password)
        
        logger.info(f"Connecting to MQTT broker {self.broker}...")
        host, port = self.broker.split(':') if ':' in self.broker else (self.broker, 1883)
        self.input_client.connect(host, int(port), keepalive=60)
        
        # Output client (publish to SkyTrace format) - same broker
        self.output_client = mqtt.Client(
            client_id='meshtastic_decoder_output',
            clean_session=True
        )
        
        if self.username and self.password:
            self.output_client.username_pw_set(self.username, self.password)
        
        logger.info(f"Connecting output client to {self.broker}...")
        self.output_client.connect(host, int(port), keepalive=60)
        self.output_client.loop_start()
    
    def _on_input_connect(self, client, userdata, flags, rc):
        """Callback when connected to input broker"""
        if rc == 0:
            logger.info("Connected to MQTT broker")
            # Subscribe to encrypted Meshtastic topics
            topic = 'msh/ANZ/2/e/#'
            logger.info(f"Subscribing to: {topic}")
            client.subscribe(topic)
        else:
            logger.error(f"Failed to connect to broker, return code: {rc}")
    
    def _on_message(self, client, userdata, msg):
        """Callback when message received"""
        try:
            self.message_count += 1
            logger.debug(f"Received message #{self.message_count} on {msg.topic}")
            
            # Decode ServiceEnvelope
            envelope = mqtt_pb2.ServiceEnvelope()
            envelope.ParseFromString(msg.payload)
            
            if not envelope.HasField('packet'):
                return
            
            packet = envelope.packet
            node_id = f"!{getattr(packet, 'from'):08x}"
            
            # Handle different packet types
            if packet.decoded.portnum == portnums_pb2.POSITION_APP:
                self._handle_position(node_id, packet.decoded.payload)
            elif packet.decoded.portnum == portnums_pb2.TELEMETRY_APP:
                self._handle_telemetry(node_id, packet.decoded.payload, packet.rx_time)
            elif packet.decoded.portnum == portnums_pb2.NODEINFO_APP:
                self._handle_nodeinfo(node_id, packet.decoded.payload)
            
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
    
    def _handle_position(self, node_id: str, payload: bytes):
        """Handle position update"""
        try:
            position = mesh_pb2.Position()
            position.ParseFromString(payload)
            
            if position.HasField('latitude_i') and position.HasField('longitude_i'):
                lat = position.latitude_i / 1e7
                lon = position.longitude_i / 1e7
                alt = position.altitude if position.HasField('altitude') else None
                timestamp = position.time if position.time else None
                
                self.cache.update_position(node_id, lat, lon, alt, timestamp)
        
        except Exception as e:
            logger.error(f"Error decoding position: {e}")
    
    def _handle_nodeinfo(self, node_id: str, payload: bytes):
        """Handle node info update"""
        try:
            user = mesh_pb2.User()
            user.ParseFromString(payload)
            
            if user.long_name:
                self.cache.update_name(node_id, user.long_name)
            
            if user.hw_model:
                self.cache.update_hardware(node_id, user.hw_model)
        
        except Exception as e:
            logger.error(f"Error decoding node info: {e}")
    
    def _handle_telemetry(self, node_id: str, payload: bytes, rx_time: int):
        """Handle telemetry update"""
        try:
            telemetry = telemetry_pb2.Telemetry()
            telemetry.ParseFromString(payload)
            
            # Process environmental metrics and air quality metrics
            has_environmental = telemetry.HasField('environment_metrics')
            has_air_quality = telemetry.HasField('air_quality_metrics')
            
            if not has_environmental and not has_air_quality:
                logger.debug(f"Skipping non-environmental telemetry from {node_id}")
                return
            
            # Get cached position and device info
            position = self.cache.get_position(node_id)
            if not position:
                logger.warning(f"No position cached for {node_id}, using null location")
                position = {
                    'latitude': None,
                    'longitude': None,
                    'altitude': None
                }
            
            # Get device info
            device_name = self.cache.get_device_name(node_id)
            hardware_model = self.cache.get_hardware_model(node_id)
            
            # Use telemetry timestamp if available, otherwise rx_time
            timestamp = telemetry.time if telemetry.time else rx_time
            
            # Process environment metrics
            if has_environmental:
                em = telemetry.environment_metrics
                
                if em.HasField('temperature'):
                    self._publish_reading(
                        device_name, 'temperature', em.temperature,
                        timestamp, position, '°C', hardware_model
                    )
                
                if em.HasField('relative_humidity'):
                    self._publish_reading(
                        device_name, 'humidity', em.relative_humidity,
                        timestamp, position, '%', hardware_model
                    )
                
                if em.HasField('barometric_pressure'):
                    self._publish_reading(
                        device_name, 'pressure', em.barometric_pressure,
                        timestamp, position, 'hPa', hardware_model
                    )
                
                if em.HasField('gas_resistance'):
                    self._publish_reading(
                        device_name, 'gas_resistance', em.gas_resistance,
                        timestamp, position, 'MOhm', hardware_model
                    )
                
                if em.HasField('iaq'):
                    self._publish_reading(
                        device_name, 'iaq', em.iaq,
                        timestamp, position, 'IAQ', hardware_model
                    )
            
            # Process air quality metrics
            if has_air_quality:
                aq = telemetry.air_quality_metrics
                
                if aq.HasField('pm10_standard'):
                    self._publish_reading(
                        device_name, 'pm1_0', aq.pm10_standard,
                        timestamp, position, 'µg/m³', hardware_model
                    )
                
                if aq.HasField('pm25_standard'):
                    self._publish_reading(
                        device_name, 'pm2_5', aq.pm25_standard,
                        timestamp, position, 'µg/m³', hardware_model
                    )
                
                if aq.HasField('pm100_standard'):
                    self._publish_reading(
                        device_name, 'pm10', aq.pm100_standard,
                        timestamp, position, 'µg/m³', hardware_model
                    )
                
                if aq.HasField('co2'):
                    self._publish_reading(
                        device_name, 'co2', aq.co2,
                        timestamp, position, 'ppm', hardware_model
                    )
                
                if aq.HasField('pm_voc_idx'):
                    self._publish_reading(
                        device_name, 'voc_index', aq.pm_voc_idx,
                        timestamp, position, 'index', hardware_model
                    )
                
                if aq.HasField('pm_nox_idx'):
                    self._publish_reading(
                        device_name, 'nox_index', aq.pm_nox_idx,
                        timestamp, position, 'index', hardware_model
                    )
                
                if aq.HasField('pm_temperature'):
                    self._publish_reading(
                        device_name, 'temperature_pm', aq.pm_temperature,
                        timestamp, position, '°C', hardware_model
                    )
                
                if aq.HasField('pm_humidity'):
                    self._publish_reading(
                        device_name, 'humidity_pm', aq.pm_humidity,
                        timestamp, position, '%', hardware_model
                    )
            
            logger.info(f"Processed environmental telemetry from {node_id}")
            
        except Exception as e:
            logger.error(f"Error decoding telemetry: {e}", exc_info=True)
    
    def _publish_reading(self, device_name: str, sensor_type: str, value: float,
                        timestamp: int, position: Dict, unit: str, hardware_model: str = None):
        """Publish a sensor reading in SkyTrace format"""
        # Format: skytrace/decoded/env/{DEVICE}/{sensor_type}
        topic = f"skytrace/decoded/env/{device_name}/{sensor_type}"
        
        # Create payload in SkyTrace format
        payload = {
            'value': value,
            'timestamp': timestamp,
            'device_id': device_name,
            'latitude': position.get('latitude'),
            'longitude': position.get('longitude'),
            'altitude': position.get('altitude'),
            'location_source': 'gps',
            'sensor_model': 'MESHTASTIC',
            'board_model': hardware_model,
            'reading_type': sensor_type,
            'unit': unit,
            'deployment_region': 'ANZ',
            'deployment_type': 'PORTABLE',
            'transport_type': 'LORA'
        }
        
        # Publish
        payload_json = json.dumps(payload)
        result = self.output_client.publish(topic, payload_json, qos=0, retain=False)
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            logger.info(f"Published {sensor_type}={value}{unit} for {node_id} to {topic}")
        else:
            logger.error(f"Failed to publish to {topic}, rc={result.rc}")
    
    def run(self):
        """Start decoder loop"""
        logger.info("Starting Meshtastic decoder...")
        logger.info("Press Ctrl+C to stop")
        
        try:
            self.input_client.loop_forever()
        except KeyboardInterrupt:
            logger.info(f"\nStopping decoder (processed {self.message_count} messages)")
            self.cleanup()
    
    def cleanup(self):
        """Clean up connections"""
        logger.info("Saving cache before exit...")
        self.cache.save_cache()
        if self.input_client:
            self.input_client.disconnect()
        if self.output_client:
            self.output_client.loop_stop()
            self.output_client.disconnect()


def main():
    parser = argparse.ArgumentParser(description='Meshtastic to SkyTrace Decoder')
    parser.add_argument('--broker', default='localhost:1883',
                       help='MQTT broker address:port')
    parser.add_argument('--username', default='', help='MQTT username')
    parser.add_argument('--password', default='', help='MQTT password')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    decoder = MeshtasticDecoder(
        broker=args.broker,
        username=args.username,
        password=args.password
    )
    
    decoder.connect()
    decoder.run()


if __name__ == '__main__':
    main()
