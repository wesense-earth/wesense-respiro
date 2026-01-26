#!/usr/bin/env python3
"""
Meshtastic MQTT Monitor
Subscribes to Meshtastic MQTT topics and decodes/displays messages for analysis.

Usage:
    python meshtastic_mqtt_monitor.py --broker localhost --username your-username --password your-password
"""

import sys
import json
import argparse
import logging
import base64
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

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


class MeshtasticMonitor:
    """Monitor and decode Meshtastic MQTT messages"""
    
    def __init__(self, broker, port=1883, username=None, password=None, topics=None, channel_key=None):
        self.broker = broker
        self.port = port
        self.username = username
        self.password = password
        self.topics = topics or ['msh/#']
        self.client = None
        self.message_count = 0
        
        # Decode channel key from base64 (default public channel is "AQ==")
        if channel_key:
            self.channel_key = base64.b64decode(channel_key)
        else:
            self.channel_key = base64.b64decode('AQ==')  # Default public channel
        
        logger.info(f"Using channel key: {base64.b64encode(self.channel_key).decode()}")
        
    def connect(self):
        """Connect to MQTT broker"""
        self.client = mqtt.Client(
            client_id='meshtastic_monitor',
            clean_session=True
        )
        
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)
        
        logger.info(f"Connecting to MQTT broker {self.broker}:{self.port}...")
        self.client.connect(self.broker, self.port, keepalive=60)
        
    def _on_connect(self, client, userdata, flags, rc):
        """Callback when connected to MQTT broker"""
        if rc == 0:
            logger.info("Connected to MQTT broker")
            for topic in self.topics:
                logger.info(f"Subscribing to: {topic}")
                client.subscribe(topic)
        else:
            logger.error(f"Failed to connect to MQTT broker, return code: {rc}")
    
    def _on_disconnect(self, client, userdata, rc):
        """Callback when disconnected from MQTT broker"""
        if rc != 0:
            logger.warning(f"Unexpected MQTT disconnection (code: {rc}), will auto-reconnect")
        else:
            logger.info("Disconnected from MQTT broker")
    
    def _on_message(self, client, userdata, msg):
        """Callback when message received"""
        try:
            self.message_count += 1
            print("\n" + "="*80)
            print(f"MESSAGE #{self.message_count} - {datetime.now().isoformat()}")
            print(f"Topic: {msg.topic}")
            print(f"Payload size: {len(msg.payload)} bytes")
            print("-"*80)
            
            # Try to decode as ServiceEnvelope (standard MQTT format)
            try:
                self._decode_service_envelope(msg.topic, msg.payload)
            except Exception as e:
                logger.debug(f"Not a ServiceEnvelope: {e}")
                # Try other formats
                self._try_other_formats(msg.topic, msg.payload)
                
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
    
    def _decrypt_packet(self, packet_id, from_node, encrypted_payload):
        """Decrypt encrypted packet using AES-CTR"""
        try:
            # Create nonce from packet_id and from_node
            # Nonce is: packet_id (4 bytes) + from_node (4 bytes) + padding (8 bytes of zeros)
            nonce = packet_id.to_bytes(8, 'little') + from_node.to_bytes(8, 'little')
            
            # Pad key to 16 bytes (128 bits) or 32 bytes (256 bits)
            key = self.channel_key
            if len(key) == 1:  # Default channel (0x01)
                key = b'\x01' + b'\x00' * 15  # Pad to 16 bytes
            elif len(key) < 16:
                key = key + b'\x00' * (16 - len(key))
            elif len(key) < 32:
                key = key + b'\x00' * (32 - len(key))
            
            # Decrypt using AES-CTR
            cipher = Cipher(
                algorithms.AES(key),
                modes.CTR(nonce),
                backend=default_backend()
            )
            decryptor = cipher.decryptor()
            decrypted = decryptor.update(encrypted_payload) + decryptor.finalize()
            
            return decrypted
            
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            return None
    
    def _decode_service_envelope(self, topic, payload):
        """Decode ServiceEnvelope protobuf message"""
        try:
            envelope = mqtt_pb2.ServiceEnvelope()
            envelope.ParseFromString(payload)
            
            print(f"ServiceEnvelope:")
            print(f"  Channel ID: {envelope.channel_id}")
            print(f"  Gateway ID: {envelope.gateway_id}")
            
            if envelope.HasField('packet'):
                packet = envelope.packet
                logger.debug(f"Packet type: {type(packet)}")
                logger.debug(f"Packet fields: {dir(packet)}")
                
                print(f"\nMeshPacket:")
                # Access fields directly - protobuf uses 'from' not 'from_'
                from_id = getattr(packet, 'from')
                print(f"  From: 0x{from_id:08x} (!{from_id:08x})")
                print(f"  To: 0x{packet.to:08x}")
                print(f"  ID: {packet.id}")
                if packet.rx_time:
                    print(f"  Rx Time: {datetime.fromtimestamp(packet.rx_time).isoformat()}")
                print(f"  Hop Limit: {packet.hop_limit}")
                print(f"  Want Ack: {packet.want_ack}")
                print(f"  Via MQTT: {packet.via_mqtt}")
                
                # Check if encrypted and try to decrypt
                logger.debug(f"Encrypted field length: {len(packet.encrypted)}")
                logger.debug(f"Decoded field: {packet.decoded}")
                
                if packet.encrypted and len(packet.encrypted) > 0:
                    print(f"  Encrypted: {len(packet.encrypted)} bytes")
                    decrypted = self._decrypt_packet(packet.id, getattr(packet, 'from'), packet.encrypted)
                    
                    if decrypted:
                        # Parse decrypted data as Data message
                        try:
                            from meshtastic import mesh_pb2
                            data = mesh_pb2.Data()
                            data.ParseFromString(decrypted)
                            
                            portnum_name = portnums_pb2.PortNum.Name(data.portnum)
                            print(f"  Decrypted Port: {portnum_name} ({data.portnum})")
                            
                            if data.portnum == portnums_pb2.TELEMETRY_APP:
                                self._decode_telemetry(data.payload)
                            elif data.portnum == portnums_pb2.POSITION_APP:
                                self._decode_position(data.payload)
                            elif data.portnum == portnums_pb2.NODEINFO_APP:
                                self._decode_nodeinfo(data.payload)
                            elif data.portnum == portnums_pb2.TEXT_MESSAGE_APP:
                                print(f"  Text Message: {data.payload.decode('utf-8', errors='ignore')}")
                            else:
                                print(f"  Decrypted Payload ({len(data.payload)} bytes): {data.payload.hex()[:100]}...")
                        except Exception as e:
                            logger.error(f"Failed to parse decrypted data: {e}")
                            print(f"  Raw decrypted ({len(decrypted)} bytes): {decrypted.hex()[:100]}...")
                            
                # Decode payload based on port number (unencrypted)
                elif packet.decoded.portnum:
                    portnum_name = portnums_pb2.PortNum.Name(packet.decoded.portnum)
                    print(f"  Port: {portnum_name} ({packet.decoded.portnum})")
                    
                    if packet.decoded.portnum == portnums_pb2.TELEMETRY_APP:
                        self._decode_telemetry(packet.decoded.payload)
                    elif packet.decoded.portnum == portnums_pb2.POSITION_APP:
                        self._decode_position(packet.decoded.payload)
                    elif packet.decoded.portnum == portnums_pb2.NODEINFO_APP:
                        self._decode_nodeinfo(packet.decoded.payload)
                    elif packet.decoded.portnum == portnums_pb2.TEXT_MESSAGE_APP:
                        print(f"  Text Message: {packet.decoded.payload.decode('utf-8', errors='ignore')}")
                    else:
                        print(f"  Raw Payload ({len(packet.decoded.payload)} bytes): {packet.decoded.payload.hex()[:100]}...")
        except AttributeError as e:
            logger.error(f"Failed to parse ServiceEnvelope: {e}")
            logger.debug(f"Raw payload: {payload.hex()[:200]}")
            raise
    
    def _decode_telemetry(self, payload):
        """Decode telemetry data"""
        telemetry = telemetry_pb2.Telemetry()
        telemetry.ParseFromString(payload)
        
        print(f"\n  TELEMETRY:")
        print(f"    Time: {datetime.fromtimestamp(telemetry.time).isoformat() if telemetry.time else 'N/A'}")
        
        if telemetry.HasField('device_metrics'):
            dm = telemetry.device_metrics
            print(f"    Device Metrics:")
            if dm.HasField('battery_level'):
                print(f"      Battery: {dm.battery_level}%")
            if dm.HasField('voltage'):
                print(f"      Voltage: {dm.voltage}V")
            if dm.HasField('channel_utilization'):
                print(f"      Channel Util: {dm.channel_utilization:.2f}%")
            if dm.HasField('air_util_tx'):
                print(f"      Air Util TX: {dm.air_util_tx:.2f}%")
            if dm.HasField('uptime_seconds'):
                print(f"      Uptime: {dm.uptime_seconds}s")
        
        if telemetry.HasField('environment_metrics'):
            em = telemetry.environment_metrics
            print(f"    Environment Metrics:")
            if em.HasField('temperature'):
                print(f"      Temperature: {em.temperature}°C")
            if em.HasField('relative_humidity'):
                print(f"      Humidity: {em.relative_humidity}%")
            if em.HasField('barometric_pressure'):
                print(f"      Pressure: {em.barometric_pressure} hPa")
            if em.HasField('gas_resistance'):
                print(f"      Gas Resistance: {em.gas_resistance} MOhm")
            if em.HasField('iaq'):
                print(f"      IAQ: {em.iaq}")
            if em.HasField('voltage'):
                print(f"      Voltage: {em.voltage}V")
            if em.HasField('current'):
                print(f"      Current: {em.current}A")
        
        if telemetry.HasField('air_quality_metrics'):
            aq = telemetry.air_quality_metrics
            print(f"    Air Quality Metrics:")
            if aq.HasField('pm10_standard'):
                print(f"      PM1.0: {aq.pm10_standard} µg/m³")
            if aq.HasField('pm25_standard'):
                print(f"      PM2.5: {aq.pm25_standard} µg/m³")
            if aq.HasField('pm100_standard'):
                print(f"      PM10: {aq.pm100_standard} µg/m³")
            if aq.HasField('co2'):
                print(f"      CO2: {aq.co2} ppm")
            if aq.HasField('pm_voc_idx'):
                print(f"      VOC Index: {aq.pm_voc_idx}")
            if aq.HasField('pm_nox_idx'):
                print(f"      NOx Index: {aq.pm_nox_idx}")
    
    def _decode_position(self, payload):
        """Decode position data"""
        position = mesh_pb2.Position()
        position.ParseFromString(payload)
        
        print(f"\n  POSITION:")
        if position.HasField('latitude_i'):
            lat = position.latitude_i / 1e7
            lon = position.longitude_i / 1e7
            print(f"    Location: {lat:.6f}, {lon:.6f}")
        if position.HasField('altitude'):
            print(f"    Altitude: {position.altitude}m")
        if position.time:
            print(f"    Time: {datetime.fromtimestamp(position.time).isoformat()}")
        if position.HasField('sats_in_view'):
            print(f"    Satellites: {position.sats_in_view}")
        if position.HasField('PDOP'):
            print(f"    PDOP: {position.PDOP/100:.2f}")
    
    def _decode_nodeinfo(self, payload):
        """Decode node info data"""
        try:
            # NodeInfo is in mesh.proto as User message
            from meshtastic import mesh_pb2
            user = mesh_pb2.User()
            user.ParseFromString(payload)
            
            print(f"\n  NODE INFO:")
            print(f"    ID: {user.id}")
            print(f"    Long Name: {user.long_name}")
            print(f"    Short Name: {user.short_name}")
            print(f"    Hardware: {mesh_pb2.HardwareModel.Name(user.hw_model) if user.hw_model else 'Unknown'}")
        except Exception as e:
            print(f"    Could not decode: {e}")
    
    def _try_other_formats(self, topic, payload):
        """Try to decode as JSON or display raw"""
        try:
            data = json.loads(payload)
            print("JSON Payload:")
            print(json.dumps(data, indent=2))
        except:
            print(f"Raw Payload (first 200 bytes):")
            print(payload[:200].hex())
    
    def run(self):
        """Start monitoring"""
        logger.info("Starting Meshtastic MQTT monitor...")
        logger.info("Press Ctrl+C to stop")
        try:
            self.client.loop_forever()
        except KeyboardInterrupt:
            logger.info(f"\nStopping monitor (processed {self.message_count} messages)")
            self.client.disconnect()


def main():
    parser = argparse.ArgumentParser(description='Meshtastic MQTT Monitor')
    parser.add_argument('--broker', default='localhost', help='MQTT broker address')
    parser.add_argument('--port', type=int, default=1883, help='MQTT broker port')
    parser.add_argument('--username', default='', help='MQTT username')
    parser.add_argument('--password', default='', help='MQTT password')
    parser.add_argument('--topics', nargs='+', default=['msh/#'], help='MQTT topics to monitor')
    parser.add_argument('--key', default='AQ==', help='Base64 encoded channel key (default: AQ== for public channel)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    monitor = MeshtasticMonitor(
        broker=args.broker,
        port=args.port,
        username=args.username,
        password=args.password,
        topics=args.topics,
        channel_key=args.key
    )
    
    monitor.connect()
    monitor.run()


if __name__ == '__main__':
    main()
