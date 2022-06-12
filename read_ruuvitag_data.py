from ruuvitag_sensor.ruuvi import RuuviTagSensor
from influxdb import InfluxDBClient
import yaml
import os
import logging

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')

current_dir = os.path.dirname(os.path.realpath(__file__))

RUUVI_DB='ruuvi'

client = InfluxDBClient(host='localhost', port=8086)

databases = list(map(lambda c: c['name'], client.get_list_database()))
if RUUVI_DB not in databases:
    client.create_database(RUUVI_DB)

client.switch_database(RUUVI_DB)

# config.yaml has contents such as 
# 
# AA_2C_6A_1E_59_3D: "Balcony"
# CC_2C_6A_1E_59_3D: "Living room"
# 
# The key is the MAC address for Ruuvitag converted to yaml suitable format and the value is a label describing the measurement sensor
configuration_yaml = f'{current_dir}/config.yaml'
configuration = {}
with open(configuration_yaml) as file:
    configuration = yaml.load(file, Loader=yaml.FullLoader)

def write_to_influxdb(mac, payload):
    logging.info(f'Received data from {mac}')
    logging.info(f'Received data: {payload}')

    mac_yaml_suitable = mac.replace(':', '_')
    tag_label = configuration[mac_yaml_suitable] if (configuration is not None and mac_yaml_suitable in configuration) else mac

    dataFormat = payload['data_format'] if ('data_format' in payload) else None
    fields = {}
    fields['temperature']               = payload['temperature'] if ('temperature' in payload) else None
    fields['humidity']                  = payload['humidity'] if ('humidity' in payload) else None
    fields['pressure']                  = payload['pressure'] if ('pressure' in payload) else None
    fields['accelerationX']             = payload['acceleration_x'] if ('acceleration_x' in payload) else None
    fields['accelerationY']             = payload['acceleration_y'] if ('acceleration_y' in payload) else None
    fields['accelerationZ']             = payload['acceleration_z'] if ('acceleration_z' in payload) else None
    fields['batteryVoltage']            = payload['battery']/1000.0 if ('battery' in payload) else None
    fields['txPower']                   = payload['tx_power'] if ('tx_power' in payload) else None
    fields['movementCounter']           = payload['movement_counter'] if ('movement_counter' in payload) else None
    fields['measurementSequenceNumber'] = payload['measurement_sequence_number'] if ('measurement_sequence_number' in payload) else None
    fields['tagID']                     = payload['tagID'] if ('tagID' in payload) else None
    fields['rssi']                      = payload['rssi'] if ('rssi' in payload) else None
    json_body = [
        {
            'measurement': 'ruuvi_measurements',
            'tags': {
                'mac': mac,
                'tag_label': tag_label,
                'dataFormat': dataFormat
            },
            'fields': fields
        }
    ]
    client.write_points(json_body)

# Get keys from configuration and convert them to MAC addresses
macs = list(map(lambda k: k.replace('_', ':'), configuration.keys()))
timeout_in_sec = 10
datas = RuuviTagSensor.get_data_for_sensors(macs, timeout_in_sec)
for mac in datas:
    write_to_influxdb(mac, datas[mac])