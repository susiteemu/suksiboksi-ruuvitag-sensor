from ruuvitag_sensor.ruuvi import RuuviTagSensor
from influxdb import InfluxDBClient
import os
import logging
import click

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')

current_dir = os.path.dirname(os.path.realpath(__file__))

RUUVI_DB='ruuvi'

client = InfluxDBClient(host='localhost', port=8086)

databases = list(map(lambda c: c['name'], client.get_list_database()))
if RUUVI_DB not in databases:
    client.create_database(RUUVI_DB)

client.switch_database(RUUVI_DB)

def write_to_influxdb(mac, device_label, payload):
    logging.info(f'Received data from {mac}')
    logging.info(f'Received data: {payload}')

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
                'tag_label': device_label,
                'dataFormat': dataFormat
            },
            'fields': fields
        }
    ]
    client.write_points(json_body)


@click.command()
@click.option('--mac-address', required=True, help='Device MAC address to monitor')
@click.option('--device-label', required=True, help='Device label to add to InfluxDB measurement')
def start_measurement(mac_address, device_label):
    macs = [mac_address]
    timeout_in_sec = 5
    datas = RuuviTagSensor.get_data_for_sensors(macs, timeout_in_sec)
    for mac in datas:
        write_to_influxdb(mac, device_label, datas[mac])


if __name__ == '__main___':
    start_measurement()