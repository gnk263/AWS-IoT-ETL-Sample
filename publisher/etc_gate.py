import json
import os
import sys
import time
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

IOT_CORE_ENDPOINT = 'xxxxx-ats.iot.ap-northeast-1.amazonaws.com'
PORT = 8883
TOPIC_NAME = 'etc_gate/passing/car'
QOS = 0

ROOT_CA_FILE = './AmazonRootCA1.pem'

ETC_GATE_INFO = {
    '1111ABCD': {
        'client_id': 'etc_gate_1111ABCD',
        'certificate_file': './etc_gate_1111ABCD_certificate.pem',
        'private_key_file': './etc_gate_1111ABCD_certificate.private',
    }
}

FINISH_FILE = './finish.txt'

def main(serial_number: str) -> None:
    init()

    client_id = ETC_GATE_INFO[serial_number]['client_id']
    certificate_file = ETC_GATE_INFO[serial_number]['certificate_file']
    private_key_file = ETC_GATE_INFO[serial_number]['private_key_file']

    # IoT Coreに接続する
    # https://github.com/aws/aws-iot-device-sdk-python
    # https://s3.amazonaws.com/aws-iot-device-sdk-python-docs/sphinx/html/index.html
    client = AWSIoTMQTTClient(client_id)
    client.configureEndpoint(IOT_CORE_ENDPOINT, PORT)
    client.configureCredentials(
        ROOT_CA_FILE,
        private_key_file,

    client.connect()

    while True:
        data = create_data()
        client.publish(TOPIC_NAME, json.dumps(data), QOS)
        time.sleep(1)

        if is_finish():
            break

def init() -> None:
    # もし finish.txt があるなら削除しておく
    if os.path.isfile(FINISH_FILE):
        os.remove(FINISH_FILE)

def create_data() -> dict:
    return {
        'serialNumber': '1111ABCD',
        'timestamp': int(time.time() * 1000),
        'open': True,
        'payment': True
    }


def is_finish():
    if os.path.isfile(FINISH_FILE):
        return True
    return False


if __name__ == '__main__':
    args = sys.argv
    if len(args) == 2:
        main(args[1])
