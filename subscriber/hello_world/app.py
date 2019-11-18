import base64
import boto3
import json
import os
import time
from botocore.exceptions import ClientError
from datetime import datetime

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    records_length = len(event['records'])
    print(f'event: {json.dumps(event)}')
    print(f'records_length: {records_length}')

    transformed_data = []

    for record in event['records']:
        result = 'Ok'
        try:
            payload = json.loads(base64.b64decode(record['data']))
            payload['feeStationName'] = 'てすと'

            device_item = get_item(payload['serialNumber'])

            payload['feeStationNumber'] = device_item['feeStationNumber']
            payload['feeStationName'] = device_item['feeStationName']
            payload['gateNumber'] = int(device_item['gateNumber'])

            data = json.dumps(payload) + '\n'
            print(f'transformed data: {data}')
        except ClientError as e:
            print('DynamoDB ClientError:')
            print(e.response['Error']['Message'])
            print(f'payload: {payload}')
            result = 'Ng'
        except Exception as e:
            print(f'Transform failed. {e}')
            print(f'payload: {payload}')
            result = 'Ng'

        transformed_data.append({
            'recordId': record['recordId'],
            'result': result,
            'data': base64.b64encode(data.encode('utf-8')).decode('utf-8')
        })

    print('finish transform.')

    return {
        'records': transformed_data
    }


def get_item(serial_number:int) -> dict:
    table = dynamodb.Table(os.getenv('ETC_GATE_MANAGEMENT_TABLE_NAME'))

    res = table.get_item(Key={
        'serialNumber': serial_number
    })

    return res['Item']