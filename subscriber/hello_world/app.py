import base64
import boto3
import json
import os
from botocore.exceptions import ClientError
from datetime import datetime, timezone, timedelta

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    records_length = len(event['records'])
    print(f'event: {json.dumps(event)}')
    print(f'records_length: {records_length}')

    transformed_data = []

    for record in event['records']:
        result = 'Ok'
        try:
            # 1件分のデータを取得する
            payload = json.loads(base64.b64decode(record['data']))

            # シリアル番号を元にDynamoDBからETCゲートの情報を取得する
            device_item = get_item(payload['serialNumber'])

            # データを変換（追加）する
            payload['feeStationNumber'] = device_item['feeStationNumber']
            payload['feeStationName'] = device_item['feeStationName']
            payload['gateNumber'] = int(device_item['gateNumber'])
            payload['timestring'] = convert_iso_format(payload['timestamp'])

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

        # Firehoseに戻すデータを作る
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
    table_name = os.getenv('ETC_GATE_MANAGEMENT_TABLE_NAME')
    table = dynamodb.Table(table_name)

    # DynamoDBからETCゲートの情報を取得する
    res = table.get_item(Key={
        'serialNumber': serial_number
    })

    return res['Item']


def convert_iso_format(timestamp:int) -> str:
    # JSTとするので+9時間する
    tz = timezone(timedelta(hours=9))
    timestamp =  datetime.fromtimestamp(timestamp / 1000, tz)
    return timestamp.isoformat(timespec='milliseconds')
