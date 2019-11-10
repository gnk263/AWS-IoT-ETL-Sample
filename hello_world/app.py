import base64
import json
import time
from datetime import datetime


def lambda_handler(event, context):
    records_length = len(event['records'])
    print(json.dumps(event))
    print(f'records_length: {records_length}')

    transformed_data = []

    for record in event['records']:
        result = 'Ok'
        try:
            payload = json.loads(base64.b64decode(record['data']))
            payload['feeStationName'] = 'てすと'
        except Exception as e:
            print('Convert failed.')
            print(e)
            result = 'Ng'

        print('transformed data:')
        print(json.dumps(payload))

        data = json.dumps(payload) + '\n'

        transformed_data.append({
            'recordId': record['recordId'],
            'result': result,
            'data': base64.b64encode(data.encode('utf-8'))
        })

    print('finish transform.')

    return {
        'records': transformed_data
    }
