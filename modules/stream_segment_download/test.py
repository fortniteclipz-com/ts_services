import run
import json

datas = [{
    'stream_id': 204521537,
    'segment': 1299,
    'fresh': True,
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data), 'receiptHandle': None}]}
    run.run(event, {})
