import run
import json

datas = [{
    'clip_id': 'c-Ap3NFnB4dvPri3yFuM2eVh',
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data), 'receiptHandle': None}]}
    run.run(event, {})
