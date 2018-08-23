import run
import json

datas = [{
    'stream_id': 204521537,
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data)}]}
    run.run(event, {})
