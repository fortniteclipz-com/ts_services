import run
import json

datas = [{
    'stream_id': 285219394,
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data)}]}
    run.run(event, {})
