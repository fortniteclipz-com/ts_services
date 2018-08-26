import run
import json

datas = [{
    'clip_id': 'c-5z7PZDQjUMrRLeenxJobcM',
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data)}]}
    run.run(event, {})
