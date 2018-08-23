import run
import json

datas = [{
    'clip_id': 'c-gUWUUHPHc9wWm4V62RkAv7',
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data)}]}
    run.run(event, {})
