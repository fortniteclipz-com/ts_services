import run
import json

datas = [{
    'clip_id': 'c-AeuwG6wqCs5ZQTXEhtu5A5',
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data)}]}
    run.run(event, {})
