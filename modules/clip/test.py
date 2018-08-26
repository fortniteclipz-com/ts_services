import run
import json

datas = [{
    'clip_id': 'c-CVbjDmg7jWQPUq8djyyeih',
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data)}]}
    run.run(event, {})
