import run
import json

datas = [{
    'clip_id': 'c-e4FGh6oRuCLPsBvnidWmaL',
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data)}]}
    run.run(event, {})
