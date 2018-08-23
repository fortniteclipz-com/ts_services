import run
import json

datas = [{
    'clip_id': 'c-idaUcTpRYRFxff4EzSx6Ej',
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data)}]}
    run.run(event, {})
