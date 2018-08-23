import run
import json

datas = [{
    'clip_id': 'c-zFbn2JyyKtii67EeFpa2qg',
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data)}]}
    run.run(event, {})
