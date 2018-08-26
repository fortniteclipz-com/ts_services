import run
import json

datas = [{
    'clip_id': 'c-oJ3fTJtb2FirpDPDkvP6Ak',
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data)}]}
    run.run(event, {})
