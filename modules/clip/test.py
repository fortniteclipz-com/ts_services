import run
import json

datas = [{
    'clip_id': 'c-PnG4tqSq4tCLpLJysxqNF8',
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data)}]}
    run.run(event, {})
