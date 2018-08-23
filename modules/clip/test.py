import run
import json

datas = [{
    'clip_id': 'c-fXYeTUJesGXdot7SC3JgRM',
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data)}]}
    run.run(event, {})
