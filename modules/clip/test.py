import run
import json

bodies = [{
    'clip_id': 'c-CVbjDmg7jWQPUq8djyyeih',
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
