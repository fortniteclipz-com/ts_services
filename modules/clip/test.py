import run
import json

bodies = [{
    'clip_id': 'c-adCMXfBURihXef2zYrZKRV',
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
