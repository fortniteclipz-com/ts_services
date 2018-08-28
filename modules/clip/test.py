import run
import json

bodies = [{
    'clip_id': 'c-Lg8SpMbfuH8LcvEUeCYbXi',
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
