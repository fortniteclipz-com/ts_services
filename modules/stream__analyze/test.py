import run
import json

bodies = [{
    'stream_id': 292361387,
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
