import run
import json

bodies = [{
    'stream_id': "435682279",
    'segment': 0,
}]

for b in bodies:
    event = {'Records': [{'body': json.dumps(b)}]}
    run.run(event, {})
