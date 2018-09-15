import run
import json

bodies = [{
    'stream_id': 308844220,
    'segment': 444,
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
