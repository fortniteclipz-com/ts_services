import run
import json

bodies = [{
    'stream_id': "328002305",
    'segment': 127,
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
