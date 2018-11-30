import run
import json

bodies = [{
    'stream_id': "328002305",
    'segment': 127,
}]

for b in bodies:
    event = {'Records': [{'body': json.dumps(b)}]}
    run.run(event, {})
