import run
import json

bodies = [{
    'stream_id': "328863396",
    'segment': 79,
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
