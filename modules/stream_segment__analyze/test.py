import run
import json

bodies = [{
    'stream_id': "330206924",
    'segment': 56,
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
