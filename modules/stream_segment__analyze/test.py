import run
import json

bodies = [{
    'stream_id': "335886831",
    'segment': 2,
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
