import run
import json

bodies = [{
    'stream_id': "308990189",
    'segment': 193,
}, {
    'stream_id': "308990189",
    'segment': 194,
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
