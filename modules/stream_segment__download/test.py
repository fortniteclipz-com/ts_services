import run
import json

bodies = [{
    'stream_id': "335886831",
    'segment': 60,
}, {
    'stream_id': "335886831",
    'segment': 61,
}, {
    'stream_id': "335886831",
    'segment': 3,
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
