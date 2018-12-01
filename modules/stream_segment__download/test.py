import run
import json

bodies = [{
    'stream_id': "335886831",
    'segment': 47,
}, {
    'stream_id': "335886831",
    'segment': 60,
}, {
    'stream_id': "335886831",
    'segment': 61,
}]

for b in bodies:
    event = {'Records': [{'body': json.dumps(b)}]}
    run.run(event, {})
