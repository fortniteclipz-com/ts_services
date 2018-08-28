import run
import json

bodies = [{
    'stream_id': 285219394,
    'segment': 0,
}, {
    'stream_id': 285219394,
    'segment': 1,
}, {
    'stream_id': 285219394,
    'segment': 2,
}, {
    'stream_id': 285219394,
    'segment': 4,
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
