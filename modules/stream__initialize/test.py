import run
import json

bodies = [{
    'stream_id': 285219394,
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
