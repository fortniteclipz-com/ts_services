import run
import json

bodies = [{
    'stream_id': 311038404,
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
