import run
import json

bodies = [{
    'stream_id': "335886831",
}]

for b in bodies:
    event = {'Records': [{'body': json.dumps(b)}]}
    run.run(event, {})
