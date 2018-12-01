import run
import json

bodies = [{
    'clip_id': "c-ZxvqDVLYYYnWx2h6j5cEN9",
}]

for b in bodies:
    event = {'Records': [{'body': json.dumps(b)}]}
    run.run(event, {})
