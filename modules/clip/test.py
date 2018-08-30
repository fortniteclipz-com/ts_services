import run
import json

bodies = [{
    'clip_id': 'c-eaDQFX9jr8tVD2Ju9GfcEh',
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
