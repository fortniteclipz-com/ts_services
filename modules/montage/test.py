import run
import json

bodies = [{
    'montage_id': 'm-eaDQFX9jr8tVD2Ju9GfcEh',
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
