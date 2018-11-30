import run
import json

bodies = [{
    'clip_id': "c-CmP4qFSHweuNwBm2Wfo7FY",
}]

for b in bodies:
    event = {'Records': [{'body': json.dumps(b)}]}
    run.run(event, {})
