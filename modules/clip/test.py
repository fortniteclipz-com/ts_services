import run
import json

bodies = [{
    'clip_id': "c-CmP4qFSHweuNwBm2Wfo7FY",
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
