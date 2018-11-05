import run
import json

bodies = [{
    'clip_id': 'c-7SvGEmBzRcmQaSycsEJptG',
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
