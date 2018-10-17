import run
import json

bodies = [{
    'montage_id': 'm-G29f2B9HBgGP4YHt8fWEwG',
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
