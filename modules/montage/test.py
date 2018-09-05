import run
import json

bodies = [{
    'montage_id': 'm-7BRoELHvLryZFLSqqgwRuC',
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
