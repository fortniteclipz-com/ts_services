import run
import json

bodies = [{
    'montage_id': 'm-pJ4dn7pehsyPVtYr7DLB26',
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
