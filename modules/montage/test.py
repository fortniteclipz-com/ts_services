import run
import json

bodies = [{
    'montage_id': "m-khpggLg8mrf2AXBpMerLkF",
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
