import run
import json

bodies = [{
    'montage_id': 'm-iH6a66ap7GfjFRMVJBj9tM',
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
