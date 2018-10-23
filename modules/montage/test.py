import run
import json

bodies = [{
    'montage_id': 'm-3UjU4Y8SrqwEDw5TqtnkUK',
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
