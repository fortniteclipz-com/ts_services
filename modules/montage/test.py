import run
import json

bodies = [{
    'montage_id': "m-A3NSgMZqRZFJDrXVjaEWAB",
}]

for b in bodies:
    event = {'Records': [{'body': json.dumps(b)}]}
    run.run(event, {})
