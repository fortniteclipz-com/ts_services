import run
import json

bodies = [{
    'montage_id': "m-w5DvaXq9CTowK6HtouGZnY",
}]

for b in bodies:
    event = {'Records': [{'body': json.dumps(b)}]}
    run.run(event, {})
