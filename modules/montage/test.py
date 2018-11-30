import run
import json

bodies = [{
    'montage_id': "m-Vh7mbuhKmEpkGqwqxrB2eC",
}]

for b in bodies:
    event = {'Records': [{'body': json.dumps(b)}]}
    run.run(event, {})
