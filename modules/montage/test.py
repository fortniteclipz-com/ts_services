import run
import json

bodies = [{
    'montage_id': "m-Vh7mbuhKmEpkGqwqxrB2eC",
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
