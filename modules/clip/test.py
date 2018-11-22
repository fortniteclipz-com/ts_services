import run
import json

bodies = [{
    'clip_id': "c-Kzgy87gomdhBdULmYj7kGh",
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
