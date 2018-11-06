import run
import json

bodies = [{
    'montage_id': "m-h8xGGn6hoMuAhVzpxJEQYG",
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
