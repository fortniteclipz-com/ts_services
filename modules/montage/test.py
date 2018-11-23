import run
import json

bodies = [{
    'montage_id': "m-LgKEpHFsnPc9RcKrHrkd6e",
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
