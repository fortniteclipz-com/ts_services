import run
import json

bodies = [{
    'clip_id': 'c-mLSTkRjtpcqePpx2zwHMqe',
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
