import run
import json

bodies = [{
    'clip_id': 'c-b2qtQ6BK8FTdAFqb8vNwDY',
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
