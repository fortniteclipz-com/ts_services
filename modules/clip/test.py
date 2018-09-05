import run
import json

bodies = [{
    'clip_id': 'c-jd8fbHXJyTmGAFz8SepSDm',
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
