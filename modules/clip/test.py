import run
import json

bodies = [{
    'clip_id': "c-h8tdbMiRGJruv6kjQKYaDj",
}, {
    'clip_id': "c-pZQu8sfw2L4JnUMUCngPrk",
}]

for b in bodies:
    event = {'Records': [{'body': json.dumps(b)}]}
    run.run(event, {})
