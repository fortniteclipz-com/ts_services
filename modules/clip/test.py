import run
import json

bodies = [{
    'clip_id': "c-hpdobSj7FL3qPccXYYBEQg",
}, {
    'clip_id': "c-daMaDcjzYVbWLkP3KsFzLT",
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
