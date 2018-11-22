import run
import json

events = [{
    'detail': {
        'status': 'COMPLETE',
        'userMetadata': {'clip_id': "c-fqViqphZdTWmok88riJhQB"},
    }
}, {
    'detail': {
        'status': 'COMPLETE',
        'userMetadata': {'clip_id': "c-Kzgy87gomdhBdULmYj7kGh"},
    }
}]

for event in events:
    run.run(event, {})
