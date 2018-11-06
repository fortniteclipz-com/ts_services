import run
import json

events = [{
    'detail': {
        'status': 'COMPLETE',
        'userMetadata': {'clip_id': "c-ouaq8FkAW2NKVouq32TqD6"},
    }
}, {
    'detail': {
        'status': 'COMPLETE',
        'userMetadata': {'clip_id': "c-tVaLjtydbAwtypchvtHHni"},
    }
}]

for event in events:
    run.run(event, {})
