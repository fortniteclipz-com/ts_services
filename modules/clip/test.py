import run
import json

bodies = [{
    'clip_id': "c-ouaq8FkAW2NKVouq32TqD6",
}, {
    'clip_id': "c-tVaLjtydbAwtypchvtHHni",
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
