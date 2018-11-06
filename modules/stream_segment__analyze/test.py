import run
import json

bodies = [{
    # 'stream_id': 318892183,
    # 'segment': 699,
    # 'stream_id': 310285421,
    # 'segment': 152,
    'stream_id': 308990189,
    'segment': 262,
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
