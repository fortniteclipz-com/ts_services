import run
import json

bodies = [{
    # 'stream_id': 309042429,
    # 'segment': 25,
# }, {
    'stream_id': 309042429,
    'segment': 26,
# }, {
#     'stream_id': 309042429,
#     'segment': 27,
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
