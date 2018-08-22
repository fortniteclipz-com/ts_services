import run
import json

datas = [{
    'stream_id': 285219394,
    'segment': 12,
    'fresh': True,
}, {
    'stream_id': 285219394,
    'segment': 13,
    'fresh': False,
}, {
    'stream_id': 285219394,
    'segment': 14,
    'fresh': False,
}]


for data in datas:
    event = {'Records': [{'body': json.dumps(data)}]}
    run.run(event, {})

# {"Records": [{"body": "{\"stream_id\":285219394,\"segment\":0,\"fresh\":true}"}]}
