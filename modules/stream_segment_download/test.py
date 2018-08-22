import run
import json

datas = [{
    'stream_id': 285219394,
    'segment': 0,
    'fresh': True,
}, {
    'stream_id': 285219394,
    'segment': 1,
    'fresh': False,
}, {
    'stream_id': 285219394,
    'segment': 2,
    'fresh': False,
}, {
    'stream_id': 285219394,
    'segment': 3,
    'fresh': False,
}, {
    'stream_id': 285219394,
    'segment': 8,
    'fresh': True,
}, {
    'stream_id': 285219394,
    'segment': 9,
    'fresh': False,
}, {
    'stream_id': 285219394,
    'segment': 10,
    'fresh': False,
}, {
    'stream_id': 285219394,
    'segment': 11,
    'fresh': False,
}, {
    'stream_id': 285219394,
    'segment': 12,
    'fresh': False,
}]


for data in datas:
    event = {'Records': [{'body': json.dumps(data)}]}
    run.run(event, {})

# {"Records": [{"body": "{\"stream_id\":285219394,\"segment\":0,\"fresh\":true}"}]}
