import run
import json

datas = [{
    'stream_id': 285219394,
    'segment': 0,
}, {
    'stream_id': 285219394,
    'segment': 1,
}, {
    'stream_id': 285219394,
    'segment': 2,
}, {
    'stream_id': 285219394,
    'segment': 4,
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data), 'receiptHandle': None}]}
    run.run(event, {})
