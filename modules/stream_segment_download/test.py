import run
import json

datas = [{
    'stream_id': 285219394,
    'segment': 8,
}, {
    'stream_id': 285219394,
    'segment': 9,
}, {
    'stream_id': 285219394,
    'segment': 10,
}, {
    'stream_id': 285219394,
    'segment': 11,
}, {
    'stream_id': 285219394,
    'segment': 12,
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data), 'receiptHandle': None}]}
    run.run(event, {})
