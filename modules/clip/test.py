import run
import json

datas = [{
    'clip_id': 'c-gJdUyL7eTVUowwxKoPKQE9',
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data), 'receiptHandle': None}]}
    run.run(event, {})
