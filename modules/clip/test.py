import run
import json

datas = [{
    'clip_id': 'c-PLHaAtJLjuoUycwaZVG4iR',
# }, {
    # 'clip_id': 'c-VqKKWjAjdUNFg6wiMDTRni',
}]

for data in datas:
    event = {'Records': [{'body': json.dumps(data)}]}
    run.run(event, {})
