import run
import json

events = [{
    'detail': {
        'status': 'COMPLETE',
        'userMetadata': {
            # 'clip_id': 'c-qCuPKxsMpYPMPqjEzUJRsV',
            'montage_id': 'm-G29f2B9HBgGP4YHt8fWEwG',
        },
    }
# }, {
#     'detail': {
#         'status': 'ERROR',
#         'userMetadata': {
#             'montage_id': 'm-G29f2B9HBgGP4YHt8fWEwG'
#         }
#     }
}]

for event in events:
    run.run(event, {})
