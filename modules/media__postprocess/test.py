import run
import json

events = [{
    'detail': {
        'status': 'COMPLETE',
        'userMetadata': {
            'clip_id': 'c-7khkE2rBch69oDtrAZJSXd'
        },
    }
# }, {
#     'detail': {
#         'status': 'ERROR',
#         'userMetadata': {
#             'montage_id': 'm-TqycrB6MvyKzwpXmBAFE5E'
#         }
#     }
}]

for event in events:
    run.run(event, {})
