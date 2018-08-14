import run
import json

data = {
	'stream_id': 296105488,
	'segment': 654,
	'fresh': False,
}
event = {'Records': [{'body': json.dumps(data)}]}
run.run(event, {})

# {"Records": [{"body": "{\"stream_id\":285219394,\"segment\":0,\"fresh\":true}"}]}
