import run
import json

data = {
	'clip_id': 'c-HyZyL7cQtsDyjcBrdrRkKU',
}
event = {'Records': [{'body': json.dumps(data)}]}
run.run(event, {})
