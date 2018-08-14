import run
import json

data = {
	'clip_id': 'c-tmxMj4rHSBtuJrhB7xTGr9',
}
event = {'Records': [{'body': json.dumps(data)}]}
run.run(event, {})
