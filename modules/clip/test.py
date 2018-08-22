import run
import json

data = {
	'clip_id': 'c-DEoHtpDVaf9gMr8rcMNpu9',
}
event = {'Records': [{'body': json.dumps(data)}]}
run.run(event, {})
