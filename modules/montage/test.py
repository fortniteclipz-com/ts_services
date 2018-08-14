import run
import json

data = {
    'montage_id': 'm-WAEi7J9GK4dwYZywUe7XM5',
}
event = {'Records': [{'body': json.dumps(data)}]}
run.run(event, {})
