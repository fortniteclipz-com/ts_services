import run
import json

bodies = [{
    'montage_id': "m-YqVYZZQJe77zv43zYFsyZi",
}]

for body in bodies:
    event = {'Records': [{'body': json.dumps(body)}]}
    run.run(event, {})
