import run
import json
from concurrent.futures import ThreadPoolExecutor

#bodies = [{'stream_id': "466862352", 'segment': 275}]
bodies = [{'stream_id': "467937909", 'segment': i} for i in range(0, 2535)]

events = []
for b in bodies:
    events.append({'Records': [{'body': json.dumps(b)}]})

with ThreadPoolExecutor(24) as executor:
    for _ in executor.map(run.run, events):
        pass
