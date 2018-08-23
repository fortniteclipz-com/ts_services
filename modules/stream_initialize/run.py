import json

logger = ts_logger.get(__name__)

def run(event, context):
    logger.info("start", event=event, context=context)
    body = json.loads(event['Records'][0]['body'])
    logger.info("body", body=body)
    stream_id = body['stream_id']


    logger.info("done", stream_segment=ss.__dict__)
    return True
