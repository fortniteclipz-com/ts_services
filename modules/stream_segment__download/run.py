import ts_aws.dynamodb.stream_segment
import ts_aws.s3
import ts_aws.sqs.stream_segment_download
import ts_file
import ts_http
import ts_logger
import ts_model.Exception
import ts_model.Status

import json
import traceback

logger = ts_logger.get(__name__)

def run(event, context):
    try:
        logger.info("start", event=event, context=context)
        body = json.loads(event['Records'][0]['body'])
        logger.info("body", body=body)
        stream_id = body['stream_id']
        segment = body['segment']
        receipt_handle = event['Records'][0].get('receiptHandle', None)

        # check stream_segment
        ss = ts_aws.dynamodb.stream_segment.get_stream_segment(stream_id, segment)
        if ss._status_download == ts_model.Status.READY:
            raise ts_model.Exception(ts_model.Exception.STREAM_SEGMENT__ALREADY_PROCESSED)
        if ss._status_download == ts_model.Status.NONE:
            raise ts_model.Exception(ts_model.Exception.STREAM_SEGMENT__NOT_INITIALIZING)

        media_filename = f"/tmp/{ss.padded}.ts"
        ts_http.download_file(ss.media_url, media_filename)

        media_key = f"streams/{stream_id}/{ss.padded}.ts"
        ts_aws.s3.upload_file(media_filename, media_key)

        ss.media_key = media_key
        ss._status_download = ts_model.Status.READY
        ts_aws.dynamodb.stream_segment.save_stream_segment(ss)

        ts_file.delete(media_filename)

        logger.info("success")
        return True

    except Exception as e:
        logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
        if type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.STREAM_SEGMENT__NOT_EXIST,
            ts_model.Exception.STREAM_SEGMENT__ALREADY_PROCESSED,
            ts_model.Exception.STREAM_SEGMENT__NOT_INITIALIZING,
        ]:
            return True
        else:
            ts_aws.sqs.stream_segment_download.change_visibility(receipt_handle)
            raise Exception(e) from None

