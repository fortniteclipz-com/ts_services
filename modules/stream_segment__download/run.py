import ts_aws.dynamodb.stream
import ts_aws.dynamodb.stream_segment
import ts_aws.s3
import ts_aws.sqs.stream_segment__download
import ts_aws.sqs.stream__initialize
import ts_file
import ts_http
import ts_logger
import ts_model.Exception
import ts_model.Status
import ts_model.Stream

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
        receipt_handle = event['Records'][0].get('receiptHandle')

        try:
            stream = ts_aws.dynamodb.stream.get_stream(stream_id)
        except ts_model.Exception as e:
            if e.code == ts_model.Exception.STREAM__NOT_EXIST:
                logger.error("warn", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
                stream = ts_model.Stream(
                    stream_id=stream_id,
                )
                ts_aws.dynamodb.stream.save_stream(stream)

        if stream._status_initialize == ts_model.Status.NONE:
            stream._status_initialize = ts_model.Status.INITIALIZING
            ts_aws.dynamodb.stream.save_stream(stream)
            ts_aws.sqs.stream__initialize.send_message({
                'stream_id': stream.stream_id,
            })
        if stream._status_initialize != ts_model.Status.READY:
            raise ts_model.Exception(ts_model.Exception.STREAM__NOT_INITIALIZED)

        ss = ts_aws.dynamodb.stream_segment.get_stream_segment(stream_id, segment)

        if ss._status_download == ts_model.Status.READY:
            raise ts_model.Exception(ts_model.Exception.STREAM_SEGMENT__ALREADY_DOWNLOADED)

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
        if type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.STREAM_SEGMENT__ALREADY_DOWNLOADED,
        ]:
            logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            return True
        elif type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.STREAM__NOT_INITIALIZED,
        ]:
            logger.warn("warn", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            ts_aws.sqs.stream_segment__download.change_visibility(receipt_handle)
            raise Exception(e) from None
        else:
            logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            ts_aws.sqs.stream_segment__download.change_visibility(receipt_handle)
            raise Exception(e) from None
