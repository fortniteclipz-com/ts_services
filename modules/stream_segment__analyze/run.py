import ts_aws.dynamodb.stream
import ts_aws.dynamodb.stream_event
import ts_aws.dynamodb.stream_segment
import ts_aws.s3
import ts_aws.sqs.stream__initialize
import ts_aws.sqs.stream_segment__analyze
import ts_aws.sqs.stream_segment__download
import ts_file
import ts_logger
import ts_media
import ts_model.Exception
import ts_model.Status
import ts_model.Stream
import ts_model.StreamEvent

import glob
import json
import os
import random
import shortuuid
import traceback

logger = ts_logger.get(__name__)
ts_media.init_ff_libs()

def run(event, context):
    try:
        logger.info("start", event=event, context=context)
        body = json.loads(event['Records'][0]['body'])
        logger.info("body", body=body)
        stream_id = body['stream_id']
        segment = body['segment']
        receipt_handle = event['Records'][0].get('receiptHandle')

        # get/initialize stream and check if stream is ready
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

        # get stream_segment
        ss = ts_aws.dynamodb.stream_segment.get_stream_segment(stream_id, segment)

        if ss._status_analyze == ts_model.Status.READY:
            raise ts_model.Exception(ts_model.Exception.STREAM_SEGMENT__ALREADY_ANALYZED)

        if ss._status_download == ts_model.Status.NONE:
            ss._status_download = ts_model.Status.INITIALIZING
            ts_aws.dynamodb.stream_segment.save_stream_segment(ss)
            ts_aws.sqs.stream_segment__download.send_message({
                'stream_id': ss.stream_id,
                'segment': ss.segment,
            })
        if ss._status_download != ts_model.Status.READY:
            raise ts_model.Exception(ts_model.Exception.STREAM_SEGMENT__NOT_DOWNLOADED)

        media_key = f"streams/{stream_id}/{ss.padded}.ts"
        media_filename = f"/tmp/{ss.padded}.ts"
        ts_aws.s3.download_file(media_key, media_filename)

        thumbnail_filename_pattern = f"/tmp/thumb_{ss.stream_id}_{ss.padded}_%06d.jpg"
        ts_media.thumbnail_media_video(media_filename, thumbnail_filename_pattern)
        for thumbnail_filename in glob.glob("/tmp/*.jpg"):
            f = os.path.basename(thumbnail_filename)
            thumbnail_key = f"{ss.stream_id}/{f}"
            ts_aws.s3.upload_file_thumbnails(thumbnail_filename, thumbnail_key)
            os.remove(thumbnail_filename)

        # ------------------------------------------------

        stream_events = []
        for i in range(0, 3):
            has_event = True if random.randint(1, 10) == 1 else False
            if has_event:
                se = ts_model.StreamEvent(
                    stream_id=stream.stream_id,
                    event_id=f"e-{shortuuid.uuid()}",
                    tag="kill" if random.randint(1, 2) == 1 else "knock",
                    time=random.uniform(ss.time_in, ss.time_out),
                    game="fortnite",
                )
                stream_events.append(se)

        # ------------------------------------------------

        # save stream_events
        ts_aws.dynamodb.stream_event.save_stream_events(stream_events)

        # save stream_segment
        ss._status_analyze = ts_model.Status.READY
        ts_aws.dynamodb.stream_segment.save_stream_segment(ss)

        # clean up
        ts_file.delete(media_filename)

        logger.info("success")
        return True

    except Exception as e:
        if type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.STREAM_SEGMENT__ALREADY_ANALYZED,
        ]:
            logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            return True
        elif type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.STREAM__NOT_INITIALIZED,
            ts_model.Exception.STREAM_SEGMENT__NOT_DOWNLOADED,
        ]:
            logger.warn("warn", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            ts_aws.sqs.stream_segment__analyze.change_visibility(receipt_handle)
            raise Exception(e) from None
        else:
            logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            ts_aws.sqs.stream_segment__analyze.change_visibility(receipt_handle)
            raise Exception(e) from None
