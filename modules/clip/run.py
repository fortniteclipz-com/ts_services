import ts_aws.dynamodb.clip
import ts_aws.dynamodb.clip_segment
import ts_aws.dynamodb.stream
import ts_aws.dynamodb.stream_segment
import ts_aws.mediaconvert.clip
import ts_aws.sqs.clip
import ts_aws.sqs.stream__initialize
import ts_aws.sqs.stream_segment__download
import ts_logger
import ts_model.ClipSegment
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
        clip_id = body['clip_id']
        receipt_handle = event['Records'][0].get('receiptHandle', None)

        # get clip
        clip = ts_aws.dynamodb.clip.get_clip(clip_id)

        # check if clip is already processed
        if clip._status == ts_model.Status.READY:
            raise ts_model.Exception(ts_model.Exception.CLIP__ALREADY_PROCESSED)

        # get/initialize stream
        try:
            stream = ts_aws.dynamodb.stream.get_stream(stream_id)
        except ts_model.Exception as e:
            if e.code == ts_model.Exception.STREAM__NOT_EXIST:
                logger.error("warn", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
                stream = ts_model.Stream(
                    stream_id=stream_id,
                    _status=ts_model.Status.INITIALIZING
                )
                ts_aws.dynamodb.stream.save_stream(stream)
                ts_aws.sqs.stream_initialize.send_message({
                    'stream_id': stream_id,
                })

        # check if stream is ready
        if stream._status != ts_model.Status.READY:
            raise ts_model.Exception(ts_model.Exception.STREAM__NOT_READY)

        # check if all clip_stream_segments are ready to process
        clip_stream_segments = ts_aws.dynamodb.clip.get_clip_stream_segments(stream, clip)
        ready_to_clip = True
        clip_stream_segments_to_save = []
        for css in clip_stream_segments:
            if css._status_download == ts_model.Status.INITIALIZING:
                ready_to_clip = False
            if css._status_download == ts_model.Status.NONE:
                ready_to_clip = False
                css._status_download = ts_model.Status.INITIALIZING
                clip_stream_segments_to_save.append(css)

        # if not ready to clip send queue/save db status of stream_segments
        if not ready_to_clip:
            ts_aws.dynamodb.stream_segment.save_stream_segments(clip_stream_segments_to_save)
            for ss in clip_stream_segments_to_save:
                ts_aws.sqs.stream_segment__download.send_message({
                    'stream_id': ss.stream_id,
                    'segment': ss.segment,
                })
            raise ts_model.Exception(ts_model.Exception.STREAM_SEGMENTS__NOT_READY)

        # create clip segments
        clip_segments = []
        for i, css in enumerate(clip_stream_segments):
            is_first_cs = True if i == 0 else False
            is_last_cs = True if i == (len(clip_stream_segments) - 1) else False
            time_in = clip.time_in - css.time_in  if is_first_cs else None
            time_out = clip.time_out - css.time_in if is_last_cs else None
            cs = ts_model.ClipSegment(
                clip_id=clip.clip_id,
                segment=css.segment,
                stream_id=clip.stream_id,
                media_key=css.media_key,
                time_in=time_in,
                time_out=time_out,
            )
            clip_segments.append(cs)

        ts_aws.mediaconvert.clip.create(stream, clip, clip_segments)
        ts_aws.dynamodb.clip_segment.save_clip_segments(clip_segments)
        ts_aws.dynamodb.clip.save_clip(clip)

        logger.info("success")
        return True

    except Exception as e:
        if type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.CLIP__NOT_EXIST,
            ts_model.Exception.CLIP__ALREADY_PROCESSED,
        ]:
            logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            return True
        elif type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.STREAM__NOT_READY,
            ts_model.Exception.STREAM_SEGMENTS__NOT_READY,
        ]:
            logger.warn("warn", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            ts_aws.sqs.clip.change_visibility(receipt_handle)
            raise Exception(e) from None
        else:
            logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            ts_aws.sqs.clip.change_visibility(receipt_handle)
            raise Exception(e) from None

