import ts_aws.rds.stream
import ts_aws.rds.stream_segment
import ts_aws.sqs.stream__analyze
import ts_aws.sqs.stream__initialize
import ts_aws.sqs.stream_segment__download
import ts_aws.sqs.stream_segment__analyze
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
        receipt_handle = event['Records'][0].get('receiptHandle')

        try:
            stream = ts_aws.rds.stream.get_stream(stream_id)
        except ts_model.Exception as e:
            if e.code == ts_model.Exception.STREAM__NOT_EXIST:
                logger.error("warn", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
                stream = ts_model.Stream(
                    stream_id=stream_id,
                )

        if stream._status_analyze == ts_model.Status.DONE:
            raise ts_model.Exception(ts_model.Exception.STREAM__STATUS_ANALYZE_DONE)

        if stream._status_analyze < ts_model.Status.WORKING:
            stream._status_analyze = ts_model.Status.WORKING
            ts_aws.rds.stream.save_stream(stream)

        if stream._status_initialize < ts_model.Status.WORKING:
            stream._status_initialize = ts_model.Status.WORKING
            ts_aws.rds.stream.save_stream(stream)
            ts_aws.sqs.stream__initialize.send_message({
                'stream_id': stream.stream_id,
            })

        if stream._status_initialize != ts_model.Status.DONE:
            raise ts_model.Exception(ts_model.Exception.STREAM__STATUS_INITIALIZE_NOT_DONE)

        stream_segments = ts_aws.rds.stream_segment.get_stream_segments(stream)
        ready = True
        jobs = []
        for ss in stream_segments:
            to_download = False
            to_analyze = False

            if ss._status_download == ts_model.Status.WORKING:
                ready = False
            if ss._status_download == ts_model.Status.NONE:
                ready = False
                to_download = True
                ss._status_download = ts_model.Status.WORKING

            if ss._status_analyze == ts_model.Status.WORKING:
                ready = False
            if ss._status_analyze == ts_model.Status.NONE:
                ready = False
                to_analyze = True
                ss._status_analyze = ts_model.Status.WORKING

            if to_download == True or to_analyze == True:
                jobs.append({
                    'to_download': to_download,
                    'to_analyze': to_analyze,
                    'ss': ss,
                })

        if len(jobs):
            stream_segments_to_save = list(map(lambda j: j['ss'], jobs))
            ts_aws.rds.stream_segment.save_stream_segments(stream_segments_to_save)

            jobs_download = []
            jobs_analyze = []
            for j in jobs:
                if j['to_download']:
                    jobs_download.append({
                        'stream_id': j['ss'].stream_id,
                        'segment': j['ss'].segment,
                    })
                if j['to_analyze']:
                    jobs_analyze.append({
                        'stream_id': j['ss'].stream_id,
                        'segment': j['ss'].segment,
                    })
                if len(jobs_download) == 10:
                    ts_aws.sqs.stream_segment__download.send_messages(jobs_download)
                    jobs_download = []
                if len(jobs_analyze) == 10:
                    ts_aws.sqs.stream_segment__analyze.send_messages(jobs_analyze)
                    jobs_analyze = []

            if len(jobs_download):
                ts_aws.sqs.stream_segment__download.send_messages(jobs_download)
                jobs_download = []
            if len(jobs_analyze):
                ts_aws.sqs.stream_segment__analyze.send_messages(jobs_analyze)
                jobs_analyze = []

        if not ready:
            raise ts_model.Exception(ts_model.Exception.STREAM__STATUS_ANALYZE_NOT_DONE)

        stream._status_analyze = ts_model.Status.DONE
        ts_aws.rds.stream.save_stream(stream)

        logger.info("success")
        return True

    except Exception as e:
        if type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.STREAM__STATUS_ANALYZE_DONE,
        ]:
            logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            return True
        elif type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.STREAM__STATUS_INITIALIZE_NOT_DONE,
            ts_model.Exception.STREAM__STATUS_ANALYZE_NOT_DONE,
        ]:
            logger.warn("warn", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            ts_aws.sqs.stream__analyze.change_visibility(receipt_handle)
            raise Exception(e) from None
        else:
            logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            ts_aws.sqs.stream__analyze.change_visibility(receipt_handle)
            raise Exception(e) from None
