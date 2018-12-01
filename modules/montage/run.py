import ts_aws.rds.clip
import ts_aws.rds.montage
import ts_aws.mediaconvert.montage
import ts_aws.sqs.clip
import ts_aws.sqs.montage
import ts_logger
import ts_model.Exception
import ts_model.Status

import functools
import json
import traceback

logger = ts_logger.get(__name__)

def run(event, context):
    try:
        logger.info("start", event=event, context=context)
        body = json.loads(event['Records'][0]['body'])
        logger.info("body", body=body)
        montage_id = body['montage_id']
        receipt_handle = event['Records'][0].get('receiptHandle')

        montage = ts_aws.rds.montage.get_montage(montage_id)

        if montage._status == ts_model.Status.DONE:
            raise ts_model.Exception(ts_model.Exception.MONTAGE__STATUS_DONE)

        if montage._status < ts_model.Status.WORKING:
            montage._status = ts_model.Status.WORKING
            montage = ts_aws.rds.montage.save_montage(montage)

        montage_clips = ts_aws.rds.clip.get_montage_clips(montage)
        ready = True
        jobs = []
        for mc in montage_clips:
            to_clip = False

            if mc._status == ts_model.Status.WORKING:
                ready = False
            if mc._status == ts_model.Status.NONE:
                ready = False
                to_clip = True
                mc._status = ts_model.Status.WORKING

            if to_clip == True:
                jobs.append({
                    'to_clip': to_clip,
                    'mc': mc,
                })

        if len(jobs):
            clips_to_save = list(map(lambda j: j['mc'], jobs))
            ts_aws.rds.clip.save_clips(clips_to_save)

            jobs_create = []
            for j in jobs:
                if j['to_clip']:
                    jobs_create.append({
                        'clip_id': j['mc'].clip_id,
                    })
                if len(jobs_create) == 10:
                    ts_aws.sqs.clip.send_messages(jobs_create)
                    jobs_create = []

            if len(jobs_create):
                ts_aws.sqs.clip.send_messages(jobs_create)
                jobs_create = []

        if not ready:
            raise ts_model.Exception(ts_model.Exception.MONTAGE_CLIPS__STATUS_NOT_DONE)

        def finalize(acc, mc):
            mcs = acc[0]
            clips = acc[1]
            duration = acc[2]
            if mc._status == ts_model.Status.DONE:
                mcs.append(mc)
                clips = clips + 1
                duration = duration + (mc.time_out - mc.time_in)
            return (mcs, clips, duration)

        (montage_clips, clips, duration) = functools.reduce(finalize, montage_clips, ([], 0 , 0))
        ts_aws.mediaconvert.montage.create(montage, montage_clips)

        montage.clips = clips
        montage.duration = duration
        ts_aws.rds.montage.save_montage(montage)

        logger.info("success", montage=montage)
        return True

    except Exception as e:
        if type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.MONTAGE__NOT_EXIST,
            ts_model.Exception.MONTAGE__STATUS_DONE,
            ts_model.Exception.MONTAGE_CLIPS__NOT_EXIST,
        ]:
            logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            return True
        elif type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.MONTAGE_CLIPS__STATUS_NOT_DONE,
        ]:
            logger.warn("warn", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            ts_aws.sqs.montage.change_visibility(receipt_handle)
            raise Exception(e) from None
        else:
            logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            ts_aws.sqs.montage.change_visibility(receipt_handle)
            raise Exception(e) from None

