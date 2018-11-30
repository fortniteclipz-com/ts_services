import ts_aws.rds.montage_clip
import ts_aws.rds.montage
import ts_aws.mediaconvert.montage
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

        if montage._status == ts_model.Status.NONE:
            montage._status = ts_model.Status.WORKING
            montage = ts_aws.rds.montage.save_montage(montage)

        if montage._status == ts_model.Status.DONE:
            raise ts_model.Exception(ts_model.Exception.MONTAGE__ALREADY_CREATED)

        montage_clips = ts_aws.rds.montage_clip.get_montage_clips(montage)
        if not all((c._status == ts_model.Status.DONE or c._status == ts_model.Status.ERROR) for c in montage_clips):
            raise ts_model.Exception(ts_model.Exception.MONTAGE_CLIPS__NOT_CREATED)

        def process(acc, mc):
            mcs = acc[0]
            clips = acc[1]
            duration = acc[2]
            if mc._status == ts_model.Status.DONE:
                mcs.append(mc)
                clips = clips + 1
                duration = duration + (mc.time_out - mc.time_in)
            return (mcs, clips, duration)

        (montage_clips, clips, duration) = functools.reduce(process, montage_clips, ([], 0 , 0))
        ts_aws.mediaconvert.montage.create(montage, montage_clips)

        montage.clips = clips
        montage.duration = duration
        ts_aws.rds.montage.save_montage(montage)

        logger.info("success", montage=montage)
        return True

    except Exception as e:
        if type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.MONTAGE__NOT_EXIST,
            ts_model.Exception.MONTAGE__ALREADY_CREATED,
            ts_model.Exception.MONTAGE_CLIPS__NOT_EXIST,
        ]:
            logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            return True
        elif type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.MONTAGE_CLIPS__NOT_CREATED,
        ]:
            logger.warn("warn", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            ts_aws.sqs.montage.change_visibility(receipt_handle)
            raise Exception(e) from None
        else:
            logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            ts_aws.sqs.montage.change_visibility(receipt_handle)
            raise Exception(e) from None

