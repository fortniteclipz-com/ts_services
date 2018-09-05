import ts_aws.dynamodb.clip
import ts_aws.dynamodb.montage
import ts_aws.dynamodb.montage_clip
import ts_aws.mediaconvert.montage
import ts_aws.sqs.montage
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
        montage_id = body['montage_id']
        receipt_handle = event['Records'][0].get('receiptHandle')

        # check montage
        montage = ts_aws.dynamodb.montage.get_montage(montage_id)

        # check if montage is already created
        if montage._status == ts_model.Status.READY:
            raise ts_model.Exception(ts_model.Exception.MONTAGE__ALREADY_CREATED)

        clips = ts_aws.dynamodb.clip.get_clips(montage.clip_ids)
        if not all(c._status == ts_model.Status.READY for c in clips):
            raise ts_model.Exception(ts_model.Exception.CLIPS__NOT_READY)

        # create montage_clips
        montage_clips = []
        for index, clip in enumerate(clips):
            montage_clip = ts_model.MontageClip(
                montage_id=montage.montage_id,
                clip_id=clip.clip_id,
                clip_order=index,
                media_key=clip.media_key,
            )
            montage_clips.append(montage_clip)

        ts_aws.mediaconvert.montage.create(montage, montage_clips)
        ts_aws.dynamodb.montage_clip.save_montage_clips(montage_clips)
        ts_aws.dynamodb.montage.save_montage(montage)

        logger.info("success", montage=montage)
        return True

    except Exception as e:
        if type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.MONTAGE__NOT_EXIST,
            ts_model.Exception.MONTAGE__ALREADY_CREATED,
        ]:
            logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            return True
        elif type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.CLIPS__NOT_READY,
        ]:
            logger.warn("warn", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            ts_aws.sqs.montage.change_visibility(receipt_handle)
            raise Exception(e) from None
        else:
            logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            ts_aws.sqs.montage.change_visibility(receipt_handle)
            raise Exception(e) from None

