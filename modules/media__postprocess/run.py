import ts_aws.dynamodb.clip
import ts_aws.dynamodb.montage
import ts_logger
import ts_model.Status
import ts_model.Exception

import traceback

logger = ts_logger.get(__name__)

def run(event, context):
    try:
        logger.info("start", event=event, context=context)
        userMetadata = event['detail'].get('userMetadata') or {}
        clip_id = userMetadata.get('clip_id') or None
        montage_id = userMetadata.get('montage_id') or None
        status = event['detail']['status']

        if clip_id is not None:
            media_type = "clip"
            media = ts_aws.dynamodb.clip.get_clip(clip_id)
            media_id = clip_id
        elif montage_id is not None:
            media_type = "montage"
            media = ts_aws.dynamodb.montage.get_montage(montage_id)
            media_id = montage_id
        else:
            raise ts_model.Exception(ts_model.Exception.MEDIA__NOT_EXIST)

        if status == "COMPLETE":
            media._status = ts_model.Status.READY
            media.media_key = f"{media_type}s/{media_id}/{media_type}.mp4"
        else:
            media._status = ts_model.Status.NONE

        if media_type == "clip":
            ts_aws.dynamodb.clip.save_clip(media)
        elif media_type == "montage":
            ts_aws.dynamodb.montage.save_montage(media)

        logger.info("success")
        return True

    except Exception as e:
        logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
        raise Exception(e) from None

