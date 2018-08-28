import ts_aws.dynamodb.clip
import ts_aws.dynamodb.montage
import ts_logger
import ts_model.Status

import json
import traceback

logger = ts_logger.get(__name__)

def run(event, context):
    try:
        logger.info("start", event=event, context=context)
        status = event['detail']['status']
        media_type = event['detail']['userMetadata']['media_type']
        media_id = event['detail']['userMetadata']['media_id']

        if media_type == "clip":
            media = ts_aws.dynamodb.clip.get_clip(media_id)
        elif media_type == "montage":
            media = ts_aws.dynamodb.montage.get_montage(media_id)

        if status == "COMPLETE":
            media._status_export = ts_model.Status.READY
        else:
            media._status_export = ts_model.Status.NONE
        media.key_media_export = f"{media_type}s/{media_id}/{media_type}.mp4"

        if media_type == "clip":
            ts_aws.dynamodb.clip.save_clip(media)
        elif media_type == "montage":
            ts_aws.dynamodb.montage.save_montage(media)

        logger.info("success")
        return True

    except Exception as e:
        logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
        raise Exception(e) from None

