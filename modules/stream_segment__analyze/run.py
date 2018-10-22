import ts_aws.dynamodb.stream
import ts_aws.dynamodb.stream_moment
import ts_aws.dynamodb.stream_segment
import ts_aws.s3
import ts_aws.sqs.stream__initialize
import ts_aws.sqs.stream_segment__analyze
import ts_aws.sqs.stream_segment__download
import ts_libs
import ts_logger
import ts_model.Exception
import ts_model.Status
import ts_model.Stream
import ts_model.StreamMoment

import glob
import json
import os
import shortuuid
import shutil
import subprocess
import traceback

import cv2
import Levenshtein
import pytesseract

logger = ts_logger.get(__name__)
ts_libs.init()

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

        stream_moments = []
        filename_prefix = f"/tmp/{ss.stream_id}/{ss.padded}"
        os.makedirs(os.path.dirname(filename_prefix), exist_ok=True)

        media_filename = f"{filename_prefix}/{ss.padded}.ts"
        ts_aws.s3.download_file(ss.media_key, media_filename)

        logger.info("creating frames")
        filename_raw_pattern = f"{filename_prefix}/raw_%06d.jpg"
        cmd = f"ffmpeg -i {media_filename} -vf fps=2 -q:v 1 {filename_raw_pattern} < /dev/null"
        p = subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)

        filenames_raw = sorted(glob.glob(f"{filename_prefix}/raw_*.jpg"))
        logger.info("filenames_raw", filenames_raw=filenames_raw)

        for filename_raw in filenames_raw:
            basename_raw = os.path.basename(filename_raw)
            frame_padded = basename_raw.replace("raw_", "").replace(".jpg", "")
            frame = int(frame_padded)
            logger.info("analyzing frame", frame=frame, filename_raw=filename_raw)

            image_raw = cv2.imread(filename_raw)
            height, width, _ = image_raw.shape
            top = int(height / 2)
            bottom = height
            left = int(width * 0.25)
            right = int(width * 0.75)
            image_cropped = image_raw[top:bottom, left:right]

            # saving for dev
            # cropped_filename = f"{filename_prefix}/cropped_{frame_padded}.jpg"
            # cv2.imwrite(cropped_filename, image_cropped)

            image_gray = cv2.cvtColor(image_cropped, cv2.COLOR_BGR2GRAY)
            _, image_threshold = cv2.threshold(image_gray, 240, 255, cv2.THRESH_BINARY)
            kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
            image_dilated = cv2.dilate(image_threshold, kernel, iterations=1)

            # saving for dev
            # dilated_filename = f"{filename_prefix}/dilated_{frame_padded}.jpg"
            # cv2.imwrite(dilated_filename, image_dilated)

            texts = pytesseract.image_to_string(image_dilated).split()
            for t in texts:
                if Levenshtein.ratio(t, u"KNOCKED") > .7:
                    sm = ts_model.StreamMoment(
                        stream_id=ss.stream_id,
                        moment_id=f"mo-{shortuuid.uuid()}",
                        tag="knocked",
                        time=(frame * 0.5) + ss.stream_time_in,
                        game="fortnite",
                        segment=ss.segment,
                    )
                    logger.info("knocked", stream_moment=sm)
                    stream_moments.append(sm)

                if Levenshtein.ratio(t, u"ELIMINATED") > .7:
                    sm = ts_model.StreamMoment(
                        stream_id=ss.stream_id,
                        moment_id=f"mo-{shortuuid.uuid()}",
                        tag="eliminated",
                        time=(frame * 0.5) + ss.stream_time_in,
                        game="fortnite",
                        segment=ss.segment,
                    )
                    logger.info("eliminated", stream_moment=sm)
                    stream_moments.append(sm)

        # save stream_moments
        ts_aws.dynamodb.stream_moment.save_stream_moments(stream_moments)

        # save stream_segment
        ss._status_analyze = ts_model.Status.READY
        ts_aws.dynamodb.stream_segment.save_stream_segment(ss)

        # clean up
        shutil.rmtree(filename_prefix)

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
