import ts_aws.dynamodb.stream
import ts_aws.dynamodb.stream_moment
import ts_aws.dynamodb.stream_segment
import ts_aws.s3
import ts_aws.sqs.stream__initialize
import ts_aws.sqs.stream_segment__analyze
import ts_aws.sqs.stream_segment__download
import ts_file
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
import PIL.Image
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

        media_key = f"streams/{ss.stream_id}/{ss.padded}.ts"
        media_filename = f"{filename_prefix}/{ss.padded}.ts"
        ts_aws.s3.download_file(media_key, media_filename)

        filename_raw_pattern = f"{filename_prefix}/raw_%06d.jpg"
        os.makedirs(os.path.dirname(filename_raw_pattern), exist_ok=True)
        cmd = f"ffmpeg -i {media_filename} -vf fps=2 -q:v 1 {filename_raw_pattern}"
        p = subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)

        filenames_raw = sorted(glob.glob(f"{filename_prefix}/raw_*.jpg"))
        for filename_raw in filenames_raw:
            basename_raw = os.path.basename(filename_raw)
            frame_padded = basename_raw.replace("raw_", "").replace(".jpg", "")
            frame = int(frame_padded)
            logger.info("analyzing frame", frame=frame)

            image_raw = PIL.Image.open(filename_raw)
            width, height = image_raw.size
            image_cropped = image_raw.crop((0.388040625 * width, 0.681944 * height, 0.5221875 * width, 0.7277777 * height))

            cropped_filename = f"{filename_prefix}/cropped_{frame_padded}.jpg"
            image_cropped.save(cropped_filename)

            image_cropped = cv2.imread(cropped_filename)
            image_gray = cv2.cvtColor(image_cropped, cv2.COLOR_BGR2GRAY)
            _, mask = cv2.threshold(image_gray, 180, 255, cv2.THRESH_BINARY)
            image_bitwise = cv2.bitwise_and(image_gray, image_gray, mask=mask)
            _, image = cv2.threshold(image_bitwise, 180, 255, cv2.THRESH_BINARY)

            kernel = cv2.getStructuringElement(cv2.MORPH_CROSS, (3, 3))
            image_dilated = cv2.dilate(image, kernel, iterations=9)

            _, contours, _ = cv2.findContours(image_dilated, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_NONE)
            for i, contour in enumerate(contours):
                [x, y, w, h] = cv2.boundingRect(contour)
                if w < 101 and h < 35:
                    continue
                cv2.rectangle(image_cropped, (x, y), (x + w, y + h), (255, 0, 255), 2)
                image_contour = image_cropped[y : y + h, x : x + w]

                contour_filename = f"{filename_prefix}/contour_{frame_padded}_{i}.jpg"
                cv2.imwrite(contour_filename, image_contour)

                image_text = PIL.Image.open(contour_filename)
                texts = pytesseract.image_to_string(image_text).split()
                for t in texts:
                    if Levenshtein.ratio(t, u"KNOCKED") > .7:
                        sm = ts_model.StreamMoment(
                            stream_id=ss.stream_id,
                            moment_id=f"mo-{shortuuid.uuid()}",
                            tag="knocked",
                            time=(frame * 0.5) + ss.time_in,
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
                            time=(frame * 0.5) + ss.time_in,
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
