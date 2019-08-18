import ts_aws.rds.stream
import ts_aws.rds.stream_moment
import ts_aws.rds.stream_segment
import ts_aws.s3
import ts_aws.sqs.stream__initialize
import ts_aws.sqs.stream_segment__analyze
import ts_aws.sqs.stream_segment__download
import ts_logger
import ts_model.Exception
import ts_model.Status
import ts_model.Stream
import ts_model.StreamMoment

import glob
import json
import os
import shutil
import subprocess
import traceback

import cv2
import Levenshtein
import pytesseract
import tensorflow as tf
import numpy as np

from tensorflow import keras


def _make_stream_moment(stream_segment, frame, tag):
    sm = ts_model.StreamMoment(
        stream_id=stream_segment.stream_id,
        segment=stream_segment.segment,
        time=(frame * 0.5) + stream_segment.stream_time_in,
        tag=tag,
    )
    logger.info(tag, stream_moment=sm)
    return sm


def _ocr_image(image, stream_segment, frame):
    texts = pytesseract.image_to_string(image).split()
    logger.info("texts", frame=frame, texts=texts)

    # If there are too many texts the streamer may be looking at a website
    # short ciruit out of this segment to prevent lambda timeout
    if len(texts) > 40:
        return {"error": "short_circuit"}

    sm = None
    for t in texts:
        knocked_ratio = Levenshtein.ratio(t, u"KNOCKED")
        eliminated_ratio = Levenshtein.ratio(t, u"ELIMINATED")
        logger.info(f"knocked_ratio: {knocked_ratio}")
        logger.info(f"eliminated_ratio: {eliminated_ratio}")

        if knocked_ratio > .5:
            tag = "knocked"
        elif eliminated_ratio > .5:
            tag = "eliminated"
        else:
            continue

        sm = _make_stream_moment(stream_segment, frame, tag)
        break

    return {"data": sm}


# TODO: we can probably predict batch of all filenames instead of one at a time
def _ml_image(image, stream_segment, frame):
    with session.as_default():
        with session.graph.as_default():
            image_generator = keras.preprocessing.image.ImageDataGenerator(rescale=1/255)

            # We need the input to have 4 dimensions so add a 0 axis
            resized_image = np.expand_dims(cv2.resize(image, IMAGE_SHAPE), axis=0)
            image_data = image_generator.flow(resized_image)
            predicted_batch = ml_model.predict(image_data, verbose=1)
            logger.info(predicted_batch)
            predicted_id = np.argmax(predicted_batch, axis=1)
            logger.info(predicted_id)

            # Label 0 = Kill, Label 1 = Not Kill
            sm = _make_stream_moment(stream_segment, frame, "kill") if predicted_id == 0 else None

            return {"data": sm}


def _get_segment_frames(stream_segment):
    segment_padded = str(stream_segment.segment).zfill(6)
    segment_filepath = f"/tmp/{stream_segment.stream_id}/{segment_padded}"
    os.makedirs(os.path.dirname(segment_filepath), exist_ok=True)

    media_filename = f"{segment_filepath}/{segment_padded}.ts"
    ts_aws.s3.download_file(stream_segment.media_key, media_filename)

    logger.info("creating frames")
    filename_raw_pattern = f"{segment_filepath}/raw_%06d.jpg"
    cmd = f"ffmpeg -i {media_filename} -vf fps=2 -q:v 1 {filename_raw_pattern} < /dev/null"
    p = subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True)

    filenames_raw = sorted(glob.glob(f"{segment_filepath}/raw_*.jpg"))
    logger.info("filenames_raw", filenames_raw=filenames_raw)

    return segment_filepath, filenames_raw

################################################################################
# Constants and global variables
################################################################################
logger = ts_logger.get(__name__)

IMAGE_SHAPE = (160, 160)
ANALYSIS_METHOD = _ml_image
#if os.getenv('REMOVE_SEGMENT_FILES', "True") == "False":
#    REMOVE_SEGMENT_FILES = False
#else:
#    REMOVE_SEGMENT_FILES = True
REMOVE_SEGMENT_FILES = False

# Setup tensorflow (needed this stuff due to esoteric errors)
config = tf.ConfigProto(
    allow_soft_placement=True
)
session = tf.Session(config=config)
keras.backend.set_session(session)

# Load tensorflow model and create prediction function
model_path = f"saved_models/kill_model"
ml_model = keras.experimental.load_from_saved_model(model_path)
ml_model._make_predict_function()

################################################################################
# Runner code
################################################################################
def run(event, context=None):
    if context is None:
        context = {}

    try:
        logger.info("start", event=event, context=context)
        body = json.loads(event['Records'][0]['body'])
        logger.info("body", body=body)
        stream_id = body['stream_id']
        segment = body['segment']
        receipt_handle = event['Records'][0].get('receiptHandle')

        try:
            stream = ts_aws.rds.stream.get_stream(stream_id)
        except ts_model.Exception as e:
            if e.code == ts_model.Exception.STREAM__NOT_EXIST:
                logger.error("warn", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
                stream = ts_model.Stream(
                    stream_id=stream_id,
                )

        if stream._status_initialize == ts_model.Status.ERROR:
            raise ts_model.Exception(ts_model.Exception.STREAM__STATUS_INITIALIZE_ERROR)

        if stream._status_initialize == ts_model.Status.NONE:
            stream._status_initialize = ts_model.Status.WORKING
            ts_aws.rds.stream.save_stream(stream)
            ts_aws.sqs.stream__initialize.send_message({
                'stream_id': stream.stream_id,
            })

        if stream._status_initialize != ts_model.Status.DONE:
            raise ts_model.Exception(ts_model.Exception.STREAM__STATUS_INITIALIZE_NOT_DONE)

        stream_segment = ts_aws.rds.stream_segment.get_stream_segment(stream, segment)

        if stream_segment._status_analyze == ts_model.Status.DONE:
            raise ts_model.Exception(ts_model.Exception.STREAM_SEGMENT__STATUS_ANALYZE_DONE)

        if stream_segment._status_analyze < ts_model.Status.WORKING:
            stream_segment._status_analyze = ts_model.Status.WORKING
            ts_aws.rds.stream_segment.save_stream_segment(stream_segment)

        if stream_segment._status_download == ts_model.Status.NONE:
            stream_segment._status_download = ts_model.Status.WORKING
            ts_aws.rds.stream_segment.save_stream_segment(stream_segment)
            ts_aws.sqs.stream_segment__download.send_message({
                'stream_id': stream_segment.stream_id,
                'segment': stream_segment.segment,
            })

        if stream_segment._status_download != ts_model.Status.DONE:
            raise ts_model.Exception(ts_model.Exception.STREAM_SEGMENT__STATUS_DOWNLOAD_NOT_DONE)

        segment_filepath, filenames_raw = _get_segment_frames(stream_segment)

        stream_moments = []
        for filename_raw in filenames_raw:
            basename_raw = os.path.basename(filename_raw)
            frame_filepath = basename_raw.replace("raw_", "").replace(".jpg", "")
            frame = int(frame_filepath)
            logger.info("analyzing frame", frame=frame, filename_raw=filename_raw)

            image_raw = cv2.imread(filename_raw)
            # TODO: need to revert to division cropping as exact crop only works on 1920x1080
            image_cropped = image_raw[756:905, 723:1200]
            #height, width, _ = image_raw.shape
            #top = int(height * 5 / 8)
            #bottom = int(height * 7 / 8)
            #left = int(width * 0.25)
            #right = int(width * 0.75)
            #image_cropped = image_raw[top:bottom, left:right]

            # Code for greyscaling and thresholding images for OCR, works well on Fortnite
            # doesn't work well on Apex Legends
            #image_gray = cv2.cvtColor(image_cropped, cv2.COLOR_BGR2GRAY)
            #_, image_threshold = cv2.threshold(image_gray, 240, 255, cv2.THRESH_BINARY)
            #kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
            #image_dilated = cv2.dilate(image_threshold, kernel, iterations=1)
            #dilated_filename = f"{segment_filepath}/dilated_{frame_filepath}.jpg"
            #cv2.imwrite(dilated_filename, image_dilated)

            response = ANALYSIS_METHOD(image_cropped, stream_segment, frame)
            if "error" in response:
                if response["error"] == "short_circuit":
                    break
            elif "data" in response:
                if response["data"] is None:
                    cropped_filename = f"{segment_filepath}/notkill_{frame_filepath}.jpg"
                else:
                    cropped_filename = f"{segment_filepath}/kill_{frame_filepath}.jpg"
                    stream_moments.append(response["data"])

                cv2.imwrite(cropped_filename, image_cropped)
                logger.info(cropped_filename)

        ts_aws.rds.stream_moment.save_stream_moments(stream_moments)

        stream_segment._status_analyze = ts_model.Status.DONE
        ts_aws.rds.stream_segment.save_stream_segment(stream_segment)

        if REMOVE_SEGMENT_FILES:
            shutil.rmtree(segment_filepath)

        logger.info("success")
        return True

    except Exception as e:
        if type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.STREAM__STATUS_INITIALIZE_ERROR,
            ts_model.Exception.STREAM_SEGMENT__STATUS_ANALYZE_DONE,
        ]:
            logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            return True
        elif type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.STREAM__STATUS_INITIALIZE_NOT_DONE,
            ts_model.Exception.STREAM_SEGMENT__STATUS_DOWNLOAD_NOT_DONE,
        ]:
            logger.warn("warn", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            ts_aws.sqs.stream_segment__analyze.change_visibility(receipt_handle)
            raise Exception(e) from None
        else:
            logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            ts_aws.sqs.stream_segment__analyze.change_visibility(receipt_handle)
            raise Exception(e) from None
