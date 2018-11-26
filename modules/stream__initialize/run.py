import ts_aws.rds.stream
import ts_aws.rds.stream_segment
import ts_aws.sqs.stream__initialize
import ts_file
import ts_http
import ts_libs
import ts_logger
import ts_model.Exception
import ts_model.Status
import ts_model.Stream
import ts_model.StreamSegment

import ffprobe3
import json
import re
import streamlink
import traceback

logger = ts_logger.get(__name__)
ts_libs.init()

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

        if stream._status_initialize == ts_model.Status.NONE:
            stream._status_initialize = ts_model.Status.INITIALIZING
            ts_aws.rds.stream.save_stream(stream)

        if stream._status_initialize == ts_model.Status.READY:
            raise ts_model.Exception(ts_model.Exception.STREAM__ALREADY_INITIALIZED)

        try:
            twitch_url = f"https://twitch.tv/videos/{stream_id}"
            twitch_streams = streamlink.streams(twitch_url)
            logger.info("twitch_streams", twitch_streams=twitch_streams)
            twitch_stream = twitch_streams['best']
            twitch_stream_url_prefix = "/".join(twitch_stream.url.split("/")[:-1])
        except Exception as e:
            logger.error("warn", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            raise ts_model.Exception(ts_model.Exception.STREAM_ID__NOT_VALID) from None

        playlist_filename = f"/tmp/playlist-raw.m3u8"
        ts_http.download_file(twitch_stream.url, playlist_filename)

        timestamp = 0
        stream_segments = []
        ss_duration = None
        ss = None
        with open(playlist_filename, 'r') as f:
            for line in f:
                if ss is None:
                    ss = ts_model.StreamSegment(
                        stream_id=stream_id,
                    )

                if "EXTINF" in line:
                    ss_duration_raw = line.strip()
                    ss_duration = float(re.findall("\d+\.\d+", ss_duration_raw)[0])

                if ".ts" in line:
                    ss_segment_raw = line.strip()
                    ss.segment = int(''.join(filter(str.isdigit, ss_segment_raw)))
                    ss.media_url = f"{twitch_stream_url_prefix}/{ss_segment_raw}"

                if ss.segment is not None and ss_duration is not None:
                    ss.stream_time_in = timestamp
                    timestamp += ss_duration
                    ss.stream_time_out = timestamp
                    stream_segments.append(ss)
                    ss = None
                    ss_duration = None

        ts_file.delete(playlist_filename)

        width = 0
        height = 0
        fps_numerator = 0
        fps_denominator = 0
        first_stream_segment = stream_segments[0]
        segment_padded = str(first_stream_segment.segment).zfill(6)
        media_filename = f"/tmp/{segment_padded}.ts"
        ts_http.download_file(first_stream_segment.media_url, media_filename)
        metadata = ffprobe3.FFProbe(media_filename)
        for s in metadata.streams:
            if s.is_video():
                width = s.width
                height = s.height
                [fps_numerator, fps_denominator] = s.r_frame_rate.split("/")
        ts_file.delete(media_filename)

        ts_aws.rds.stream_segment.save_stream_segments(stream_segments)

        stream.streamer = "_".join(twitch_stream.url.split("/")[3].split("_")[1:-2])
        stream.playlist_url = twitch_stream.url
        stream.duration = timestamp
        stream.fps_numerator = int(fps_numerator)
        stream.fps_denominator = int(fps_denominator)
        stream.width = width
        stream.height = height
        stream._status_initialize = ts_model.Status.READY
        ts_aws.rds.stream.save_stream(stream)

        logger.info("success")
        return True

    except Exception as e:
        logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
        if type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.STREAM__ALREADY_INITIALIZED,
            ts_model.Exception.STREAM_ID__NOT_VALID,
        ]:
            return True
        else:
            ts_aws.sqs.stream__initialize.change_visibility(receipt_handle)
            raise Exception(e) from None

