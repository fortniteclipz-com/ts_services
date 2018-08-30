import ts_aws.dynamodb.stream
import ts_aws.dynamodb.stream_segment
import ts_aws.sqs.stream_initialize
import ts_http
import ts_logger
import ts_model.Exception
import ts_model.Status
import ts_model.Stream
import ts_model.StreamSegment

import json
import re
import streamlink
import traceback

logger = ts_logger.get(__name__)

def run(event, context):
    try:
        logger.info("start", event=event, context=context)
        body = json.loads(event['Records'][0]['body'])
        logger.info("body", body=body)
        stream_id = body['stream_id']
        receipt_handle = event['Records'][0].get('receiptHandle', None)

        # check stream
        stream = ts_aws.dynamodb.stream.get_stream(stream_id)
        if stream._status == ts_model.Status.READY:
            raise ts_model.Exception(ts_model.Exception.STREAM__ALREADY_PROCESSED)

        # get raw m3u8 url from twitch stream url
        try:
            twitch_stream_url = f"https://twitch.tv/videos/{stream_id}"
            twitch_streams = streamlink.streams(twitch_stream_url)
            twitch_stream = twitch_streams['best']
            twitch_stream_url_prefix = "/".join(twitch_stream.url.split("/")[:-1])
        except Exception as e:
            logger.error("warn", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
            raise ts_model.Exception(ts_model.Exception.STREAM_ID__INVALID) from None

        # download raw m3u8
        playlist_filename = f"/tmp/playlist-raw.m3u8"
        ts_http.download_file(twitch_stream.url, playlist_filename)

        # extract segment info from m3u8
        timestamp = 0
        stream_segments = []
        ss_duration = None
        ss = None
        with open(playlist_filename, 'r') as f:
            for line in f:
                if ss is None:
                    ss = ts_model.StreamSegment(
                        stream_id=stream_id
                    )

                if "EXTINF" in line:
                    ss_duration_raw = line.strip()
                    ss_duration = float(re.findall("\d+\.\d+", ss_duration_raw)[0])

                if ".ts" in line:
                    ss_segment_raw = line.strip()
                    ss.segment = int(''.join(filter(str.isdigit, ss_segment_raw)))
                    ss.padded = str(ss.segment).zfill(6)
                    ss.media_url = f"{twitch_stream_url_prefix}/{ss_segment_raw}"

                if ss.segment is not None and ss_duration is not None:
                    ss.time_in = timestamp
                    timestamp += ss_duration
                    ss.time_out = timestamp
                    stream_segments.append(ss)
                    ss = None
                    ss_duration = None

        # save stream_segments
        ts_aws.dynamodb.stream_segment.save_stream_segments(stream_segments)

        # save stream
        stream = ts_model.Stream(
            stream_id=stream_id,
            playlist_url=twitch_stream.url,
            _status=ts_model.Status.READY,
        )
        ts_aws.dynamodb.stream.save_stream(stream)

        logger.info("success")
        return True

    except Exception as e:
        logger.error("error", _module=f"{e.__class__.__module__}", _class=f"{e.__class__.__name__}", _message=str(e), traceback=''.join(traceback.format_exc()))
        if type(e) == ts_model.Exception and e.code in [
            ts_model.Exception.STREAM__NOT_EXIST,
            ts_model.Exception.STREAM__ALREADY_PROCESSED,
            ts_model.Exception.STREAM_ID__INVALID,
        ]:
            return True
        else:
            ts_aws.sqs.stream_initialize.change_visibility(receipt_handle)
            raise Exception(e) from None

