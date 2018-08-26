import ts_aws.dynamodb.stream
import ts_aws.dynamodb.stream_segment
import ts_aws.sqs.stream_initialize
import ts_file
import ts_http
import ts_logger
import ts_media
import ts_model.Exception
import ts_model.Status
import ts_model.Stream
import ts_model.StreamSegment

import json
import re
import streamlink

ts_media.init_ff_libs()
logger = ts_logger.get(__name__)

def run(event, context):
    try:
        logger.info("start", event=event, context=context)
        body = json.loads(event['Records'][0]['body'])
        logger.info("body", body=body)
        stream_id = body['stream_id']

        # check stream
        stream = ts_aws.dynamodb.stream.get_stream(stream_id)
        if stream is None:
            raise ts_model.Exception(ts_model.Exception.STREAM_NOT_EXISTS)

        if stream._status == ts_model.Status.READY:
            raise ts_model.Exception(ts_model.Exception.STREAM_ALREADY_PROCESSED)

        # get raw m3u8 url from twitch stream url
        twitch_stream_url = f"https://twitch.tv/videos/{stream_id}"
        twitch_streams = streamlink.streams(twitch_stream_url)
        twitch_stream = twitch_streams['best']
        twitch_stream_url_prefix = "/".join(twitch_stream.url.split("/")[:-1])

        # download raw m3u8 and extract segment info from m3u8
        playlist_filename = f"/tmp/playlist-raw.m3u8"
        ts_http.download_file(twitch_stream.url, playlist_filename)
        stream_segments = []
        ss = ts_model.StreamSegment()
        with open(playlist_filename, 'r') as f:
            for line in f:
                if "EXTINF" in line:
                    time_duration_raw = line.strip()
                    time_duration = float(re.findall("\d+\.\d+", time_duration_raw)[0])
                    ss.time_duration = time_duration
                if ".ts" in line:
                    segment_raw = line.strip()
                    segment = int(''.join(filter(str.isdigit, segment_raw)))
                    segment_padded = str(segment).zfill(6)
                    url_media_raw = f"{twitch_stream_url_prefix}/{segment_raw}"
                    ss.segment = segment
                    ss.padded = segment_padded
                    ss.url_media_raw = url_media_raw
                if ss.time_duration is not None and ss.segment is not None:
                    ss._status_download = ts_model.Status.NONE
                    ss._status_fresh = ts_model.Status.NONE
                    ss._status_analyze = ts_model.Status.NONE
                    stream_segments.append(ss)
                    ss = ts_model.StreamSegment()

        # download first segment, probe, and get stream_time_offset
        first_ss = stream_segments[0]
        media_filename = f"/tmp/{first_ss.padded}.ts"
        packets_filename = f"/tmp/{first_ss.padded}.json"
        ts_http.download_file(first_ss.url_media_raw, media_filename)
        ts_media.probe_media_video(media_filename, packets_filename)
        packets_video = ts_file.get_json(packets_filename)['packets']
        stream_time_offset = float(packets_video[0]['pts_time'])

        # delete tmp files
        ts_file.delete(playlist_filename)
        ts_file.delete(media_filename)
        ts_file.delete(packets_filename)

        # add stream_id, time_in, and time_out to stream_segments
        timestamp = stream_time_offset
        for ss in stream_segments:
            ss.stream_id = stream_id
            ss.time_in = timestamp
            timestamp += ss.time_duration
            ss.time_out = timestamp

        # save stream_segments
        ts_aws.dynamodb.stream_segment.save_stream_segments(stream_segments)

        # save stream
        stream = ts_model.Stream(
            stream_id=stream_id,
            time_offset=stream_time_offset,
            url_playlist_raw=twitch_stream.url,
            _status=ts_model.Status.READY
        )
        ts_aws.dynamodb.stream.save_stream(stream)

        logger.info("success")
        return True

    except ts_model.Exception as e:
        if e.code in [
            ts_model.Exception.STREAM_NOT_EXISTS,
            ts_model.Exception.STREAM_ALREADY_PROCESSED,
        ]:
            logger.error("error", code=e.code)
            pass
        else:
            logger.warn("warn", code=e.code)
            receipt_handle = event['Records'][0]['receiptHandle']
            ts_aws.sqs.stream_initialize.change_visibility(receipt_handle)
            raise Exception(e) from None

    except Exception as e:
        logger.error("error", traceback=''.join(traceback.format_tb(e.__traceback__)))
        receipt_handle = event['Records'][0]['receiptHandle']
        ts_aws.sqs.stream_initialize.change_visibility(receipt_handle)
        raise Exception(e) from None

