import ts_aws.dynamodb
import ts_aws.dynamodb.clip
import ts_aws.dynamodb.clip_segment
import ts_aws.dynamodb.stream
import ts_aws.dynamodb.stream_segment
import ts_aws.s3
import ts_aws.sqs.clip
import ts_aws.sqs.stream_initialize
import ts_aws.sqs.stream_segment_download
import ts_config
import ts_file
import ts_logger
import ts_media
import helpers

import json

ts_media.init_ff_libs()
logger = ts_logger.get(__name__)

def run(event, context):
    logger.info("start", event=event, context=context)
    body = json.loads(event['Records'][0]['body'])
    logger.info("body", body=body)
    clip_id = body['clip_id']

    # get clip
    clip = ts_aws.dynamodb.clip.get_clip(clip_id)
    if clip is None:
        logger.error(f"No clip with clip_id {clip_id}")
        return

    # check if clip already ready
    logger.info("clip", clip=clip.__dict__)
    if clip._status == ts_aws.dynamodb.Status.READY:
        logger.warn(f"Already processed clip")
        return

    # init/get stream and stream_segments
    logger.info("checking stream")
    stream = ts_aws.dynamodb.stream.get_stream(clip.stream_id)
    if stream is None or stream._status == ts_aws.:
        payload = {
            'stream_id': clip.stream_id,
        }
        logger.info("pushing to stream_initialize sqs", payload=payload)
        ts_aws.sqs.stream_initialize.send_message(payload)

        stream = ts_aws.dynamodb.stream.Stream(
            stream_id=stream_id,
            _status=ts_aws.dynamodb.Status.INITIALIZING
        )
        ts_aws.dynamodb.stream.save_stream(stream)
        raise Exception("Stream not initialized yet")

    # get clip_stream_segments
    logger.info("stream", stream=stream.__dict__)
    logger.info("get clip_stream_segments")
    clip_stream_segments = ts_aws.dynamodb.clip.get_clip_stream_segments(stream, clip)
    logger.info("clip_stream_segments", clip_stream_segments_length=len(clip_stream_segments))

    # check if all stream_segments are ready to process
    ready_to_clip = True
    stream_segments_to_update = []
    for i, css in enumerate(clip_stream_segments):
        download = False
        is_first_css = True if i == 0 else False
        if is_first_css and css._status_fresh == ts_aws.dynamodb.Status.NONE:
            download = True
            css._status_fresh = ts_aws.dynamodb.Status.INITIALIZING
        if css._status_download == ts_aws.dynamodb.Status.NONE:
            download = True
            css._status_download = ts_aws.dynamodb.Status.INITIALIZING
        if download:
            ready_to_clip = False
            payload = {
                'stream_id': css.stream_id,
                'segment': css.segment,
            }
            logger.info("pushing to stream_segment_download sqs", payload=payload)
            ts_aws.sqs.stream_segment_download.send_message(payload)
            stream_segments_to_update.append(css)

    # update queue status of stream_segments
    ts_aws.dynamodb.stream_segment.save_stream_segments(stream_segments_to_update)
    if not ready_to_clip:
        receipt_handle = event['Records'][0]['receiptHandle']
        ts_aws.sqs.clip.change_visibility(receipt_handle)
        raise Exception("Not all clip segments processed yet")

    # update clip segments
    logger.info("creating clip_segments")
    clip_segments = []
    for i, css in enumerate(clip_stream_segments):
        logger.info("ingesting clip_segment", segment=css.segment)
        is_first_cs = True if i == 0 else False
        is_last_cs = True if i == (len(clip_stream_segments) - 1) else False
        cs = ts_aws.dynamodb.clip_segment.ClipSegment(
            clip_id=clip.clip_id,
            segment=css.segment,
        )

        bucket = ts_config.get('aws.s3.main.name')
        region = ts_config.get('aws.s3.main.region')
        url_media_prefix = F"https://s3-{region}.amazonaws.com/{bucket}"
        video_url_media = css.key_media_video_fresh if is_first_cs else css.key_media_video
        cs.video_url_media = f"{url_media_prefix}/{video_url_media}"
        cs.audio_url_media = f"{url_media_prefix}/{css.key_media_audio}"

        if is_first_cs is False and is_last_cs is False:
            cs.video_time_in = css.time_in
            cs.video_time_out = css.time_out
            cs.video_time_duration = css.time_duration
            cs.audio_time_in = css.time_in
            cs.audio_time_out = css.time_out
            cs.audio_time_duration = css.time_duration
        else:
            packets_filename_video = f"/tmp/{css.padded}_video.json"
            packets_filename_audio = f"/tmp/{css.padded}_audio.json"
            packets_key_video = css.key_packets_video_fresh if is_first_cs else css.key_packets_video
            ts_aws.s3.download_file(packets_key_video, packets_filename_video)
            ts_aws.s3.download_file(css.key_packets_audio, packets_filename_audio)

            packets_video = ts_file.get_json(packets_filename_video)['packets']
            packets_audio = ts_file.get_json(packets_filename_audio)['packets']
            clip_time_in_offset = clip.time_in + stream.time_offset
            clip_time_out_offset = clip.time_out + stream.time_offset
            (
                cs.video_time_duration,
                cs.video_time_in,
                cs.video_time_out,
                cs.video_packets_pos,
                cs.video_packets_byterange,
            ) = helpers.get_packets_data(
                packets_video,
                is_first_cs,
                is_last_cs,
                clip_time_in_offset,
                clip_time_out_offset,
            )
            (
                cs.audio_time_duration,
                cs.audio_time_in,
                cs.audio_time_out,
                audio_packets_pos,
                audio_packets_byterange,
            ) = helpers.get_packets_data(
                packets_audio,
                is_first_cs,
                is_last_cs,
                cs.video_time_in,
                cs.video_time_out,
            )
            if is_last_cs:
                cs.audio_packets_pos = audio_packets_pos
                cs.audio_packets_byterange = audio_packets_byterange

            ts_file.delete(packets_filename_video)
            ts_file.delete(packets_filename_audio)

        clip_segments.append(cs)

    # creating/uploading m3u8
    logger.info("creating/uploading m3u8")
    m3u8_filename_master = f"/tmp/playlist-master.m3u8"
    m3u8_filename_video = f"/tmp/playlist-video.m3u8"
    m3u8_filename_audio = f"/tmp/playlist-audio.m3u8"
    ts_media.create_m3u8(clip_segments, m3u8_filename_master, m3u8_filename_video, m3u8_filename_audio)
    m3u8_key_master = f"clips/{clip_id}/playlist-master.m3u8"
    m3u8_key_video = f"clips/{clip_id}/playlist-video.m3u8"
    m3u8_key_audio = f"clips/{clip_id}/playlist-audio.m3u8"
    ts_aws.s3.upload_file(m3u8_filename_master, m3u8_key_master)
    ts_aws.s3.upload_file(m3u8_filename_video, m3u8_key_video)
    ts_aws.s3.upload_file(m3u8_filename_audio, m3u8_key_audio)
    ts_file.delete(m3u8_filename_master)
    ts_file.delete(m3u8_filename_video)
    ts_file.delete(m3u8_filename_audio)

    logger.info("saving clip and clip_segments")
    clip.key_playlist_master = m3u8_key_master
    clip.key_playlist_video = m3u8_key_video
    clip.key_playlist_audio = m3u8_key_audio
    clip._status = ts_aws.dynamodb.Status.READY
    ts_aws.dynamodb.clip.save_clip(clip)
    ts_aws.dynamodb.clip_segment.save_clip_segments(clip_segments)


    logger.info("done")
    return True
