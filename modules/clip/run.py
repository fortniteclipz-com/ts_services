import ts_aws.dynamodb.clip
import ts_aws.dynamodb.stream
import ts_aws.s3
import ts_aws.sqs
import ts_config
import ts_file
import ts_logger
import ts_media
import ts_twitch
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
    logger.set(clip=clip.__dict__)

    # init/get stream and stream segments
    logger.info("checking stream and stream_segments")
    stream = ts_aws.dynamodb.stream.get_stream(clip.stream_id)
    stream_segments = ts_aws.dynamodb.stream.get_stream_segments(clip.stream_id)
    if not stream.is_init() or len(stream_segments) == 0:
        logger.info("init stream and stream_segments")
        (stream, stream_segments) = ts_twitch.initialize_stream(clip.stream_id)
        stream = ts_aws.dynamodb.stream.save_stream(stream)
        stream_segments = ts_aws.dynamodb.stream.save_stream_segments(stream_segments)
    logger.info("stream", stream=stream.__dict__)
    logger.info("stream_segments", stream_segments_length=len(stream_segments))

    # init/get clip segments
    logger.info("checking clip_segments")
    clip_segments = ts_aws.dynamodb.clip.get_clip_segments(clip.clip_id)
    if len(clip_segments) == 0:
        logger.info("init clip_segments")
        clip_segments = []
        clip_time_in_offset = clip.time_in + stream.time_offset
        clip_time_out_offset = clip.time_out + stream.time_offset
        for ss in stream_segments:
            if ss.time_in <= clip_time_out_offset and ss.time_out >= clip_time_in_offset:
                cs = ts_aws.dynamodb.clip.ClipSegment(
                    clip_id=clip.clip_id,
                    segment=ss.segment,
                )
                clip_segments.append(cs)
    logger.info("clip_segments", in_segment=clip_segments[0].segment, out_segment=clip_segments[len(clip_segments) - 1].segment)

    # update clip segments
    logger.info("ingesting clip_semgents")
    ready_to_clip = True
    i_ss = next((i for i, ss in enumerate(stream_segments) if ss.segment == clip_segments[0].segment), -1)
    for i_cs, cs in enumerate(clip_segments):
        ss = stream_segments[i_ss + i_cs]
        is_first_cs = True if i_cs == 0 else False

        if not ss.is_init_raw() or (is_first_cs and not ss.is_init_fresh()):
            logger.info("stream_segment needs ingesting", segment=ss.segment)
            ready_to_clip = False
            payload = {
                'stream_id': ss.stream_id,
                'segment': ss.segment,
                'fresh': is_first_cs,
            }
            logger.info("pushing to stream_download sqs", payload=payload)
            ts_aws.sqs.send_stream_download(payload)

        elif cs.is_init():
            logger.info("clip_semgent already ingested", segment=cs.segment)
            continue

        else:
            logger.info("ingesting clip_semgent", segment=cs.segment)
            is_last_cs = True if i_cs == (len(clip_segments) - 1) else False

            bucket = ts_config.get('aws.s3.main.name')
            region = ts_config.get('aws.s3.main.region')
            url_media_prefix = F"https://s3-{region}.amazonaws.com/{bucket}"
            video_url_media = ss.key_media_video_fresh if is_first_cs else ss.key_media_video
            cs.video_url_media = f"{url_media_prefix}/{video_url_media}"
            cs.audio_url_media = f"{url_media_prefix}/{ss.key_media_audio}"

            if is_first_cs is False and is_last_cs is False:
                cs.video_time_in = ss.time_in
                cs.video_time_out = ss.time_out
                cs.video_time_duration = ss.time_duration
                cs.audio_time_in = ss.time_in
                cs.audio_time_out = ss.time_out
                cs.audio_time_duration = ss.time_duration
            else:
                packets_filename_video = f"/tmp/{ss.padded}_video.json"
                packets_filename_audio = f"/tmp/{ss.padded}_audio.json"
                packets_key_video = ss.key_packets_video_fresh if is_first_cs else ss.key_packets_video
                ts_aws.s3.download_file(packets_key_video, packets_filename_video)
                ts_aws.s3.download_file(ss.key_packets_audio, packets_filename_audio)

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

    # save clips segments to dynamodb
    logger.info("saving clip_segments")
    clip_segments = ts_aws.dynamodb.clip.save_clip_segments(clip_segments)

    if not ready_to_clip:
        logger.error("clip_segments not ready to clip")
        raise Exception("Not all clip segments processed yet")

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

    logger.info("saving clip")
    clip.key_playlist_master = m3u8_key_master
    clip.key_playlist_video = m3u8_key_video
    clip.key_playlist_audio = m3u8_key_audio
    clip = ts_aws.dynamodb.clip.save_clip(clip)

    logger.info("done")
    return True
