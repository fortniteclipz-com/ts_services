import ts_aws.dynamodb.stream_segment
import ts_aws.s3
import ts_file
import ts_http
import ts_logger
import ts_media
import ts_model.Status

import json

ts_media.init_ff_libs()
logger = ts_logger.get(__name__)

def run(event, context):
    logger.info("start", event=event, context=context)
    body = json.loads(event['Records'][0]['body'])
    logger.info("body", body=body)
    stream_id = body['stream_id']
    segment = body['segment']

    # get stream_segment from dynamodb
    ss = ts_aws.dynamodb.stream_segment.get_stream_segment(stream_id, segment)

    media_filename_video = f"/tmp/{ss.padded}_video.ts"
    media_key_video = f"streams/{stream_id}/raw/video/{ss.padded}.ts"
    download_raw = ss._status_download == ts_model.Status.INITIALIZING
    download_fresh = ss._status_fresh == ts_model.Status.INITIALIZING

    if not download_fresh and not download_raw:
        logger.error(f"stream_segment already processed", stream_segment=ss.__dict__)
        return

    if download_raw:
        media_filename = f"/tmp/{ss.padded}_raw.ts"
        media_filename_audio = f"/tmp/{ss.padded}_audio.ts"
        packets_filename_video = f"/tmp/{ss.padded}_video.json"
        packets_filename_audio = f"/tmp/{ss.padded}_audio.json"
        ts_http.download_file(ss.url_media_raw, media_filename)
        ts_media.split_media_video(media_filename, media_filename_video)
        ts_media.split_media_audio(media_filename, media_filename_audio)
        ts_media.probe_media_video(media_filename_video, packets_filename_video)
        ts_media.probe_media_audio(media_filename_audio, packets_filename_audio)

        media_key_audio = f"streams/{stream_id}/raw/audio/{ss.padded}.ts"
        packets_key_video = f"streams/{stream_id}/raw/meta/video/{ss.padded}.json"
        packets_key_audio = f"streams/{stream_id}/raw/meta/audio/{ss.padded}.json"
        ts_aws.s3.upload_file(media_filename_video, media_key_video)
        ts_aws.s3.upload_file(media_filename_audio, media_key_audio)
        ts_aws.s3.upload_file(packets_filename_video, packets_key_video)
        ts_aws.s3.upload_file(packets_filename_audio, packets_key_audio)

        ss.key_media_video = media_key_video
        ss.key_packets_video = packets_key_video
        ss.key_media_audio = media_key_audio
        ss.key_packets_audio = packets_key_audio

        ts_file.delete(media_filename)
        ts_file.delete(media_filename_audio)
        ts_file.delete(packets_filename_video)
        ts_file.delete(packets_filename_audio)
        ss._status_download = ts_model.Status.READY

    else:
        ts_aws.s3.download_file(media_key_video, media_filename_video)

    # process fresh segment if not processed
    if download_fresh:
        media_filename_video_fresh = f"/tmp/{ss.padded}_video_fresh.ts"
        packets_filename_video_fresh = f"/tmp/{ss.padded}_video_fresh.json"
        gop = ts_media.calculate_gop(media_filename_video)
        ts_media.fresh_media_video(gop, media_filename_video, media_filename_video_fresh)
        ts_media.probe_media_video(media_filename_video_fresh, packets_filename_video_fresh)

        media_key_video_fresh = f"streams/{stream_id}/fresh/video/{ss.padded}.ts"
        packets_key_video_fresh = f"streams/{stream_id}/fresh/meta/video/{ss.padded}.json"
        ts_aws.s3.upload_file(media_filename_video_fresh, media_key_video_fresh),
        ts_aws.s3.upload_file(packets_filename_video_fresh, packets_key_video_fresh),
        ss.key_media_video_fresh = media_key_video_fresh
        ss.key_packets_video_fresh = packets_key_video_fresh

        ts_file.delete(media_filename_video_fresh)
        ts_file.delete(packets_filename_video_fresh)
        ss._status_fresh = ts_model.Status.READY

    ts_file.delete(media_filename_video)
    ts_aws.dynamodb.stream_segment.save_stream_segment(ss)

    logger.info("done")
    return True
