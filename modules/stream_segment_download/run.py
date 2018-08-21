import ts_aws.dynamodb.stream_segment
import ts_aws.s3
import ts_file
import ts_http
import ts_logger
import ts_media

import os
import glob
import json

ts_media.init_ff_libs()
logger = ts_logger.get(__name__)

def run(event, context):
    logger.info("start", event=event, context=context)
    body = json.loads(event['Records'][0]['body'])
    logger.info("body", body=body)
    stream_id = body['stream_id']
    segment = body['segment']
    fresh = body['fresh']

    # get stream_segment from dynamodb
    ss = ts_aws.dynamodb.stream_segment.get_stream_segment(stream_id, segment)
    logger.info("stream_segment", stream_segment=ss.__dict__)

    # check if stream_segment is processed raw/fresh else do nothing
    if not ss.is_init_raw() or (fresh and not ss.is_init_fresh()):
        media_filename_video = f"/tmp/{ss.padded}_video.ts"
        media_key_video = f"streams/{stream_id}/raw/video/{ss.padded}.ts"

        # process raw segment if not processed else get from s3
        if not ss.is_init_raw():
            logger.info("downloading and processing raw segment")
            media_filename = f"/tmp/{ss.padded}_raw.ts"
            media_filename_audio = f"/tmp/{ss.padded}_audio.ts"
            packets_filename_video = f"/tmp/{ss.padded}_video.json"
            packets_filename_audio = f"/tmp/{ss.padded}_audio.json"
            ts_http.download_file(ss.url_media_raw, media_filename)
            ts_media.split_media_video(media_filename, media_filename_video)
            ts_media.split_media_audio(media_filename, media_filename_audio)
            ts_media.probe_media_video(media_filename_video, packets_filename_video)
            ts_media.probe_media_audio(media_filename_audio, packets_filename_audio)

            logger.info("uploading raw segment")
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

        else:
            logger.info("getting raw segment from s3")
            ts_aws.s3.download_file(media_key_video, media_filename_video)

        # process thumbnails from raw segment
        logger.info("thumbnailing raw segment")
        ts_media.thumbnail_media_video(media_filename_video, "/tmp/thumb%06d.jpg")
        for thumbnail_filename in glob.glob("/tmp/*.jpg"):
            f = os.path.basename(thumbnail_filename)
            thumbnail_key = f"streams/{stream_id}/{ss.padded}/{f}"
            ts_aws.s3.upload_file_thumbnails(thumbnail_filename, thumbnail_key)
            os.remove(thumbnail_filename)

        # process fresh segment if not processed
        if fresh and not ss.is_init_fresh():
            logger.info("freshing raw segment")
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

        ts_file.delete(media_filename_video)

        logger.info("updating db")
        ts_aws.dynamodb.stream_segment.save_stream_segment(ss)
    else:
        logger.warn("already downloaded")

    logger.info("done", stream_segment=ss.__dict__)
    return True
