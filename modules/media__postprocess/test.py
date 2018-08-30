import run
import json

events = [{
    'version': '0',
    'id': '1bd45bb8-c272-c719-249b-41faf90ca6e5',
    'detail-type': 'MediaConvert Job State Change',
    'source': 'aws.mediaconvert',
    'account': '589344262905',
    'time': '2018-08-28T08:32:40Z',
    'region': 'us-west-1',
    'resources': ['arn:aws:mediaconvert:us-west-1:589344262905:jobs/1535445152005-uwqnsy'],
    'detail':
    {
        'timestamp': 1535445160581,
        'accountId': '589344262905',
        'queue': 'arn:aws:mediaconvert:us-west-1:589344262905:queues/Default',
        'jobId': '1535445152005-uwqnsy',
        'status': 'COMPLETE',
        'userMetadata':
        {
            'clip_id': 'c-NibQGRzhJ6ZeKktGezP4Cg'
        },
        'outputGroupDetails': [
        {
            'outputDetails': [
            {
                'durationInMs': 6673,
                'videoDetails':
                {
                    'widthInPx': 1280,
                    'heightInPx': 720
                }
            }],
            'type': 'FILE_GROUP'
        }]
    }
}, {
    'version': '0',
    'id': 'f7ebd71a-74fa-db35-dc21-333d9c22452c',
    'detail-type': 'MediaConvert Job State Change',
    'source': 'aws.mediaconvert',
    'account': '589344262905',
    'time': '2018-08-28T08:36:47Z',
    'region': 'us-west-1',
    'resources': ['arn:aws:mediaconvert:us-west-1:589344262905:jobs/1535445400397-i70asm'],
    'detail':
    {
        'timestamp': 1535445407020,
        'accountId': '589344262905',
        'queue': 'arn:aws:mediaconvert:us-west-1:589344262905:queues/Default',
        'jobId': '1535445400397-i70asm',
        'status': 'ERROR',
        'errorCode': 1404,
        'errorMessage': 'Unable to open input file [s3://twitch-stitch-main/clips/c-zbFvt88cty9VhkpKqHVLDo/playlist-video.m3u8]: [Failed probe/open: [HLS reader failed to create HLS manifest: Failed to read data: Input not found at s3://twitch-stitch-main/clips/c-zbFvt88cty9VhkpKqHVLDo/playlist-video.m3u8]]',
        'userMetadata':
        {
            'montage_id': 'm-TqycrB6MvyKzwpXmBAFE5E'
        }
    }
}]

for event in events:
    run.run(event, {})
