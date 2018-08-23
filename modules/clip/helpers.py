import ts_aws.dynamodb.clip

def get_packets_data(packets, is_first_cs, is_last_cs, time_in, time_out):
    last_keyframe_index = None
    packet_start_index = None
    packet_end_index = None
    for index, packet in enumerate(packets):
        pts_time = float(packet['pts_time'])
        if "K" in packet['flags']:
            last_keyframe_index = index
        if packet_start_index is None:
            if (is_first_cs and time_in <= pts_time) or (not is_first_cs and is_last_cs):
                packet_start_index = last_keyframe_index
        if packet_end_index is None:
            if time_out <= pts_time:
                packet_end_index = index
                break

    if packet_start_index is None:
        packet_start_index = last_keyframe_index
    if packet_end_index is None:
        packet_end_index = len(packets) - 1
    packet_start = packets[packet_start_index]
    packet_end = packets[packet_end_index]

    time_in = float(packet_start['pts_time'])
    time_out = float(packet_end['pts_time'])
    duration = time_out - time_in
    byterange = int(packet_end['pos']) + int(packet_end['size']) - int(packet_start['pos'])
    pos = int(packet_start['pos'])
    return (duration, time_in, time_out, pos, byterange)
