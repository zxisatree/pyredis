from . import commands, data_types, interfaces
from .exceptions import ParseError, UnsupportedOperationError, ValidationError
from .logs import logger


def parse_bytes_into_cmds(
    cmd_bytes: bytes,
) -> tuple[list[interfaces.Command], list[bytes]]:
    final_cmds: list[interfaces.Command] = []
    final_raw_cmds: list[bytes] = []
    pos = 0
    while pos < len(cmd_bytes):
        orig = pos
        try:
            resp_data, pos = data_types.dispatch(cmd_bytes, pos)
        except ParseError:
            logger.error(f"Failed to parse input of length {len(cmd_bytes)}, skipping")
            logger.error(f"{cmd_bytes=}")
            return ([], [])
        final_raw_cmds.append(cmd_bytes[orig:pos])
        logger.info(f"Codec.parse {resp_data=}, {pos=}")
        match resp_data:
            # works like an isinstance, does not actually instantiate new instances every fn call
            case data_types.RespArray():
                final_cmds.append(parse_resp_cmd(resp_data, cmd_bytes, orig, pos))
            case data_types.RespSimpleString():
                # is +FULLRESYNC
                final_cmds.append(commands.FullResyncCommand(resp_data.data))
            case data_types.RespRdbFile():
                final_cmds.append(commands.RdbFileCommand(resp_data.data))
            case _:
                logger.error(
                    f"Unsupported command (is not array) {resp_data}, {type(resp_data)}"
                )
                final_cmds.append(commands.NoOpCommand())
    return final_cmds, final_raw_cmds


def parse_resp_cmd(
    resp_data: data_types.RespArray, cmd: bytes, start: int, end: int
) -> interfaces.Command:
    resp_elements: list[data_types.RespBulkString] = []
    for element in resp_data.elements:
        try:
            resp_elements.append(data_types.RespBulkString.validate(element))
        except ValidationError:
            logger.error(
                f"Unsupported command {cmd[start:end]}, {element} is not a bulk string"
            )
            return commands.NoOpCommand()

    cmd_str = resp_elements[0].data.upper()
    raw_cmd = cmd[start:end]
    if cmd_str == b"PING":
        return commands.PingCommand()
    elif cmd_str == b"ECHO":
        msg = resp_elements[1].data
        return commands.EchoCommand(msg)
    elif cmd_str == b"SET":
        key = resp_elements[1].data
        value = resp_elements[2].data
        if len(resp_data) <= 3:
            return commands.SetCommand(key, value, None)
        px_cmd = resp_elements[3].data
        expiry = resp_elements[4].data
        if px_cmd.upper() != b"PX":
            raise UnsupportedOperationError(
                f"Unsupported SET command (fourth element exists but is not 'PX') {px_cmd}"
            )
        return commands.SetCommand(key, value, expiry)
    elif cmd_str == b"GET":
        key = resp_elements[1].data
        return commands.GetCommand(key)
    elif cmd_str == b"INCR":
        key = resp_elements[1].data
        return commands.IncrCommand(key)
    elif cmd_str == b"COMMAND":
        return commands.CommandCommand()
    elif cmd_str == b"INFO":
        # should check for next word, but only replication is supported
        return commands.InfoCommand()
    elif cmd_str == b"REPLCONF":
        if len(resp_data) >= 3:
            cmd_str2 = resp_elements[1].data
            if cmd_str2.upper() == b"GETACK":
                return commands.ReplConfGetAckCommand()
            elif cmd_str2.upper() == b"ACK":
                return commands.ReplConfAckCommand()
        return commands.ReplConfCommand()
    elif cmd_str == b"WAIT":
        replica_count = resp_elements[1].data
        timeout = resp_elements[2].data
        return commands.WaitCommand(int(replica_count), int(timeout))
    elif cmd_str == b"PSYNC":
        return commands.PsyncCommand()
    elif cmd_str == b"CONFIG":
        key = resp_elements[2].data
        return commands.ConfigGetCommand(key)
    elif cmd_str == b"KEYS":
        pattern = resp_elements[1].data
        return commands.KeysCommand(pattern)
    elif cmd_str == b"TYPE":
        key = resp_elements[1].data
        return commands.TypeCommand(key)
    elif cmd_str == b"MULTI":
        return commands.MultiCommand()
    elif cmd_str == b"EXEC":
        return commands.ExecCommand()
    elif cmd_str == b"DISCARD":
        return commands.DiscardCommand()
    elif cmd_str == b"RPUSH":
        key = resp_elements[1].data
        values = [value.data for value in resp_elements[2:]]
        return commands.RpushCommand(key, values)
    elif cmd_str == b"LPUSH":
        key = resp_elements[1].data
        values = [value.data for value in resp_elements[2:]]
        return commands.LpushCommand(key, values)
    elif cmd_str == b"LPOP":
        key = resp_elements[1].data
        if len(resp_data) == 3:
            try:
                count = int(resp_elements[2].data)
            except ValueError:
                logger.error(
                    f"Tried to LPOP with non int count {resp_elements[2].data}. Defaulting to 0"
                )
                count = 0
        else:
            count = 1
        return commands.LpopCommand(key, count)
    elif cmd_str == b"BLPOP":
        key = resp_elements[1].data
        if len(resp_data) == 3:
            try:
                timeout = float(resp_elements[2].data)
            except ValueError:
                logger.error(
                    f"Tried to BLPOP with non int timeout {resp_elements[2].data}. Defaulting to 0 (indefinite)"
                )
                timeout = 0
        else:
            timeout = 0
        return commands.BlpopCommand(key, timeout)
    elif cmd_str == b"LLEN":
        key = resp_elements[1].data
        return commands.LlenCommand(key)
    elif cmd_str == b"LRANGE":
        key = resp_elements[1].data
        try:
            lrange_start = int(resp_elements[2].data)
            lrange_stop = int(resp_elements[3].data)
        except ValueError:
            logger.error("Tried to LRANGE with non int start/stop. Defaulting to 0, 0")
            lrange_start = 0
            lrange_stop = 0
        return commands.LrangeCommand(key, lrange_start, lrange_stop)
    elif cmd_str == b"ZADD":
        set_key = resp_elements[1].data
        score = resp_elements[2].data
        name = resp_elements[3].data
        try:
            zadd_score = float(score)
        except ValueError:
            logger.error("Tried to ZADD with non float score, defaulting to 0.0")
            zadd_score = 0.0
        return commands.ZaddCommand(set_key, zadd_score, name)
    elif cmd_str == b"ZRANK":
        set_key = resp_elements[1].data
        name = resp_elements[2].data
        return commands.ZrankCommand(set_key, name)
    elif cmd_str == b"ZRANGE":
        set_key = resp_elements[1].data
        zrange_start = resp_elements[2].data
        zrange_end = resp_elements[3].data
        return commands.ZrangeCommand(set_key, int(zrange_start), int(zrange_end))
    elif cmd_str == b"ZCARD":
        set_key = resp_elements[1].data
        return commands.ZcardCommand(set_key)
    elif cmd_str == b"ZSCORE":
        set_key = resp_elements[1].data
        name = resp_elements[2].data
        return commands.ZscoreCommand(set_key, name)
    elif cmd_str == b"ZREM":
        set_key = resp_elements[1].data
        name = resp_elements[2].data
        return commands.ZremCommand(set_key, name)
    elif cmd_str == b"XADD":
        stream_key = resp_elements[1].data
        values = [value.data for value in resp_elements[2:]]
        return commands.XaddCommand(stream_key, values)
    elif cmd_str == b"XRANGE":
        key = resp_elements[1].data
        xrange_start = resp_elements[2].data
        xrange_end = resp_elements[3].data
        return commands.XrangeCommand(
            key,
            xrange_start.decode(),
            xrange_end.decode(),
        )
    elif cmd_str == b"XREAD":
        # first argument should be "streams"
        streams = resp_elements[1].data
        key_id_start_idx = 2
        is_block = False
        if streams.upper() == b"BLOCK":
            is_block = True
            key_id_start_idx = 4
        remaining_len = len(resp_data) - key_id_start_idx
        keys = [
            k.data
            for k in resp_elements[
                key_id_start_idx : key_id_start_idx + remaining_len // 2
            ]
        ]
        ids = [
            i.data.decode()
            for i in resp_elements[key_id_start_idx + remaining_len // 2 :]
        ]
        if is_block:
            timeout = resp_elements[2].data
            return commands.XreadCommand(keys, ids, int(timeout.decode()))
        else:
            return commands.XreadCommand(keys, ids)
    elif cmd_str == b"SUBSCRIBE":
        channel_name = resp_elements[1].data
        return commands.SubscribeCommand(channel_name)
    elif cmd_str == b"UNSUBSCRIBE":
        channel_name = resp_elements[1].data
        return commands.UnsubscribeCommand(channel_name)
    elif cmd_str == b"PUBLISH":
        channel_name = resp_elements[1].data
        msg = resp_elements[2].data
        return commands.PublishCommand(channel_name, msg)
    elif cmd_str == b"GEOADD":
        key = resp_elements[1].data
        longitude = resp_elements[2].data
        latitude = resp_elements[3].data
        member = resp_elements[4].data
        try:
            geoadd_longitude = float(longitude)
            geoadd_latitude = float(latitude)
        except ValueError:
            logger.error(
                "The longitude or latitude provided are not floats, defaulting to 0"
            )
            geoadd_longitude = 0.0
            geoadd_latitude = 0.0
        return commands.GeoaddCommand(key, geoadd_longitude, geoadd_latitude, member)
    elif cmd_str == b"GEOPOS":
        key = resp_elements[1].data
        members = [resp_element.data for resp_element in resp_elements[2:]]
        return commands.GeoposCommand(key, members)
    elif cmd_str == b"GEODIST":
        key = resp_elements[1].data
        place1 = resp_elements[2].data
        place2 = resp_elements[3].data
        return commands.GeodistCommand(key, place1, place2)
    elif cmd_str == b"GEOSEARCH":
        key = resp_elements[1].data
        mode = resp_elements[2].data
        longitude = resp_elements[3].data
        latitude = resp_elements[4].data
        byradius = resp_elements[5].data
        radius = resp_elements[6].data
        unit = resp_elements[7].data
        try:
            geosearch_longitude = float(longitude)
            geosearch_latitude = float(latitude)
            geosearch_radius = float(radius)
        except ValueError:
            logger.error(
                "The longitude/latitude/radius provided are not floats, defaulting to 0"
            )
            geosearch_longitude = 0.0
            geosearch_latitude = 0.0
            geosearch_radius = 0
        return commands.GeosearchCommand(
            key,
            mode,
            geosearch_longitude,
            geosearch_latitude,
            byradius,
            geosearch_radius,
            unit,
        )
    elif cmd_str.startswith(b"REDIS"):
        return commands.RdbFileCommand(raw_cmd)
    elif cmd_str == b"ACL":
        subcmd = resp_elements[1].data.upper()
        if subcmd == b"WHOAMI":
            return commands.AclWhoamiCommand()
        elif subcmd == b"GETUSER":
            user = resp_elements[2].data
            return commands.AclGetuserCommand(user)
        elif subcmd == b"SETUSER":
            user = resp_elements[2].data
            property = resp_elements[3].data
            return commands.AclSetuserCommand(user, property)
        else:
            raise Exception(f"unknown ACL command {raw_cmd=}")
    elif cmd_str == b"AUTH":
        user = resp_elements[1].data
        password = resp_elements[2].data
        return commands.AuthCommand(user, password)
    elif cmd_str == b"WATCH":
        keys = [resp_element.data for resp_element in resp_elements[1:]]
        return commands.WatchCommand(
            keys,
        )
    elif cmd_str == b"UNWATCH":
        return commands.UnwatchCommand()
    else:
        logger.error(f"skipping unknown command {raw_cmd=}")
        return commands.NoOpCommand()
