import commands
import data_types
from logs import logger


def parse_cmd(cmd: bytes) -> list[commands.Command]:
    final_cmds: list[commands.Command] = []
    pos = 0
    while pos < len(cmd):
        orig = pos
        resp_data, pos = data_types.dispatch(cmd, pos)
        logger.info(f"Codec.parse {resp_data=}, {pos=}")
        match resp_data:
            # works like an isinstance, does not actually instantiate new instances every fn call
            case data_types.RespArray():
                final_cmds.append(parse_resp_cmd(resp_data, cmd, orig, pos))
            case data_types.RespSimpleString():
                # is +FULLRESYNC
                final_cmds.append(commands.FullResyncCommand(resp_data.data))
            case data_types.RespRdbFile():
                final_cmds.append(commands.RdbFileCommand(resp_data.data.data))
            case _:
                logger.error(
                    f"Unsupported command (is not array) {resp_data}, {type(resp_data)}"
                )
                final_cmds.append(commands.NoOp(cmd[orig:pos]))
    return final_cmds


def parse_resp_cmd(
    resp_data: data_types.RespArray, cmd: bytes, start: int, end: int
) -> commands.Command:
    resp_elements: list[data_types.RespBulkString] = []
    for element in resp_data.elements:
        result = data_types.RespBulkString.safe_validate(element)
        if result[1] is not None:
            logger.error(
                f"Unsupported command {cmd[start:end]}, {element} is not a bulk string"
            )
            return commands.NoOp(cmd[start:end])
        resp_elements.append(result[0])

    cmd_str = resp_elements[0].data.upper()
    raw_cmd = cmd[start:end]
    if cmd_str == b"PING":
        return commands.PingCommand(raw_cmd)
    elif cmd_str == b"ECHO":
        msg = resp_elements[1]
        return commands.EchoCommand(raw_cmd, msg)
    elif cmd_str == b"SET":
        key = resp_elements[1]
        value = resp_elements[2]
        if len(resp_data) <= 3:
            return commands.SetCommand(raw_cmd, key, value, None)
        px_cmd = resp_elements[3]
        expiry = resp_elements[4]
        commands.SetCommand.validate_px(px_cmd)
        return commands.SetCommand(raw_cmd, key, value, expiry)
    elif cmd_str == b"GET":
        key = resp_elements[1]
        return commands.GetCommand(raw_cmd, key.data)
    elif cmd_str == b"INCR":
        key = resp_elements[1]
        return commands.IncrCommand(raw_cmd, key.data)
    elif cmd_str == b"COMMAND":
        return commands.CommandCommand(raw_cmd)
    elif cmd_str == b"INFO":
        # should check for next word, but only replication is supported
        return commands.InfoCommand(raw_cmd)
    elif cmd_str == b"REPLCONF":
        if len(resp_data) >= 3:
            cmd_str2 = resp_elements[1]
            if cmd_str2.data.upper() == b"GETACK":
                return commands.ReplConfGetAckCommand(raw_cmd)
            elif cmd_str2.data.upper() == b"ACK":
                return commands.ReplConfAckCommand(raw_cmd)
        return commands.ReplConfCommand(raw_cmd)
    elif cmd_str == b"WAIT":
        replica_count = resp_elements[1]
        timeout = resp_elements[2]
        return commands.WaitCommand(raw_cmd, int(replica_count.data), int(timeout.data))
    elif cmd_str == b"PSYNC":
        return commands.PsyncCommand(raw_cmd)
    elif cmd_str == b"CONFIG":
        key = resp_elements[2]
        return commands.ConfigGetCommand(raw_cmd, key.data)
    elif cmd_str == b"KEYS":
        pattern = resp_elements[1]
        return commands.KeysCommand(raw_cmd, pattern.data)
    elif cmd_str == b"TYPE":
        key = resp_elements[1]
        return commands.TypeCommand(raw_cmd, key.data)
    elif cmd_str == b"MULTI":
        return commands.MultiCommand(raw_cmd)
    elif cmd_str == b"EXEC":
        return commands.ExecCommand(raw_cmd)
    elif cmd_str == b"DISCARD":
        return commands.DiscardCommand(raw_cmd)
    elif cmd_str == b"RPUSH":
        key = resp_elements[1]
        values = [value.data for value in resp_elements[2:]]
        return commands.RpushCommand(raw_cmd, key.data, values)
    elif cmd_str == b"LPUSH":
        key = resp_elements[1]
        values = [value.data for value in resp_elements[2:]]
        return commands.LpushCommand(raw_cmd, key.data, values)
    elif cmd_str == b"LPOP":
        key = resp_elements[1]
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
        return commands.LpopCommand(raw_cmd, key.data, count)
    elif cmd_str == b"BLPOP":
        key = resp_elements[1]
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
        return commands.BlpopCommand(raw_cmd, key.data, timeout)
    elif cmd_str == b"LLEN":
        key = resp_elements[1]
        return commands.LlenCommand(raw_cmd, key.data)
    elif cmd_str == b"LRANGE":
        key = resp_elements[1]
        try:
            lrange_start = int(resp_elements[2].data)
            lrange_stop = int(resp_elements[3].data)
        except ValueError:
            logger.error("Tried to LRANGE with non int start/stop. Defaulting to 0, 0")
            lrange_start = 0
            lrange_stop = 0
        return commands.LrangeCommand(raw_cmd, key.data, lrange_start, lrange_stop)
    elif cmd_str == b"ZADD":
        set_key = resp_elements[1]
        score = resp_elements[2]
        name = resp_elements[3]
        try:
            zadd_score = float(score.data)
        except ValueError:
            logger.error("Tried to ZADD with non float score, defaulting to 0.0")
            zadd_score = 0.0
        return commands.ZaddCommand(raw_cmd, set_key.data, zadd_score, name.data)
    elif cmd_str == b"ZRANK":
        set_key = resp_elements[1]
        name = resp_elements[2]
        return commands.ZrankCommand(raw_cmd, set_key.data, name.data)
    elif cmd_str == b"ZRANGE":
        set_key = resp_elements[1]
        zrange_start = resp_elements[2]
        zrange_end = resp_elements[3]
        return commands.ZrangeCommand(
            raw_cmd, set_key.data, int(zrange_start.data), int(zrange_end.data)
        )
    elif cmd_str == b"ZCARD":
        set_key = resp_elements[1]
        return commands.ZcardCommand(raw_cmd, set_key.data)
    elif cmd_str == b"ZSCORE":
        set_key = resp_elements[1]
        name = resp_elements[2]
        return commands.ZscoreCommand(raw_cmd, set_key.data, name.data)
    elif cmd_str == b"ZREM":
        set_key = resp_elements[1]
        name = resp_elements[2]
        return commands.ZremCommand(raw_cmd, set_key.data, name.data)
    elif cmd_str == b"XADD":
        stream_key = resp_elements[1]
        return commands.XaddCommand(raw_cmd, stream_key.data, resp_elements[2:])
    elif cmd_str == b"XRANGE":
        key = resp_elements[1]
        xrange_start = resp_elements[2]
        xrange_end = resp_elements[3]
        return commands.XrangeCommand(
            raw_cmd,
            key.data,
            xrange_start.data.decode(),
            xrange_end.data.decode(),
        )
    elif cmd_str == b"XREAD":
        # first argument should be "streams"
        streams = resp_elements[1]
        key_id_start_idx = 2
        is_block = False
        if streams.data.upper() == b"BLOCK":
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
            timeout = resp_elements[2]
            return commands.XreadCommand(raw_cmd, keys, ids, int(timeout.data.decode()))
        else:
            return commands.XreadCommand(raw_cmd, keys, ids)
    elif cmd_str == b"SUBSCRIBE":
        channel_name = resp_elements[1].data
        return commands.SubscribeCommand(raw_cmd, channel_name)
    elif cmd_str == b"UNSUBSCRIBE":
        channel_name = resp_elements[1].data
        return commands.UnsubscribeCommand(raw_cmd, channel_name)
    elif cmd_str == b"PUBLISH":
        channel_name = resp_elements[1].data
        msg = resp_elements[2].data
        return commands.PublishCommand(raw_cmd, channel_name, msg)
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
        return commands.GeoaddCommand(
            raw_cmd, key, geoadd_longitude, geoadd_latitude, member
        )
    elif cmd_str == b"GEOPOS":
        key = resp_elements[1].data
        members = [resp_element.data for resp_element in resp_elements[2:]]
        return commands.GeoposCommand(raw_cmd, key, members)
    elif cmd_str == b"GEODIST":
        key = resp_elements[1].data
        place1 = resp_elements[2].data
        place2 = resp_elements[3].data
        return commands.GeodistCommand(raw_cmd, key, place1, place2)
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
            raw_cmd,
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
            return commands.AclWhoamiCommand(raw_cmd)
        elif subcmd == b"GETUSER":
            user = resp_elements[2].data
            return commands.AclGetuserCommand(raw_cmd, user)
        elif subcmd == b"SETUSER":
            user = resp_elements[2].data
            property = resp_elements[3].data
            # properties = [resp_element.data for resp_element in resp_elements[3:]]
            return commands.AclSetuserCommand(raw_cmd, user, property)
        else:
            raise Exception(f"unknown ACL command {raw_cmd=}")
    elif cmd_str == b"AUTH":
        user = resp_elements[1].data
        password = resp_elements[2].data
        return commands.AuthCommand(raw_cmd, user, password)
    elif cmd_str == b"WATCH":
        keys = [resp_element.data for resp_element in resp_elements[1:]]
        return commands.WatchCommand(
            raw_cmd,
            keys,
        )
    elif cmd_str == b"UNWATCH":
        return commands.UnwatchCommand(raw_cmd)
    else:
        raise Exception(f"skipping unknown command {raw_cmd=}")
