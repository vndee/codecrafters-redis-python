"""Synchronous command execution shared by ordinary requests and EXEC.

No command in this module may await: an EXEC batch must finish its in-memory
mutations before another client gets a turn on the event loop.
"""
from app.data import RedisError
from app.resp import RESPArray, RESPBulkString, RESPSimpleString

# Argument counts include the command name; None means no upper bound.
ARITIES = {
    "ping": (1, 2),
    "echo": (2, 2),
    "set": (3, None),
    "get": (2, 2),
    "keys": (2, 2),
    "type": (2, 2),
    "incr": (2, 2),
    "lpush": (3, None),
    "rpush": (3, None),
    "lpop": (2, 2),
    "rpop": (2, 2),
    "llen": (2, 2),
    "lrange": (4, 4),
    "sadd": (3, None),
    "srem": (3, None),
    "sismember": (3, 3),
    "scard": (2, 2),
    "smembers": (2, 2),
    "xadd": (5, None),
    "xrange": (4, 6),
    "xread": (4, None),
    "multi": (1, 1),
    "exec": (1, 1),
    "discard": (1, 1),
}
DATA_COMMANDS = set(ARITIES) - {"multi", "exec", "discard"}


def validate(args):
    command = args[0].lower()
    if command not in ARITIES:
        raise RedisError("ERR unknown command")
    low, high = ARITIES[command]
    if len(args) < low or (high is not None and len(args) > high):
        raise RedisError(f"ERR wrong number of arguments for '{command}' command")
    if command == "xadd" and (len(args) - 3) % 2:
        raise RedisError("ERR wrong number of arguments for 'xadd' command")


def integer(value):
    try:
        # Unlike int(), Redis rejects surrounding whitespace and plus signs.
        if value != str(int(value)):
            raise ValueError
        number = int(value)
        if not -(2**63) <= number < 2**63:
            raise ValueError
        return number
    except ValueError:
        raise RedisError("ERR value is not an integer or out of range")


def xread_options(store, args):
    i, block, count = 1, None, None
    while i < len(args) and args[i].lower() != "streams":
        option = args[i].lower()
        if option not in {"block", "count"} or i + 1 >= len(args):
            raise RedisError("ERR syntax error")
        value = integer(args[i + 1])
        if value < 0 or (option == "count" and value == 0):
            raise RedisError("ERR invalid XREAD option value")
        if option == "block":
            block = value
        else:
            count = value
        i += 2
    remaining = args[i + 1 :]
    if i >= len(args) or not remaining or len(remaining) % 2:
        raise RedisError("ERR Unbalanced XREAD list of streams")
    middle = len(remaining) // 2
    keys, ids = remaining[:middle], remaining[middle:]
    ids = [
        store.get_current_max_stream_id(key) if id == "$" else id
        for key, id in zip(keys, ids)
    ]
    if any(id in {"-", "+"} for id in ids):
        raise RedisError("ERR Invalid stream ID specified as stream command argument")
    for id in ids:
        store.stream_id(id)
    return keys, ids, count, block


def execute(store, args, applied_sets=None):
    validate(args)
    command = args[0].lower()
    if command == "ping":
        return (
            RESPBulkString(value=args[1])
            if len(args) == 2
            else RESPSimpleString(value="PONG")
        )
    if command == "echo":
        return RESPBulkString(value=args[1])
    key = args[1]
    if command == "set":
        options = {}
        i = 3
        while i < len(args):
            name = args[i].lower()
            if name in {"ex", "px", "exat", "pxat"}:
                if i + 1 >= len(args) or any(
                    k in options for k in ("ex", "px", "exat", "pxat", "keepttl")
                ):
                    raise RedisError("ERR syntax error")
                options[name] = integer(args[i + 1])
                if options[name] <= 0:
                    raise RedisError("ERR invalid expire time in 'set' command")
                i += 2
            elif name in {"nx", "xx", "get", "keepttl"}:
                if name == "keepttl" and any(
                    k in options for k in ("ex", "px", "exat", "pxat")
                ):
                    raise RedisError("ERR syntax error")
                options[name] = True
                i += 1
            else:
                raise RedisError("ERR syntax error")
        if options.get("nx") and options.get("xx"):
            raise RedisError("ERR syntax error")
        result, applied = store.set(key, args[2], return_applied=True, **options)
        if applied and applied_sets is not None:
            applied_sets.append(args)
        return (
            RESPBulkString(value=None if result is None else str(result))
            if options.get("get") or result is None
            else RESPSimpleString(value="OK")
        )
    if command == "get":
        return store.get(key)
    if command == "keys":
        return RESPArray(value=[RESPBulkString(value=v) for v in store.keys(key)])
    if command == "type":
        return RESPSimpleString(value=store.type(key))
    if command == "incr":
        return store.incr(key)
    if command in {"lpush", "rpush"}:
        return store.list_push(key, args[2:], left=command == "lpush")
    if command in {"lpop", "rpop"}:
        return store.list_pop(key, left=command == "lpop")
    if command == "llen":
        return store.list_len(key)
    if command == "lrange":
        return store.list_range(key, integer(args[2]), integer(args[3]))
    if command == "sadd":
        return store.set_add(key, args[2:])
    if command == "srem":
        return store.set_remove(key, args[2:])
    if command == "smembers":
        return store.set_members(key)
    if command == "scard":
        return store.set_card(key)
    if command == "sismember":
        return store.set_ismember(key, args[2])
    if command == "xadd":
        return store.xadd(key, args[2], [RESPBulkString(value=v) for v in args[3:]])
    if command == "xrange":
        count = None
        if len(args) != 4:
            if len(args) != 6 or args[4].lower() != "count":
                raise RedisError("ERR syntax error")
            count = integer(args[5])
            if count <= 0:
                raise RedisError("ERR COUNT must be positive")
        return store.xrange(key, args[2], args[3], count)
    if command == "xread":
        keys, ids, count, _ = xread_options(store, args)
        return store.xread(keys, ids, count)
    raise RedisError("ERR unsupported command in synchronous executor")
