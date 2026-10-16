import time

import pytest

from app.commands import execute
from app.data import RedisDataStore, RedisError
from app.resp import RESPArray, RESPBulkString, RESPParser


@pytest.fixture
def store(tmp_path):
    return RedisDataStore(dir=str(tmp_path))


def run(store, *args):
    return execute(store, list(args))


def values(reply):
    return [v.value for v in reply.value]


def test_list_order_ranges_and_empty_key(store):
    assert run(store, "LPUSH", "jobs", "a", "b").value == 2
    assert run(store, "RPUSH", "jobs", "c").value == 3
    assert values(run(store, "LRANGE", "jobs", "0", "-1")) == ["b", "a", "c"]
    assert values(run(store, "LRANGE", "jobs", "-2", "-1")) == ["a", "c"]
    assert values(run(store, "LRANGE", "jobs", "-99", "-4")) == []
    assert run(store, "LPOP", "jobs").value == "b"
    assert run(store, "RPOP", "jobs").value == "c"
    assert run(store, "LPOP", "jobs").value == "a"
    assert run(store, "TYPE", "jobs").value == "none"
    assert run(store, "LLEN", "jobs").value == 0
    assert run(store, "LPOP", "jobs").value is None


def test_set_uniqueness_and_deletion(store):
    assert run(store, "SADD", "tags", "python", "redis", "redis").value == 2
    assert run(store, "SADD", "tags", "redis").value == 0
    assert run(store, "SCARD", "tags").value == 2
    assert run(store, "SISMEMBER", "tags", "python").value == 1
    assert set(values(run(store, "SMEMBERS", "tags"))) == {"python", "redis"}
    assert run(store, "SREM", "tags", "python", "python", "missing").value == 1
    assert run(store, "SREM", "tags", "redis").value == 1
    assert run(store, "TYPE", "tags").value == "none"


@pytest.mark.parametrize(
    "command,args", [("LPUSH", ["x"]), ("SADD", ["x"]), ("XADD", ["1-0", "f", "v"])]
)
def test_wrong_type_does_not_replace_string(store, command, args):
    run(store, "SET", "key", "text")
    with pytest.raises(RedisError, match="WRONGTYPE"):
        run(store, command, "key", *args)
    assert run(store, "GET", "key").value == "text"


def test_get_rejects_collections_and_set_replaces_them(store):
    run(store, "LPUSH", "key", "x")
    with pytest.raises(RedisError, match="WRONGTYPE"):
        run(store, "GET", "key")
    run(store, "SET", "key", "0012")
    assert run(store, "GET", "key").value == "0012"
    with pytest.raises(RedisError, match="integer"):
        run(store, "INCR", "key")


def test_expiry_and_integer_boundaries(store, monkeypatch):
    now = time.time()
    monkeypatch.setattr("app.data.time.time", lambda: now)
    run(store, "SET", "key", "old", "PX", "1")
    monkeypatch.setattr("app.data.time.time", lambda: now + 1)
    assert run(store, "SADD", "key", "fresh").value == 1
    run(store, "SET", "counter", "-2")
    assert run(store, "INCR", "counter").value == -1
    run(store, "SET", "counter", str(2**63 - 1))
    with pytest.raises(RedisError, match="overflow"):
        run(store, "INCR", "counter")
    assert run(store, "GET", "counter").value == str(2**63 - 1)


def test_keys_expiry_and_exact_pattern(store):
    store.set("dead", "x", pxat=1)
    store.set("prefix", "x")
    store.set("prefix-long", "x")
    assert store.keys("prefix") == ["prefix"]
    assert set(store.keys("*")) == {"prefix", "prefix-long"}


def test_stream_ids_ranges_count_and_zero_cursor(store, monkeypatch):
    assert run(store, "XADD", "Jobs", "0-*", "state", "queued").value == "0-1"
    assert run(store, "XADD", "Jobs", "100-0", "state", "running").value == "100-0"
    monkeypatch.setattr("app.data.time.time", lambda: 0.05)
    assert run(store, "XADD", "Jobs", "*", "state", "done").value == "100-1"
    entries = run(store, "XRANGE", "Jobs", "-", "+", "COUNT", "2").value
    assert [entry.value[0].value for entry in entries] == ["0-1", "100-0"]
    read = run(store, "XREAD", "STREAMS", "Jobs", "0")
    assert read.value[0].value[0].value == "Jobs"
    assert len(read.value[0].value[1].value) == 3
    assert run(store, "XREAD", "STREAMS", "Jobs", "100-1").serialize() == b"*-1\r\n"


def test_invalid_stream_id_does_not_create_key(store):
    with pytest.raises(RedisError):
        run(store, "XADD", "jobs", "0-0", "f", "v")
    assert run(store, "TYPE", "jobs").value == "none"


def test_utf8_crlf_and_all_fragment_boundaries():
    frame = RESPArray(
        value=[RESPBulkString(value="ECHO"), RESPBulkString(value="Hà Nội\r\n☕")]
    ).serialize()
    for split in range(len(frame) + 1):
        parser = RESPParser()
        results = parser.feed(frame[:split]) + parser.feed(frame[split:])
        assert len(results) == 1
        assert results[0].value[1].value == "Hà Nội\r\n☕"
    assert RESPBulkString(value="☕").serialize() == b"$3\r\n\xe2\x98\x95\r\n"


def test_fragmented_pipeline_and_connection_isolation():
    first, second = RESPParser(), RESPParser()
    assert first.feed(b"*1\r\n$4\r\nPI") == []
    assert second.feed(b"*1\r\n$4\r\nPING\r\n")[0].value[0].value == "PING"
    assert len(first.feed(b"NG\r\n*1\r\n$4\r\nPING\r\n")) == 2


def test_conditional_set_get_returns_old_value_without_write(store):
    applied = []
    assert execute(store, ["SET", "key", "old"], applied).value == "OK"
    applied.clear()
    assert execute(store, ["SET", "key", "new", "NX", "GET"], applied).value == "old"
    assert run(store, "GET", "key").value == "old"
    assert applied == []
    assert execute(store, ["SET", "missing", "new", "XX", "GET"], applied).value is None
    assert run(store, "GET", "missing").value is None
    assert applied == []
