import asyncio
from contextlib import asynccontextmanager

from app.resp import RESPArray, RESPBulkString
from app.server import RedisServer


def encode(*args):
    return RESPArray(value=[RESPBulkString(value=str(v)) for v in args]).serialize()


async def read(reader):
    header = await reader.readuntil(b"\r\n")
    prefix, value = header[:1], header[1:-2]
    if prefix == b"+":
        return value.decode()
    if prefix == b"-":
        return ("error", value.decode())
    if prefix == b":":
        return int(value)
    if prefix == b"$":
        length = int(value)
        if length == -1:
            return None
        data = await reader.readexactly(length + 2)
        assert data[-2:] == b"\r\n"
        return data[:-2].decode()
    if prefix == b"*":
        length = int(value)
        return None if length == -1 else [await read(reader) for _ in range(length)]
    raise AssertionError(header)


class Client:
    def __init__(self, reader, writer):
        self.reader, self.writer = reader, writer

    async def command(self, *args):
        self.writer.write(encode(*args))
        await self.writer.drain()
        return await asyncio.wait_for(read(self.reader), 2)


@asynccontextmanager
async def running(tmp_path):
    redis = RedisServer(dir=str(tmp_path))
    server = await asyncio.start_server(redis.handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    clients = [
        Client(*await asyncio.open_connection("127.0.0.1", port)) for _ in range(2)
    ]
    try:
        yield redis, *clients
    finally:
        for client in clients:
            client.writer.close()
        for client in clients:
            await client.writer.wait_closed()
        server.close()
        await server.wait_closed()
        await asyncio.sleep(0.02)


def test_queue_exec_runtime_error_and_discard(tmp_path):
    async def scenario():
        async with running(tmp_path) as (_, a, b):
            assert await a.command("MULTI") == "OK"
            assert await a.command("SET", "count", "1") == "QUEUED"
            assert await a.command("LPUSH", "count", "bad") == "QUEUED"
            assert await a.command("INCR", "count") == "QUEUED"
            assert await b.command("GET", "count") is None
            results = await a.command("EXEC")
            assert results[0] == "OK"
            assert results[1][0] == "error" and results[1][1].startswith("WRONGTYPE")
            assert results[2] == 2
            assert await b.command("GET", "count") == "2"
            assert await a.command("MULTI") == "OK"
            assert await a.command("SET", "count", "99") == "QUEUED"
            assert await a.command("DISCARD") == "OK"
            assert await a.command("GET", "count") == "2"

    asyncio.run(scenario())


def test_queue_errors_abort_and_nested_multi_preserves_queue(tmp_path):
    async def scenario():
        async with running(tmp_path) as (_, a, b):
            for bad in [("NO_SUCH_COMMAND",), ("SET", "only-key")]:
                assert await a.command("MULTI") == "OK"
                assert await a.command("SET", "key", "value") == "QUEUED"
                error = await a.command(*bad)
                assert error[0] == "error"
                aborted = await a.command("EXEC")
                assert aborted[0] == "error" and aborted[1].startswith("EXECABORT")
                assert await b.command("GET", "key") is None
            assert await a.command("MULTI") == "OK"
            assert await a.command("SET", "key", "value") == "QUEUED"
            assert (await a.command("MULTI"))[0] == "error"
            assert await a.command("EXEC") == ["OK"]
            assert (await a.command("EXEC"))[0] == "error"

    asyncio.run(scenario())


def test_exec_mutations_do_not_interleave_across_network_awaits(tmp_path):
    async def scenario():
        async with running(tmp_path) as (server, a, b):
            propagation_started = asyncio.Event()

            async def slow_replication(data):
                propagation_started.set()
                await asyncio.sleep(0.03)

            server._RedisServer__propagate_to_slaves = slow_replication
            await a.command("MULTI")
            await a.command("SET", "state", "intermediate")
            await a.command("SET", "state", "complete")
            execution = asyncio.create_task(a.command("EXEC"))
            await asyncio.wait_for(propagation_started.wait(), 1)
            assert await b.command("GET", "state") == "complete"
            assert await execution == ["OK", "OK"]

    asyncio.run(scenario())


def test_xread_wakes_early_preserves_key_case_and_does_not_block_exec(tmp_path):
    async def scenario():
        async with running(tmp_path) as (_, a, b):
            waiting = asyncio.create_task(
                a.command("XREAD", "BLOCK", "10000", "STREAMS", "Jobs", "$")
            )
            await asyncio.sleep(0.02)
            assert await b.command("XADD", "Jobs", "1-0", "state", "ready") == "1-0"
            assert await asyncio.wait_for(waiting, 1) == [
                ["Jobs", [["1-0", ["state", "ready"]]]]
            ]
            assert (
                await a.command("XREAD", "BLOCK", "1", "STREAMS", "Jobs", "1-0") is None
            )
            await a.command("MULTI")
            await a.command("XREAD", "BLOCK", "0", "STREAMS", "Jobs", "$")
            await a.command("PING")
            assert await a.command("EXEC") == [None, "PONG"]

    asyncio.run(scenario())


def test_fragmented_wire_and_syntax_errors_do_not_hang(tmp_path):
    async def scenario():
        async with running(tmp_path) as (_, a, b):
            packet = encode("ECHO", "Hà Nội\r\n☕")
            a.writer.write(packet[:17])
            await a.writer.drain()
            assert await b.command("PING") == "PONG"
            a.writer.write(packet[17:] + encode("PING"))
            await a.writer.drain()
            assert await asyncio.wait_for(read(a.reader), 1) == "Hà Nội\r\n☕"
            assert await asyncio.wait_for(read(a.reader), 1) == "PONG"
            assert (await a.command("SET", "x", "y", "invalid-option"))[0] == "error"
            assert await a.command("PING") == "PONG"

    asyncio.run(scenario())


def test_conditional_set_noop_is_not_propagated(tmp_path):
    async def scenario():
        async with running(tmp_path) as (server, a, _):
            propagated = []

            async def capture(data):
                propagated.append([part.value for part in data.value])

            server._RedisServer__propagate_to_slaves = capture
            await a.command("SET", "key", "old")
            propagated.clear()
            assert await a.command("SET", "key", "new", "NX", "GET") == "old"
            assert propagated == []
            await a.command("MULTI")
            await a.command("SET", "key", "new", "NX")
            await a.command("SET", "missing", "new", "XX")
            assert await a.command("EXEC") == [None, None]
            assert propagated == []

    asyncio.run(scenario())
