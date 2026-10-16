# Build your own Redis in Python

A teaching server for the [CodeCrafters Redis challenge](https://codecrafters.io/challenges/redis)
and the [Build your own Redis series](https://blog.duy.dev/build-your-own-redis-1-5-redis-serialization-protocol-and-concurrency-model/).
Requires Python 3.11 or newer. This is a learning implementation, not a production Redis replacement.

## Run and test

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install pytest
python -m app.server --host 127.0.0.1 --port 6379
```

From a second terminal:

```sh
redis-cli RPUSH jobs first second
redis-cli LRANGE jobs 0 -1
redis-cli SADD languages python redis python
redis-cli XADD Events 1-0 state ready
redis-cli XREAD STREAMS Events 0
```

Run the tests from the repository root:

```sh
python -m pytest -q
```

The regression suite includes real loopback TCP tests; the environment must permit
binding to `127.0.0.1`. No external Redis server or network service is required.

## Implemented command subset

| Area | Commands / behavior |
| --- | --- |
| Strings | PING, ECHO, SET, GET, INCR, TYPE, KEYS |
| SET options | EX, PX, EXAT, PXAT, NX, XX, GET, KEEPTTL |
| Lists | LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE (inclusive stop, negative indexes) |
| Sets | SADD, SREM, SISMEMBER, SCARD, SMEMBERS |
| Streams | XADD, XRANGE with optional COUNT, XREAD with COUNT and BLOCK |
| Transactions | MULTI, EXEC, DISCARD for the data commands above |
| Protocol | RESP2 UTF-8 strings, pipeline parsing, connection-local partial-frame buffering |

Lists use `collections.deque`; sets use Python `set`; streams use an ordered
Python list of entries. These are deliberately simple representations, not the
internal encodings used by Redis itself. Empty lists and sets remove their key.
GET on a collection returns WRONGTYPE. Integer increment enforces signed 64-bit
bounds, while SET preserves strings such as `0012` exactly.

XREAD preserves case-sensitive key names, treats `0` as the beginning, resolves
`$` once per request, wakes when XADD makes entries available, and uses monotonic
time for its timeout. Within EXEC, XREAD never blocks. COUNT must be positive in
this subset. Range exclusivity syntax and XADD trimming options are not included.

Transactions have connection-local queues. Unknown commands and invalid arity
mark the queue invalid; EXEC then returns EXECABORT without running the queue.
Runtime data errors occupy their reply slot and do not roll back successful
commands. Nested MULTI reports an error without discarding the existing queue.
The in-memory EXEC loop does not await, so another local client cannot observe
intermediate mutations. Client disconnect cleans up queue state.

## Explicit limitations

- Administrative/replication commands (CONFIG, INFO, REPLCONF, PSYNC, WAIT) retain
  the earlier challenge implementation and are outside the transaction subset.
- Replication is incomplete: only SET has a propagation path, full sync still
  emits an empty example RDB, and replica snapshot loading/partial resync remain
  unfinished. Transactions are atomic only on the local server, not across replicas.
- RDB loading supports the earlier parser subset. There is no real snapshot writer,
  AOF logging, durability guarantee, automatic failover, or Redis Cluster support.
- WATCH, consumer groups, blocking list pops, hashes, sorted sets, and optional
  LPOP/RPOP count arguments are not implemented.
- RESP bulk strings are UTF-8 text, not arbitrary binary payloads. The client input
  buffer has a teaching-server limit of 16 MiB. RESP3 and inline command syntax are
  outside the supported client protocol.
- Stream scans and LRANGE materialization are linear. There are no eviction,
  authentication, resource isolation, or production observability facilities.

The tests verify this declared subset; they do not establish full Redis compatibility,
crash safety, or performance claims.
