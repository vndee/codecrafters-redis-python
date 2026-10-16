import os
import re
import time
import tempfile
from enum import StrEnum
from fnmatch import translate
from functools import lru_cache
from dataclasses import dataclass
from collections import deque
from typing import Dict, Any, Optional, List, Set, Union

from app.rdb import RDBParser, RDBEncoding
from app.resp import (
    RESPArray,
    RESPBulkString,
    RESPInteger,
)


class RedisCommand(StrEnum):
    PING = "ping"
    ECHO = "echo"
    SET = "set"
    GET = "get"
    CONFIG = "config"
    KEYS = "keys"
    INFO = "info"
    REPLCONF = "replconf"
    PSYNC = "psync"
    FULLRESYNC = "fullresync"
    WAIT = "wait"
    TYPE = "type"
    XADD = "xadd"
    XRANGE = "xrange"
    XREAD = "xread"
    INCR = "incr"
    MULTI = "multi"
    EXEC = "exec"
    DISCARD = "discard"


RedisInt = int
RedisString = str
RedisList = List[Any]
RedisSet = Set[Any]
RedisZSet = Dict[Any, Any]
RedisHash = Dict[Any, Any]
RedisNone = None


class RedisError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return self.message


@dataclass
class RedisDataObject:
    """
    Represents a Redis data object that can hold different types of values
    with associated metadata
    """

    data_type: RDBEncoding
    value: Union[RedisInt, RedisString, RedisList, RedisSet, RedisZSet, RedisHash, None]
    expire_at: Optional[float] = None

    @classmethod
    def create_string(
        cls, value: RedisString, expire_at: Optional[float] = None
    ) -> "RedisDataObject":
        return cls(
            data_type=RDBEncoding.STRING,
            expire_at=expire_at,
            value=value,
        )

    @classmethod
    def create_int(
        cls, value: RedisInt, expire_at: Optional[float] = None
    ) -> "RedisDataObject":
        return cls(
            data_type=RDBEncoding.STRING,
            expire_at=expire_at,
            value=value,
        )

    @classmethod
    def create_list(
        cls, value: RedisList, expire_at: Optional[float] = None
    ) -> "RedisDataObject":
        return cls(
            data_type=RDBEncoding.LIST,
            expire_at=expire_at,
            value=value,
        )

    @classmethod
    def create_set(
        cls, value: RedisSet, expire_at: Optional[float] = None
    ) -> "RedisDataObject":
        return cls(
            data_type=RDBEncoding.SET,
            expire_at=expire_at,
            value=value,
        )

    @classmethod
    def create_zset(
        cls, value: RedisZSet, expire_at: Optional[float] = None
    ) -> "RedisDataObject":
        return cls(
            data_type=RDBEncoding.ZSET,
            expire_at=expire_at,
            value=value,
        )

    @classmethod
    def create_hash(
        cls, value: RedisHash, expire_at: Optional[float] = None
    ) -> "RedisDataObject":
        return cls(
            data_type=RDBEncoding.HASH,
            expire_at=expire_at,
            value=value,
        )

    @classmethod
    def create_stream(
        cls, value: RedisList, expire_at: Optional[float] = None
    ) -> "RedisDataObject":
        return cls(
            data_type=RDBEncoding.STREAM,
            expire_at=expire_at,
            value=value,
        )

    @classmethod
    def create_none(cls) -> "RedisDataObject":
        return cls(
            data_type=RDBEncoding.STRING,
            expire_at=None,
            value=None,
        )

    def set_expiry(self, expire_at: Optional[float]):
        self.expire_at = expire_at

    def is_expired(self):
        return self.expire_at is not None and self.expire_at < time.time() * 1000

    def serialize(self) -> str:
        if self.data_type == RDBEncoding.STRING:
            return str(self.value)
        if self.data_type == RDBEncoding.LIST:
            return f"[{', '.join(self.value)}]"  # type: ignore[arg-type]
        if self.data_type == RDBEncoding.SET:
            return f"{{{', '.join(self.value)}}}"  # type: ignore[arg-type]
        if self.data_type == RDBEncoding.ZSET:
            return f"{{{', '.join([f'{k}={v}' for k, v in self.value.items()])}}}"  # type: ignore[arg-type, union-attr]
        if self.data_type == RDBEncoding.HASH:
            return f"{{{', '.join([f'{k}={v}' for k, v in self.value.items()])}}}"  # type: ignore[arg-type, union-attr]

        return ""

    def serialize_for_rdb(self) -> Dict[str, Any]:
        return {
            "type": self.data_type,
            "expire_at": self.expire_at,
            "value": self.value,
        }

    @classmethod
    def deserialize_from_rdb(cls, data: Dict[str, Any]) -> "RedisDataObject":
        data_type = RDBEncoding(data["type"])
        expire_at = data["expire_at"]
        value = data["value"]
        return cls(data_type=data_type, expire_at=expire_at, value=value)

    @property
    def ttl(self):
        if self.expire_at is None:
            return -1

        remaining = self.expire_at - time.time() * 1000
        return remaining if remaining > 0 else -1


class RedisDataStore:
    def __init__(
        self,
        dir: str = "/tmp/redis-files",
        dbfilename: str = "dump.rdb_example",
        database_idx: int = 0,
    ):
        self.dir = dir
        self.dbfilename = dbfilename
        self.database_idx = database_idx
        self.__data_dict: Dict[int, Dict[str, RedisDataObject]] = {}

        os.makedirs(self.dir, exist_ok=True)
        self.__data_dict.setdefault(self.database_idx, {})

        self.__load_from_rdb(os.path.join(self.dir, self.dbfilename))

    def __load_from_rdb(self, file_path: str):
        """
        Load data from RDB file if it exists
        :return:
        """
        if not os.path.exists(file_path):
            return

        try:
            rdb_parser = RDBParser(file_path=file_path)
            data = rdb_parser.parse()

            for db, keys in data.items():
                for key, value in keys.items():
                    expire_at = value["expire_at"]
                    if expire_at and expire_at < time.time() * 1000:
                        print(
                            f"The key: {key} has expired {time.time() - expire_at} seconds ago - skipping"
                        )
                        continue

                    self.__data_dict[db][key] = RedisDataObject.deserialize_from_rdb(
                        value
                    )
                    print(
                        f"Restored key: {key} with value: {self.__data_dict[db][key].serialize()}"  # type: ignore[union-attr]
                    )

            print(
                f"Loaded {len(self.__data_dict[self.database_idx].keys())} keys from RDB file"
            )
            print(f"Data: {self.__data_dict}")
        except Exception as e:
            print(f"Error loading RDB file: {str(e)}")

    def load_from_rdb_bytes(self, data: bytes):
        """
        Load data from RDB bytes
        :param data:
        :return:
        """
        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            temp_file.write(data)
            temp_file_path = temp_file.name
            self.__load_from_rdb(temp_file_path)

    @lru_cache(maxsize=None)
    def __glob_to_regex(self, pattern: str) -> re.Pattern:
        """
        Convert a glob-style pattern to a regex pattern.
        Cache the compiled regex pattern for future use.
        :param pattern:
        :return:
        """
        rg_pattern = None
        if pattern == "*":
            return re.compile(".*")

        if "*" not in pattern and "?" not in pattern and "[" not in pattern:
            return re.compile(re.escape(pattern))

        rg_pattern = translate(pattern)
        rg_pattern = rg_pattern.rstrip("\\Z")
        return re.compile(rg_pattern)

    def switch_database(self, database_idx: int):
        self.database_idx = database_idx
        self.__data_dict.setdefault(self.database_idx, {})

    def set(
        self,
        key: str,
        value: Union[RedisInt, RedisString],
        ex: Optional[int] = None,
        px: Optional[int] = None,
        exat: Optional[int] = None,
        pxat: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
        get: bool = False,
        return_applied: bool = False,
    ):
        """
        Set key to hold the string value. If key already holds a value, it is overwritten, regardless of its type.
        Any previous time to live associated with the key is discarded on successful SET operation.
        Time complexity: O(1)
        :param key: str
        :param value: Union[RedisString, RedisList, RedisSet, RedisZSet, RedisHash, RedisNone]
        :param ex: int - Set the specified expire time, in seconds.
        :param px: int - Set the specified expire time, in milliseconds.
        :param exat: int - Set the specified Unix time at which the key will expire, in seconds.
        :param pxat: int - Set the specified Unix time at which the key will expire, in milliseconds.
        :param nx: bool - Only set the key if it does not already exist.
        :param xx: bool - Only set the key if it already exist.
        :param keepttl: bool - Retain the time to live associated with the key.
        :param get: bool - Return the old value stored at key, or None when key did not exist.
        :return:
        """
        old = self._lookup(key)
        if get and old is not None and old.data_type != RDBEncoding.STRING:
            raise RedisError(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        old_value = old.value if old is not None else None
        key_exists = old is not None
        if (nx and key_exists) or (xx and not key_exists):
            result = old_value if get else None
            return (result, False) if return_applied else result

        expire_at = None
        current_time_ms = time.time() * 1000

        if ex:
            expire_at = current_time_ms + ex * 1000
        elif px:
            expire_at = current_time_ms + px
        elif exat:
            expire_at = exat * 1000
        elif pxat:
            expire_at = pxat

        if keepttl and key in self.__data_dict[self.database_idx]:
            expire_at = self.__data_dict[self.database_idx][key].expire_at  # type: ignore[union-attr]

        if isinstance(value, int):
            data_obj = RedisDataObject.create_int(value, expire_at)
        else:
            data_obj = RedisDataObject.create_string(value, expire_at)

        self.__data_dict[self.database_idx][key] = data_obj

        result = old_value if get else "OK"
        return (result, True) if return_applied else result

    def get(self, key: str) -> RESPBulkString:
        """
        Get the value of key. If the key does not exist the special value nil is returned.
        :param key:
        :return:
        """
        if key not in self.__data_dict[self.database_idx]:
            return RESPBulkString(value=None)

        data_obj = self.__data_dict[self.database_idx][key]
        if data_obj.is_expired():  # type: ignore[union-attr]
            del self.__data_dict[self.database_idx][key]
            return RESPBulkString(value=None)

        if data_obj.data_type != RDBEncoding.STRING:
            raise RedisError(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        return RESPBulkString(value=str(data_obj.value))  # type: ignore[union-attr]

    def _lookup(self, key: str) -> Optional[RedisDataObject]:
        """All data types share the same lazy-expiration boundary."""
        db = self.__data_dict[self.database_idx]
        obj = db.get(key)
        if obj is not None and obj.is_expired():
            del db[key]
            return None
        return obj

    def _collection(self, key, kind, create=None):
        obj = self._lookup(key)
        if obj is not None and obj.data_type != kind:
            raise RedisError(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        if obj is None and create is not None:
            obj = RedisDataObject(data_type=kind, value=create())
            self.__data_dict[self.database_idx][key] = obj
        return obj

    def list_push(self, key, values, left=False):
        obj = self._collection(key, RDBEncoding.LIST, deque)
        # RDB-loaded lists may use a Python list rather than a deque.
        if not isinstance(obj.value, deque):
            obj.value = deque(obj.value)
        for value in values:
            (obj.value.appendleft if left else obj.value.append)(value)
        return RESPInteger(value=len(obj.value))

    def list_pop(self, key, left=False):
        obj = self._collection(key, RDBEncoding.LIST)
        if obj is None:
            return RESPBulkString(value=None)
        if not isinstance(obj.value, deque):
            obj.value = deque(obj.value)
        value = obj.value.popleft() if left else obj.value.pop()
        if not obj.value:
            del self.__data_dict[self.database_idx][key]
        return RESPBulkString(value=value)

    def list_len(self, key):
        obj = self._collection(key, RDBEncoding.LIST)
        return RESPInteger(value=len(obj.value) if obj else 0)

    def list_range(self, key, start, stop):
        obj = self._collection(key, RDBEncoding.LIST)
        values = list(obj.value) if obj else []
        size = len(values)
        start = max(0, size + start if start < 0 else start)
        stop = size + stop if stop < 0 else stop
        values = values[start : stop + 1] if stop >= start else []
        return RESPArray(value=[RESPBulkString(value=v) for v in values])

    def set_add(self, key, values):
        obj = self._collection(key, RDBEncoding.SET, set)
        before = len(obj.value)
        obj.value.update(values)
        return RESPInteger(value=len(obj.value) - before)

    def set_remove(self, key, values):
        obj = self._collection(key, RDBEncoding.SET)
        if obj is None:
            return RESPInteger(value=0)
        before = len(obj.value)
        obj.value.difference_update(values)
        if not obj.value:
            del self.__data_dict[self.database_idx][key]
        return RESPInteger(value=before - len(obj.value))

    def set_members(self, key):
        obj = self._collection(key, RDBEncoding.SET)
        return RESPArray(
            value=[RESPBulkString(value=v) for v in (obj.value if obj else [])]
        )

    def set_card(self, key):
        obj = self._collection(key, RDBEncoding.SET)
        return RESPInteger(value=len(obj.value) if obj else 0)

    def set_ismember(self, key, value):
        obj = self._collection(key, RDBEncoding.SET)
        return RESPInteger(value=int(obj is not None and value in obj.value))

    def keys(self, pattern: str) -> List[str]:
        """
        Returns all keys matching pattern using Redis glob-style patterns.

        Time complexity: O(N) with N being the number of keys in the database
        Space complexity: O(N) for the returned list

        Pattern special characters:
        * - matches any sequence of characters
        ? - matches any single character
        [...] - matches any single character within the brackets
        \\x - escape character x

        Examples:
        - h?llo matches hello, hallo, hxllo
        - h*llo matches hllo, heeeello, h123llo
        - h[ae]llo matches hello and hallo, but not hillo

        :param pattern:
        :return:
        """
        regex = self.__glob_to_regex(pattern)

        matched_keys = []
        for key, data_obj in list(self.__data_dict[self.database_idx].items()):
            if data_obj.is_expired():  # type: ignore[union-attr]
                del self.__data_dict[self.database_idx][key]
                continue

            if regex.fullmatch(key):
                matched_keys.append(key)

        return matched_keys

    def type(self, key: str) -> str:
        """
        Returns the string representation of the type of the value stored at key.
        The different types that can be returned are: string, list, set, zset, hash and none.
        :param key:
        :return:
        """
        if key not in self.__data_dict[self.database_idx]:
            return "none"

        data_obj = self.__data_dict[self.database_idx][key]
        if data_obj.is_expired():  # type: ignore[union-attr]
            del self.__data_dict[self.database_idx][key]
            return "none"

        return data_obj.data_type.name.lower()  # type: ignore[union-attr]

    @staticmethod
    def stream_id(value, upper=False):
        if value == "-":
            return (0, 0)
        if value == "+":
            return (2**64 - 1, 2**64 - 1)
        try:
            parts = value.split("-")
            if not 1 <= len(parts) <= 2 or any(
                not p.isascii() or not p.isdigit() for p in parts
            ):
                raise ValueError
            ms = int(parts[0])
            seq = int(parts[1]) if len(parts) == 2 else (2**64 - 1 if upper else 0)
            if not (0 <= ms < 2**64 and 0 <= seq < 2**64):
                raise ValueError
            return ms, seq
        except (ValueError, TypeError):
            raise RedisError(
                "ERR Invalid stream ID specified as stream command argument"
            )

    def xadd(self, key, id, fields):
        stream = self._collection(key, RDBEncoding.STREAM)
        previous = (
            self.stream_id(stream.value[-1][0]) if stream and stream.value else (0, 0)
        )
        if id == "*":
            ms = max(int(time.time() * 1000), previous[0])
            current = (ms, previous[1] + 1 if ms == previous[0] else 0)
        elif id.endswith("-*"):
            ms = self.stream_id(id[:-2])[0]
            current = (ms, previous[1] + 1 if ms == previous[0] else 0)
        else:
            if "-" not in id or id in ("-", "+"):
                raise RedisError(
                    "ERR Invalid stream ID specified as stream command argument"
                )
            current = self.stream_id(id)
        if current == (0, 0):
            raise RedisError("ERR The ID specified in XADD must be greater than 0-0")
        if current <= previous:
            raise RedisError(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            )
        if current[1] >= 2**64:
            raise RedisError("ERR stream sequence overflow")
        if stream is None:
            stream = self._collection(key, RDBEncoding.STREAM, list)
        new_id = f"{current[0]}-{current[1]}"
        stream.value.append((new_id, list(fields)))
        return RESPBulkString(value=new_id)

    @staticmethod
    def _stream_entry(id, fields):
        return RESPArray(value=[RESPBulkString(value=id), RESPArray(value=fields)])

    def xrange(self, key, lower_bound, upper_bound, count=None):
        lower = self.stream_id(lower_bound)
        upper = self.stream_id(upper_bound, upper=True)
        stream = self._collection(key, RDBEncoding.STREAM)
        entries = []
        if stream:
            for id, fields in stream.value:
                if lower <= self.stream_id(id) <= upper:
                    entries.append(self._stream_entry(id, fields))
                    if count is not None and len(entries) >= count:
                        break
        return RESPArray(value=entries)

    def get_current_max_stream_id(self, key):
        stream = self._collection(key, RDBEncoding.STREAM)
        return stream.value[-1][0] if stream and stream.value else "0-0"

    def xread(self, keys, ids, count=None):
        result = []
        for key, id in zip(keys, ids):
            after = self.stream_id(id)
            stream = self._collection(key, RDBEncoding.STREAM)
            entries = []
            if stream:
                for entry_id, fields in stream.value:
                    if self.stream_id(entry_id) > after:
                        entries.append(self._stream_entry(entry_id, fields))
                        if count is not None and len(entries) >= count:
                            break
            if entries:
                result.append(
                    RESPArray(
                        value=[RESPBulkString(value=key), RESPArray(value=entries)]
                    )
                )
        return RESPArray(value=result or None)

    def incr(self, key):
        obj = self._lookup(key)
        if obj is None:
            self.set(key, "1")
            return RESPInteger(value=1)
        if obj.data_type != RDBEncoding.STRING:
            raise RedisError(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        value = str(obj.value)
        if not re.fullmatch(r"0|-?[1-9][0-9]*", value):
            raise RedisError("ERR value is not an integer or out of range")
        number = int(value)
        if not -(2**63) <= number < 2**63 - 1:
            raise RedisError("ERR increment or decrement would overflow")
        obj.value = str(number + 1)
        return RESPInteger(value=number + 1)

    def dump_to_rdb(self) -> bytes:
        """
        Serialize the data to RDB format
        :return:
        """
        # Hardcode empty RDB file for now
        return b"REDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2"
