from enum import StrEnum
from dataclasses import dataclass
from typing import Optional, Any, List, Dict, Union
from abc import ABC, abstractmethod


# Your existing enums
class RESPObjectTypeCategory(StrEnum):
    SIMPLE = "simple"
    AGGREGATE = "aggregate"


class RESPProtocolVersion(StrEnum):
    RESP2 = "RESP2"
    RESP3 = "RESP3"


class RESPObjectType(StrEnum):
    SIMPLE_STRING = "+"
    SIMPLE_ERROR = "-"
    INTEGER = ":"
    BULK_STRING = "$"
    ARRAY = "*"
    NULL = "_"
    BOOLEAN = "#"
    DOUBLE = ","
    BIG_NUMBER = "("
    BULK_ERROR = "!"
    VERBATIM_STRING = "="
    MAP = "%"
    ATTRIBUTE = "`"
    SET = "~"
    PUSH = ">"


@dataclass
class RESPObject(ABC):
    type: RESPObjectType
    value: Optional[Any]

    @property
    def minimal_protocol_version(self) -> RESPProtocolVersion:
        if self.type in {
            RESPObjectType.SIMPLE_STRING,
            RESPObjectType.SIMPLE_ERROR,
            RESPObjectType.INTEGER,
            RESPObjectType.BULK_STRING,
            RESPObjectType.ARRAY,
        }:
            return RESPProtocolVersion.RESP2
        return RESPProtocolVersion.RESP3

    @property
    def category(self) -> RESPObjectTypeCategory:
        if self.type in {
            RESPObjectType.SIMPLE_STRING,
            RESPObjectType.SIMPLE_ERROR,
            RESPObjectType.INTEGER,
            RESPObjectType.NULL,
            RESPObjectType.BOOLEAN,
            RESPObjectType.DOUBLE,
            RESPObjectType.BIG_NUMBER
        }:
            return RESPObjectTypeCategory.SIMPLE
        return RESPObjectTypeCategory.AGGREGATE

    @abstractmethod
    def serialize(self) -> bytes:
        """Convert the object to RESP wire format"""
        pass


@dataclass
class RESPSimpleString(RESPObject):
    value: str

    def __init__(self, value: str):
        super().__init__(type=RESPObjectType.SIMPLE_STRING, value=value)

    def serialize(self) -> bytes:
        return f"+{self.value}\r\n".encode()


@dataclass
class RESPError(RESPObject):
    value: str

    def __init__(self, value: str):
        super().__init__(type=RESPObjectType.SIMPLE_ERROR, value=value)

    def serialize(self) -> bytes:
        return f"-{self.value}\r\n".encode()


@dataclass
class RESPInteger(RESPObject):
    value: int

    def __init__(self, value: int):
        super().__init__(type=RESPObjectType.INTEGER, value=value)

    def serialize(self) -> bytes:
        return f":{self.value}\r\n".encode()


@dataclass
class RESPBulkString(RESPObject):
    value: Optional[str]

    def __init__(self, value: Optional[str]):
        super().__init__(type=RESPObjectType.BULK_STRING, value=value)

    def serialize(self) -> bytes:
        if self.value is None:
            return b"$-1\r\n"
        return f"${len(self.value)}\r\n{self.value}\r\n".encode()


@dataclass
class RESPArray(RESPObject):
    value: List[RESPObject]

    def __init__(self, value: List[RESPObject]):
        super().__init__(type=RESPObjectType.ARRAY, value=value)

    def serialize(self) -> bytes:
        if not self.value:
            return b"*0\r\n"
        parts = [f"*{len(self.value)}\r\n".encode()]
        for item in self.value:
            parts.append(item.serialize())
        return b"".join(parts)


class RESPParser:
    def __init__(self, protocol_version: RESPProtocolVersion = RESPProtocolVersion.RESP2):
        self.protocol_version = protocol_version

    def parse(self, data: bytes) -> Optional[RESPObject]:
        if not data:
            return None

        try:
            type_byte = data[0:1].decode()
            resp_type = RESPObjectType(type_byte)

            lines = data.split(b'\r\n')

            if resp_type == RESPObjectType.SIMPLE_STRING:
                return RESPSimpleString(lines[0][1:].decode())

            elif resp_type == RESPObjectType.SIMPLE_ERROR:
                return RESPError(lines[0][1:].decode())

            elif resp_type == RESPObjectType.INTEGER:
                return RESPInteger(int(lines[0][1:]))

            elif resp_type == RESPObjectType.BULK_STRING:
                length = int(lines[0][1:])
                if length == -1:
                    return RESPBulkString(None)
                return RESPBulkString(lines[1].decode())

            elif resp_type == RESPObjectType.ARRAY:
                length = int(lines[0][1:])
                if length == 0:
                    return RESPArray([])

                elements = []
                current_data = b'\r\n'.join(lines[1:])
                for _ in range(length):
                    element = self.parse(current_data)
                    if element:
                        elements.append(element)
                        current_data = current_data[len(element.serialize()):]
                return RESPArray(elements)

        except (ValueError, IndexError) as e:
            raise ValueError(f"Invalid RESP data: {str(e)}")

        return None


class RedisCommand(StrEnum):
    PING = "PING"
    ECHO = "ECHO"
    SET = "SET"
    GET = "GET"
    INCR = "INCR"
    DECR = "DECR"
    EXISTS = "EXISTS"
    DEL = "DEL"
    KEYS = "KEYS"
    FLUSHALL = "FLUSHALL"
    FLUSHDB = "FLUSHDB"
    TYPE = "TYPE"
    EXPIRE = "EXPIRE"
    TTL = "TTL"
    PERSIST = "PERSIST"
    RENAME = "RENAME"
    RENAMENX = "RENAMENX"
    RANDOMKEY = "RANDOMKEY"
    DBSIZE = "DBSIZE"
    SELECT = "SELECT"
    MOVE = "MOVE"
    PEXPIRE = "PEXPIRE"
    PTTL = "PTTL"
    EXPIREAT = "EXPIREAT"
    PEXPIREAT = "PEXPIREAT"
    SCAN = "SCAN"
    MGET = "MGET"
    MSET = "MSET"
    MSETNX = "MSETNX"
    SETNX = "SETNX"
    SETEX = "SETEX"
    APPEND = "APPEND"
    GETSET = "GETSET"
    STRLEN = "STRLEN"
    INCRBY = "INCRBY"
    DECRBY = "DECRBY"
    INCRBYFLOAT = "INCRBYFLOAT"
    HSET = "HSET"
    HGET = "HGET"
    HGETALL = "HGETALL"
    HDEL = "HDEL"
    HEXISTS = "HEXISTS"
    HLEN = "HLEN"
    HMGET = "HMGET"
    HMSET = "HMSET"
    HINCRBY = "HINCRBY"
    HINCRBYFLOAT = "HINCRBYFLOAT"
    HKEYS = "HKEYS"
    HVALS = "HVALS"
    HSTRLEN = "HSTRLEN"
    LINDEX = "LINDEX"
    LINSERT = "LINSERT"
    LLEN = "LLEN"
    LPOP = "LPOP"
    LPUSH = "LPUSH"
    LPUSHX = "LPUSHX"
    LRANGE = "LRANGE"
