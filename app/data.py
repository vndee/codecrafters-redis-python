import time
from enum import StrEnum
from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Set, Union


class RedisCommand(StrEnum):
    PING = "ping"
    ECHO = "echo"
    SET = "set"
    GET = "get"
    CONFIG = "config"


class RedisDataType(StrEnum):
    STRING = "string"
    LIST = "list"
    SET = "set"
    ZSET = "zset"
    HASH = "hash"
    STREAM = "stream"
    NONE = "none"


RedisString = str
RedisList = List[str]
RedisSet = Set[str]
RedisZSet = Dict[str, float]
RedisHash = Dict[str, str]
RedisNone = None


@dataclass
class RedisMetaData:
    created_at: float
    updated_at: float
    expire_at: Optional[int] = None
    access_count: int = 0

    def is_expired(self):
        if self.expire_at is None:
            return False
        return time.time() * 1000 >= self.expire_at

    def update_access_count(self):
        self.access_count += 1
        self.updated_at = time.time() * 1000


@dataclass
class RedisDataObject:
    """
    Represents a Redis data object that can hold different types of values
    with associated metadata
    """
    data_type: RedisDataType
    metadata: RedisMetaData
    value: Union[RedisString, RedisList, RedisSet, RedisZSet, RedisHash, None]

    @classmethod
    def create_string(cls, value: RedisString, expire_at: Optional[int] = None) -> "RedisDataObject":
        return cls(
            data_type=RedisDataType.STRING,
            metadata=RedisMetaData(
                created_at=time.time() * 1000,
                updated_at=time.time() * 1000,
                expire_at=expire_at,
            ),
            value=value,
        )

    @classmethod
    def create_list(cls, value: RedisList, expire_at: Optional[int] = None) -> "RedisDataObject":
        return cls(
            data_type=RedisDataType.LIST,
            metadata=RedisMetaData(
                created_at=time.time() * 1000,
                updated_at=time.time() * 1000,
                expire_at=expire_at,
            ),
            value=value,
        )

    @classmethod
    def create_set(cls, value: RedisSet, expire_at: Optional[int] = None) -> "RedisDataObject":
        return cls(
            data_type=RedisDataType.SET,
            metadata=RedisMetaData(
                created_at=time.time() * 1000,
                updated_at=time.time() * 1000,
                expire_at=expire_at,
            ),
            value=value,
        )

    @classmethod
    def create_zset(cls, value: RedisZSet, expire_at: Optional[int] = None) -> "RedisDataObject":
        return cls(
            data_type=RedisDataType.ZSET,
            metadata=RedisMetaData(
                created_at=time.time() * 1000,
                updated_at=time.time() * 1000,
                expire_at=expire_at,
            ),
            value=value,
        )

    @classmethod
    def create_hash(cls, value: RedisHash, expire_at: Optional[int] = None) -> "RedisDataObject":
        return cls(
            data_type=RedisDataType.HASH,
            metadata=RedisMetaData(
                created_at=time.time() * 1000,
                updated_at=time.time() * 1000,
                expire_at=expire_at,
            ),
            value=value,
        )

    @classmethod
    def create_none(cls) -> "RedisDataObject":
        return cls(
            data_type=RedisDataType.NONE,
            metadata=RedisMetaData(
                created_at=time.time() * 1000,
                updated_at=time.time() * 1000,
            ),
            value=None,
        )

    def set_expiry(self, expire_at: Optional[int]):
        self.metadata.expire_at = expire_at
        self.metadata.updated_at = time.time() * 1000

    def is_expired(self):
        return self.metadata.is_expired()

    def update_access_count(self):
        self.metadata.update_access_count()

    def serialize(self) -> str:
        if self.data_type == RedisDataType.STRING:
            return self.value
        if self.data_type == RedisDataType.LIST:
            return f"[{', '.join(self.value)}]"
        if self.data_type == RedisDataType.SET:
            return f"{{{', '.join(self.value)}}}"
        if self.data_type == RedisDataType.ZSET:
            return f"{{{', '.join([f'{k}={v}' for k, v in self.value.items()])}}}"
        if self.data_type == RedisDataType.HASH:
            return f"{{{', '.join([f'{k}={v}' for k, v in self.value.items()])}}}"
        if self.data_type == RedisDataType.NONE:
            return "(nil)"
        return ""

    @property
    def ttl(self):
        if self.metadata.expire_at is None:
            return -1

        remaining = self.metadata.expire_at - time.time() * 1000
        return remaining if remaining > 0 else -1


class RedisDataStore:
    def __init__(
        self,
        dir: str = "/tmp/redis-files",
        dbfilename: str = "dump.rdb",
    ):
        self.dir = dir
        self.dbfilename = dbfilename
        self.__data_dict: Dict[str, RedisDataObject] = {}

    def set(
        self,
        key: str,
        value: Union[RedisString, RedisList, RedisSet, RedisZSet, RedisHash, RedisNone],
        ex: Optional[int] = None,
        px: Optional[int] = None,
        exat: Optional[int] = None,
        pxat: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
        get: bool = False,
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
        old_value = None
        if key in self.__data_dict:
            old_value = self.__data_dict[key].value

        key_exists = key in self.__data_dict
        if (nx and key_exists) or (xx and not key_exists):
            return None

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

        if keepttl and key in self.__data_dict:
            expire_at = self.__data_dict[key].metadata.expire_at

        data_obj = RedisDataObject.create_string(value, expire_at)
        self.__data_dict[key] = data_obj

        return old_value if get else "OK"

    def get(self, key: str) -> Optional[RedisString]:
        """
        Get the value of key. If the key does not exist the special value nil is returned.
        :param key:
        :return:
        """
        if key not in self.__data_dict:
            return None

        data_obj = self.__data_dict[key]
        if data_obj.is_expired():
            del self.__data_dict[key]
            return None

        data_obj.update_access_count()
        return data_obj.value
