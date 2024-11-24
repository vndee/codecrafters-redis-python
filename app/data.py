import os
import re
import time
from enum import StrEnum
from fnmatch import translate
from functools import lru_cache
from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Set, Union

from app.rdb import RDBParser, RDBEncoding


class RedisCommand(StrEnum):
    PING = "ping"
    ECHO = "echo"
    SET = "set"
    GET = "get"
    CONFIG = "config"
    KEYS = "keys"


RedisString = str
RedisList = List[str]
RedisSet = Set[str]
RedisZSet = Dict[str, float]
RedisHash = Dict[str, str]
RedisNone = None


@dataclass
class RedisDataObject:
    """
    Represents a Redis data object that can hold different types of values
    with associated metadata
    """
    data_type: RDBEncoding
    value: Union[RedisString, RedisList, RedisSet, RedisZSet, RedisHash, None]
    expire_at: Optional[float] = None

    @classmethod
    def create_string(cls, value: RedisString, expire_at: Optional[int] = None) -> "RedisDataObject":
        return cls(
            data_type=RDBEncoding.STRING,
            expire_at=expire_at,
            value=value,
        )

    @classmethod
    def create_list(cls, value: RedisList, expire_at: Optional[int] = None) -> "RedisDataObject":
        return cls(
            data_type=RDBEncoding.LIST,
            expire_at=expire_at,
            value=value,
        )

    @classmethod
    def create_set(cls, value: RedisSet, expire_at: Optional[int] = None) -> "RedisDataObject":
        return cls(
            data_type=RDBEncoding.SET,
            expire_at=expire_at,
            value=value,
        )

    @classmethod
    def create_zset(cls, value: RedisZSet, expire_at: Optional[int] = None) -> "RedisDataObject":
        return cls(
            data_type=RDBEncoding.ZSET,
            expire_at=expire_at,
            value=value,
        )

    @classmethod
    def create_hash(cls, value: RedisHash, expire_at: Optional[int] = None) -> "RedisDataObject":
        return cls(
            data_type=RDBEncoding.HASH,
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

    def set_expiry(self, expire_at: Optional[int]):
        self.expire_at = expire_at

    def is_expired(self):
        return self.expire_at is not None and self.expire_at < time.time() * 1000

    def serialize(self) -> str:
        if self.data_type == RDBEncoding.STRING:
            return self.value
        if self.data_type == RDBEncoding.LIST:
            return f"[{', '.join(self.value)}]"
        if self.data_type == RDBEncoding.SET:
            return f"{{{', '.join(self.value)}}}"
        if self.data_type == RDBEncoding.ZSET:
            return f"{{{', '.join([f'{k}={v}' for k, v in self.value.items()])}}}"
        if self.data_type == RDBEncoding.HASH:
            return f"{{{', '.join([f'{k}={v}' for k, v in self.value.items()])}}}"

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
        dbfilename: str = "dump.rdb",
        database_idx: int = 0,
    ):
        self.dir = dir
        self.dbfilename = dbfilename
        self.database_idx = database_idx
        self.__data_dict: Dict[int, Dict[str, RedisDataObject]] = {}    # database -> key -> value

        os.makedirs(self.dir, exist_ok=True)
        self.__data_dict.setdefault(self.database_idx, {})

        self.__load_from_rdb()

    def __load_from_rdb(self):
        """
        Load data from RDB file if it exists
        :return:
        """
        rdb_file_path = os.path.join(self.dir, self.dbfilename)
        if not os.path.exists(rdb_file_path):
            return

        try:
            rdb_parser = RDBParser(file_path=rdb_file_path)
            data = rdb_parser.parse()

            for db, keys in data.items():
                for key, value in keys.items():
                    expire_at = value["expire_at"]
                    if expire_at * 1000 and expire_at < time.time() * 1000:
                        print(f"The key: {key} has expired {time.time() * 1000 - expire_at}ms ago - skipping")
                        continue

                    self.__data_dict[db][key] = RedisDataObject.deserialize_from_rdb(value)
                    print(f"Restored key: {key} with value: {self.__data_dict[db][key].serialize()}")

            print(f"Loaded {len(self.__data_dict[self.database_idx].keys())} keys from RDB file")
            print(f"Data: {self.__data_dict}")
        except Exception as e:
            print(f"Error loading RDB file: {str(e)}")

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
            old_value = self.__data_dict[self.database_idx][key].value

        key_exists = key in self.__data_dict[self.database_idx]
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

        if keepttl and key in self.__data_dict[self.database_idx]:
            expire_at = self.__data_dict[self.database_idx][key].expire_at

        data_obj = RedisDataObject.create_string(value, expire_at)
        self.__data_dict[self.database_idx][key] = data_obj

        return old_value if get else "OK"

    def get(self, key: str) -> Optional[RedisString]:
        """
        Get the value of key. If the key does not exist the special value nil is returned.
        :param key:
        :return:
        """
        if key not in self.__data_dict[self.database_idx]:
            return None

        data_obj = self.__data_dict[self.database_idx][key]
        if data_obj.is_expired():
            del self.__data_dict[self.database_idx][key]
            return None

        return data_obj.value

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
        for key, data_obj in self.__data_dict[self.database_idx].items():
            if data_obj.is_expired():
                del self.__data_dict[self.database_idx][key]
                continue

            if regex.match(key):
                matched_keys.append(key)

        return matched_keys
