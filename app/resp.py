from enum import StrEnum
from dataclasses import dataclass
from typing import Optional, Any, List
from abc import ABC, abstractmethod


# Your existing enums
class RESPObjectTypeCategory(StrEnum):
    SIMPLE = "simple"
    AGGREGATE = "aggregate"
    EXTRA = "extra"


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


@dataclass
class RESPBytesLength(RESPObject):
    value = int

    def __init__(self, value: int):
        """
        Custom RESP byte length object for RDB file transfer between replicas
        :param value:
        """
        super().__init__(type=RESPObjectType.BULK_STRING, value=value)

    def serialize(self) -> bytes:
        return f"${self.value}\r\n".encode()


class RESPParser:
    def __init__(self, protocol_version: RESPProtocolVersion = RESPProtocolVersion.RESP2):
        self.protocol_version = protocol_version

    def parse(self, data: bytes) -> List[RESPObject]:
        """
        Parse RESP data into a list of RESPObjects.
        Each complete command will be one RESPObject in the list.
        """
        if not data:
            return []

        objects = []
        remaining = data

        while remaining:
            try:
                object_size = self._get_object_size(remaining)
                if object_size <= 0:
                    break

                obj = self._parse_single(remaining[:object_size])
                if obj:
                    objects.append(obj)

                remaining = remaining[object_size:]
            except (ValueError, IndexError):
                break

        return objects

    def _get_object_size(self, data: bytes) -> int:
        """
        Calculate the size of the next complete RESP object in bytes.
        """
        if len(data) < 4:  # Minimum valid RESP object size
            return 0

        try:
            type_byte = data[0:1].decode()
            resp_type = RESPObjectType(type_byte)

            line_end = data.find(b'\r\n')
            if line_end == -1:
                return 0

            if resp_type in {RESPObjectType.SIMPLE_STRING, RESPObjectType.SIMPLE_ERROR, RESPObjectType.INTEGER}:
                return line_end + 2

            elif resp_type == RESPObjectType.BULK_STRING:
                length = int(data[1:line_end])
                if length == -1:
                    return line_end + 2
                return line_end + 2 + length + 2  # First line + content + final \r\n

            elif resp_type == RESPObjectType.ARRAY:
                total_size = line_end + 2  # Size of the array header
                elements_count = int(data[1:line_end])
                if elements_count == 0:
                    return total_size

                remaining = data[total_size:]
                for _ in range(elements_count):
                    element_size = self._get_object_size(remaining)
                    if element_size <= 0:
                        return 0
                    total_size += element_size
                    remaining = remaining[element_size:]

                return total_size

        except (ValueError, IndexError):
            return 0

        return 0

    def _parse_single(self, data: bytes) -> Optional[RESPObject]:
        """
        Parse a single complete RESP object.
        """
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
                current_data = data[data.find(b'\r\n') + 2:]
                for _ in range(length):
                    element_size = self._get_object_size(current_data)
                    if element_size <= 0:
                        break
                    element = self._parse_single(current_data[:element_size])
                    if element:
                        elements.append(element)
                    current_data = current_data[element_size:]
                return RESPArray(elements)

        except (ValueError, IndexError) as e:
            print(f"Error parsing RESP data: {e}")
            return None

        return None
