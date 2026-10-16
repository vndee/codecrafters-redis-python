from enum import StrEnum
from dataclasses import dataclass
from typing import Optional, Any, List
from abc import ABC, abstractmethod


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
    BULK_BYTES = (
        "&"  # Custom type for bytes length, this is not part of the RESP protocol
    )


@dataclass
class RESPObject(ABC):
    type: RESPObjectType
    value: Optional[Any]
    bytes_length: int

    def __init__(self, type: RESPObjectType, value: Any, bytes_length: int = 0):
        self.type = type
        self.value = value
        self.bytes_length = bytes_length

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
            RESPObjectType.BIG_NUMBER,
        }:
            return RESPObjectTypeCategory.SIMPLE

        elif self.type in {RESPObjectType.BULK_BYTES}:
            return RESPObjectTypeCategory.EXTRA

        return RESPObjectTypeCategory.AGGREGATE

    @abstractmethod
    def serialize(self) -> bytes:
        """Convert the object to RESP wire format"""
        pass

    @property
    def serialized_bytes_length(self) -> int:
        return len(self.serialize())


@dataclass
class RESPBytesLength(RESPObject):
    def __init__(self, **kwargs):
        """
        Custom RESP byte length object for RDB file transfer between replicas
        :param value:
        """
        super().__init__(type=RESPObjectType.BULK_STRING, **kwargs)

    def serialize(self) -> bytes:
        return f"${self.value}\r\n".encode()


@dataclass
class RESPSimpleString(RESPObject):
    def __init__(self, **kwargs):
        super().__init__(type=RESPObjectType.SIMPLE_STRING, **kwargs)

    def serialize(self) -> bytes:
        return f"+{self.value}\r\n".encode()


@dataclass
class RESPError(RESPObject):
    def __init__(self, **kwargs):
        super().__init__(type=RESPObjectType.SIMPLE_ERROR, **kwargs)

    def serialize(self) -> bytes:
        return f"-{self.value}\r\n".encode()


@dataclass
class RESPInteger(RESPObject):
    def __init__(self, **kwargs):
        super().__init__(type=RESPObjectType.INTEGER, **kwargs)

    def serialize(self) -> bytes:
        return f":{self.value}\r\n".encode()


@dataclass
class RESPBulkString(RESPObject):
    def __init__(self, **kwargs):
        super().__init__(type=RESPObjectType.BULK_STRING, **kwargs)

    def serialize(self) -> bytes:
        if self.value is None:
            return b"$-1\r\n"
        value = self.value.encode()
        return f"${len(value)}\r\n".encode() + value + b"\r\n"


@dataclass
class RESPBulkBytes(RESPObject):
    def __init__(self, **kwargs):
        super().__init__(type=RESPObjectType.BULK_BYTES, **kwargs)

    def serialize(self) -> bytes:
        if self.value is None:
            return b"$-1\r\n"
        return f"${len(self.value)}\r\n{self.value}".encode()


@dataclass
class RESPArray(RESPObject):
    def __init__(self, **kwargs):
        super().__init__(type=RESPObjectType.ARRAY, **kwargs)

    def serialize(self) -> bytes:
        if self.value is None:
            return b"*-1\r\n"
        if not self.value:
            return b"*0\r\n"
        parts = [f"*{len(self.value)}\r\n".encode()]
        for item in self.value:
            parts.append(item.serialize())
        return b"".join(parts)


@dataclass
class RESPSimpleError(RESPObject):
    def __init__(self, **kwargs):
        super().__init__(type=RESPObjectType.SIMPLE_ERROR, **kwargs)

    def serialize(self) -> bytes:
        return f"-{self.value}\r\n".encode()


@dataclass
class RESPNull(RESPObject):
    def __init__(self, **kwargs):
        super().__init__(type=RESPObjectType.NULL, value=None, bytes_length=0)

    def serialize(self) -> bytes:
        return b"_\r\n"


class RESPParser:
    def __init__(
        self, protocol_version: RESPProtocolVersion = RESPProtocolVersion.RESP2
    ):
        self.protocol_version = protocol_version
        self._buffer = b""

    def feed(self, data: bytes) -> List[RESPObject]:
        """Decode ordinary client frames, retaining incomplete bytes per connection.

        Replication RDB transfers use a different framing convention and continue
        to use the separate parse() path.
        """
        self._buffer += data
        if len(self._buffer) > 16 * 1024 * 1024:
            raise ValueError("client input exceeds the 16 MiB teaching-server limit")
        result = []
        while self._buffer:
            size = self._frame_size(self._buffer)
            if not size:
                break
            obj = self._parse_single(self._buffer[:size])
            if obj is None:
                raise ValueError("invalid RESP frame")
            result.append(obj)
            self._buffer = self._buffer[size:]
        return result

    def _frame_size(self, data: bytes) -> int:
        line_end = data.find(b"\r\n")
        if line_end < 0:
            return 0
        prefix = data[:1]
        end = line_end + 2
        if prefix in (b"+", b"-", b":"):
            return end
        if prefix not in (b"$", b"*"):
            raise ValueError("unsupported RESP type")
        length = int(data[1:line_end])
        if length < -1:
            raise ValueError("invalid RESP length")
        if length == -1:
            return end
        if prefix == b"$":
            end += length + 2
            if len(data) < end:
                return 0
            if data[end - 2 : end] != b"\r\n":
                raise ValueError("invalid bulk string terminator")
            return end
        for _ in range(length):
            size = self._frame_size(data[end:])
            if not size:
                return 0
            end += size
        return end

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

                if (
                    isinstance(obj, RESPSimpleString)
                    and len(remaining) - object_size >= 2
                    and remaining[object_size : object_size + 2] == b"\r\n"
                ):
                    object_size += 2

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

            line_end = data.find(b"\r\n")
            if line_end == -1:
                return 0

            if resp_type in {
                RESPObjectType.SIMPLE_STRING,
                RESPObjectType.SIMPLE_ERROR,
                RESPObjectType.INTEGER,
            }:
                return line_end + 2

            elif resp_type == RESPObjectType.BULK_STRING:
                length = int(data[1:line_end])
                if length == -1:
                    return line_end + 2

                if (
                    len(data) - line_end + length + 2 > 0
                    and data[line_end + 2 + length : line_end + 2 + length + 2]
                    == b"\r\n"
                ):
                    return (
                        line_end + 2 + length + 2
                    )  # Size of the bulk string header + length + CRLF

                return line_end + 2 + length

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

            is_not_end_with_crlf = not data.endswith(b"\r\n")
            lines = data.split(b"\r\n")

            if resp_type == RESPObjectType.SIMPLE_STRING:
                return RESPSimpleString(
                    value=lines[0][1:].decode(), bytes_length=len(data)
                )

            elif resp_type == RESPObjectType.SIMPLE_ERROR:
                return RESPError(value=lines[0][1:].decode(), bytes_length=len(data))

            elif resp_type == RESPObjectType.INTEGER:
                return RESPInteger(value=int(lines[0][1:]), bytes_length=len(data))

            elif resp_type == RESPObjectType.BULK_STRING:
                length = int(lines[0][1:])
                if length == -1:
                    return RESPBulkString(value=None)
                start = data.find(b"\r\n") + 2
                payload = data[start : start + length]
                return (
                    RESPBulkBytes(value=payload, bytes_length=len(data))
                    if is_not_end_with_crlf
                    else RESPBulkString(value=payload.decode(), bytes_length=len(data))
                )

            elif resp_type == RESPObjectType.ARRAY:
                length = int(lines[0][1:])
                if length == -1:
                    return RESPArray(value=None, bytes_length=len(data))
                if length == 0:
                    return RESPArray(value=[], bytes_length=len(data))

                elements = []
                current_data = data[data.find(b"\r\n") + 2 :]
                for _ in range(length):
                    element_size = self._get_object_size(current_data)
                    if element_size <= 0:
                        break
                    element = self._parse_single(current_data[:element_size])
                    if element:
                        elements.append(element)
                    current_data = current_data[element_size:]
                return RESPArray(value=elements, bytes_length=len(data))

        except (ValueError, IndexError) as e:
            print(f"Error parsing RESP data: {e}")
            return None

        return None
