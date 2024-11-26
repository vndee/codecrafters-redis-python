import struct
from datetime import datetime
from enum import IntEnum
from typing import BinaryIO, Dict, Any, Optional


class RDBOperationType(IntEnum):
    EOF = 0xFF  # 255
    SELECTDB = 0xFE  # 254
    EXPIRETIME = 0xFD  # 253
    EXPIRETIME_MS = 0xFC  # 252
    RESIZEDB = 0xFB  # 251
    AUX = 0xFA  # 250


class RDBEncoding(IntEnum):
    STRING = 0
    LIST = 1
    SET = 2
    ZSET = 3
    HASH = 4
    ZIPMAP = 9
    ZIPLIST = 10
    INTSET = 11
    ZSET_ZIPLIST = 12
    HASH_ZIPLIST = 13
    LIST_QUICKLIST = 14
    STREAM = 15


class RDBParser:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.data: Dict[int, Dict[str, Any]] = {}  # database -> key -> value
        self.aux_fields: Dict[str, str] = {}  # Store auxiliary fields
        self.current_db = 0

    def parse(self) -> Dict[int, Dict[str, Any]]:
        """Parse the RDB file and return the data structure"""
        with open(self.file_path, "rb") as f:
            magic = f.read(5)
            if magic != b"REDIS":
                raise ValueError("Invalid RDB file format")

            version = f.read(4)
            if not version.startswith(b"00"):
                raise ValueError(f"Unsupported RDB version: {version.decode()}")

            while True:
                type_byte = f.read(1)
                if not type_byte:
                    break

                op_type = type_byte[0]

                if op_type == RDBOperationType.EOF:
                    break
                elif op_type == RDBOperationType.SELECTDB:
                    self.current_db = self._read_length(f)
                    if self.current_db not in self.data:
                        self.data[self.current_db] = {}
                elif op_type == RDBOperationType.EXPIRETIME:
                    expire_time = struct.unpack("I", f.read(4))[0]
                    self._process_key_value_pair(f, expire_time * 1000)
                elif op_type == RDBOperationType.EXPIRETIME_MS:
                    expire_time = struct.unpack("Q", f.read(8))[0]
                    self._process_key_value_pair(f, expire_time)
                elif op_type == RDBOperationType.AUX:
                    aux_key = self._read_string(f)
                    aux_value = self._read_string(f)
                    self.aux_fields[aux_key] = aux_value
                elif op_type == RDBOperationType.RESIZEDB:
                    # Read hash table sizes
                    db_size = self._read_length(f)  # noqa: F841
                    expire_size = self._read_length(f)  # noqa: F841
                    # Could store these sizes if needed
                else:
                    self._process_key_value_pair(f, None, op_type)

            # Verify CRC64 (8 bytes)
            crc = f.read(8)  # noqa: F841
            # TODO: Implement CRC verification

        return self.data

    def _process_key_value_pair(
        self,
        f: BinaryIO,
        expire_time: Optional[float] = None,
        value_type: Optional[int] = None,
    ):
        """Process a key-value pair entry"""
        if value_type is None:
            value_type = f.read(1)[0]

        key = self._read_string(f)

        # Read value based on value type
        if value_type == RDBEncoding.STRING:
            value = self._read_string(f)
        else:
            # For now, just read as string, but in a complete implementation
            # we'd handle different value types (LIST, SET, HASH, etc.)
            value = self._read_string(f)

        self.data.setdefault(self.current_db, {})[key] = {
            "value": value,
            "type": value_type,
            "expire_at": expire_time,
        }

    def _read_length(self, f: BinaryIO) -> int:
        """Read a length-encoded integer"""
        first_byte = f.read(1)[0]

        # Check the first two bits (00, 01, 10, 11)
        bits = first_byte >> 6

        if bits == 0:  # 00xxxxxx
            return first_byte & 0x3F
        elif bits == 1:  # 01xxxxxx
            next_byte = f.read(1)[0]
            return ((first_byte & 0x3F) << 8) | next_byte
        elif bits == 2:  # 10xxxxxx
            # Discard the remaining 6 bits and read a 32-bit integer
            return struct.unpack(">I", f.read(4))[0]
        else:  # 11xxxxxx
            remaining = first_byte & 0x3F
            if remaining == 0:  # Read 8 bit integer
                return struct.unpack("B", f.read(1))[0]
            elif remaining == 1:  # Read 16 bit integer
                return struct.unpack(">H", f.read(2))[0]
            elif remaining == 2:  # Read 32 bit integer
                return struct.unpack(">I", f.read(4))[0]
            else:
                raise ValueError(f"Invalid length encoding: {first_byte}")

    def _read_string(self, f: BinaryIO) -> str:
        """Read a Redis string encoding"""
        first_byte = f.read(1)[0]
        bits = first_byte >> 6

        if bits == 3:  # 11xxxxxx
            special_format = first_byte & 0x3F
            if special_format == 0:  # 8 bit integer
                return str(struct.unpack("B", f.read(1))[0])
            elif special_format == 1:  # 16 bit integer
                return str(struct.unpack(">H", f.read(2))[0])
            elif special_format == 2:  # 32 bit integer
                return str(struct.unpack(">I", f.read(4))[0])
            elif special_format == 3:  # LZF compressed string
                compressed_len = self._read_length(f)
                uncompressed_len = self._read_length(f)
                compressed_str = f.read(compressed_len)  # noqa: F841
                # TODO: Implement LZF decompression
                return f"<LZF compressed string of length {uncompressed_len}>"
            else:
                raise ValueError(f"Invalid string encoding: {special_format}")

        # Reset file position and read as a length-prefixed string
        f.seek(-1, 1)  # Go back one byte
        length = self._read_length(f)
        return f.read(length).decode("utf-8")

    def get_value(self, db: int, key: str) -> Any:
        """Get a value from the parsed data"""
        if db not in self.data or key not in self.data[db]:
            return None

        entry = self.data[db][key]
        if entry["expire_time"] and entry["expire_time"] < datetime.now().timestamp():
            return None
        return entry["value"]

    def get_aux_fields(self) -> Dict[str, str]:
        """Get all auxiliary fields"""
        return self.aux_fields


def main():
    # Example usage
    parser = RDBParser("../rdb_example/dump.rdb")
    data = parser.parse()

    # Print auxiliary fields
    print("Auxiliary Fields:")
    for key, value in parser.get_aux_fields().items():
        print(f"  {key}: {value}")

    # Print all databases and their contents
    for db, contents in data.items():
        print(f"\nDatabase {db}:")
        for key, value in contents.items():
            print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
