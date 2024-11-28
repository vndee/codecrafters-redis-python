import pytest
from app.resp import (
    RESPParser,
    RESPSimpleString,
    RESPError,
    RESPInteger,
    RESPBulkString,
    RESPArray,
)


def test_simple_string(parser):
    result = parser.parse(b"+OK\r\n")
    assert len(result) == 1
    assert isinstance(result[0], RESPSimpleString)
    assert result[0].value == "OK"


def test_error(parser):
    result = parser.parse(b"-Error message\r\n")
    assert len(result) == 1
    assert isinstance(result[0], RESPError)
    assert result[0].value == "Error message"


def test_integer(parser):
    result = parser.parse(b":1000\r\n")
    assert len(result) == 1
    assert isinstance(result[0], RESPInteger)
    assert result[0].value == 1000


def test_bulk_string(parser):
    result = parser.parse(b"$5\r\nhello\r\n")
    assert len(result) == 1
    assert isinstance(result[0], RESPBulkString)
    assert result[0].value == "hello"


def test_null_bulk_string(parser):
    result = parser.parse(b"$-1\r\n")
    assert len(result) == 1
    assert isinstance(result[0], RESPBulkString)
    assert result[0].value is None


def test_empty_array(parser):
    result = parser.parse(b"*0\r\n")
    assert len(result) == 1
    assert isinstance(result[0], RESPArray)
    assert len(result[0].value) == 0


def test_nested_array(parser):
    data = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
    result = parser.parse(data)
    assert len(result) == 1
    assert isinstance(result[0], RESPArray)
    assert len(result[0].value) == 2
    assert result[0].value[0].value == "hello"
    assert result[0].value[1].value == "world"


def test_complex_array(parser):
    data = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
    result = parser.parse(data)
    assert len(result) == 1
    assert isinstance(result[0], RESPArray)
    assert len(result[0].value) == 3
    assert result[0].value[0].value == "REPLCONF"
    assert result[0].value[1].value == "GETACK"
    assert result[0].value[2].value == "*"


def test_multiple_commands(parser):
    data = b"+OK\r\n:1000\r\n"
    result = parser.parse(data)
    assert len(result) == 2
    assert isinstance(result[0], RESPSimpleString)
    assert result[0].value == "OK"
    assert isinstance(result[1], RESPInteger)
    assert result[1].value == 1000


def test_incomplete_data(parser):
    result = parser.parse(b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r")
    assert len(result) == 0


@pytest.mark.parametrize(
    "obj,expected",
    [
        (RESPSimpleString(value="OK"), b"+OK\r\n"),
        (RESPError(value="Error"), b"-Error\r\n"),
        (RESPInteger(value=1000), b":1000\r\n"),
        (RESPBulkString(value="hello"), b"$5\r\nhello\r\n"),
        (RESPBulkString(value=None), b"$-1\r\n"),
        (RESPArray(value=[]), b"*0\r\n"),
    ],
)
def test_serialization(obj, expected):
    assert obj.serialize() == expected


@pytest.fixture
def parser():
    return RESPParser()
