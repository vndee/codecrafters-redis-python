import unittest
from app.resp import RESPParser


class TestRespParser(unittest.TestCase):
    def test_parse_multiple_object_in_one_segment(self):
        data = b"+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
        parser = RESPParser()
        objects = parser.parse(data)
        print(objects)
