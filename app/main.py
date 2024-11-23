import time
import asyncio
from typing import Tuple, Any, Dict, List

from app.resp import (
    RESPParser,
    RESPObjectType,
    RESPObject,
    RESPProtocolVersion,
    RESPSimpleString,
    RESPInteger,
    RESPBulkString,
    RESPArray,
)
from app.data import RedisCommand, RedisDataObject, RedisDataStore


class RedisServer:
    def __init__(self, host='127.0.0.1', port=6379):
        self.host = host
        self.port = port
        self.resp_parser = RESPParser()

        self.__data_store = RedisDataStore()

    def handle_command(self, data: RESPObject) -> RESPObject:
        if not isinstance(data, RESPArray):
            return RESPSimpleString("ERR unknown command")

        command = data.value[0].value.lower()
        match command:
            case RedisCommand.PING:
                return RESPSimpleString("PONG")

            case RedisCommand.ECHO:
                return RESPBulkString(data.value[1].value)

            case RedisCommand.SET:
                key = data.value[1].value
                value = data.value[2].value

                i = 3
                args: Dict[str, Any] = {}
                while i < len(data.value):
                    arg_name = data.value[i].value.lower()
                    if arg_name in ("ex", "px", "exat", "pxat"):
                        args[arg_name] = int(data.value[i + 1].value)
                        i = i + 2
                    elif arg_name in ("nx", "xx", "keepttl", "get"):
                        args[arg_name] = True
                        i = i + 1
                    else:
                        return RESPSimpleString("ERR syntax error")

                return RESPSimpleString(self.__data_store.set(key, value, **args))

            case RedisCommand.GET:
                key = data.value[1].value
                return RESPBulkString(self.__data_store.get(key))
            case _:
                return RESPSimpleString("ERR unknown command")

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        print(f"New connection from {addr}")

        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break

                data = self.resp_parser.parse(data)
                print(f"Received {data} from {addr}")

                if isinstance(data, RESPSimpleString) and data.value == "PING":
                    response = b"+PONG\r\n"
                elif data.type == RESPObjectType.ARRAY:
                    response = self.handle_command(data).serialize()
                else:
                    response = b"-ERR unknown command\r\n"

                writer.write(response)
                await writer.drain()

        except Exception as e:
            print(f"Error handling client {addr}: {str(e)}")
        finally:
            print(f"Closing connection from {addr}")
            writer.close()
            try:
                await writer.wait_closed()
            except Exception as e:
                print(f"Error while closing connection: {str(e)}")

    async def start(self):
        server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )

        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')

        async with server:
            await server.serve_forever()


def main():
    redis_server = RedisServer()
    asyncio.run(redis_server.start())


if __name__ == "__main__":
    main()
