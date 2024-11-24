import uuid
import asyncio
import argparse
from enum import StrEnum
from typing import Any, Dict
from dataclasses import dataclass

from app.resp import (
    RESPParser,
    RESPObjectType,
    RESPObject,
    RESPSimpleString,
    RESPBulkString,
    RESPArray,
)
from app.data import RedisCommand, RedisDataStore


class RedisReplicationRole(StrEnum):
    MASTER = "master"
    SLAVE = "slave"
    REPLICA = "replica"


@dataclass
class RedisReplicationInformation:
    role: RedisReplicationRole
    connected_slaves: int
    master_replid: str
    master_repl_offset: int
    second_repl_offset: int
    repl_backlog_active: bool
    repl_backlog_size: int
    repl_backlog_first_byte_offset: int
    repl_backlog_histlen: int

    def serialize(self) -> RESPBulkString:
        """
        Serialize the replication information with the format expected by the INFO command, where each line is a key-value pair
        separate by a colon. The key is the attribute name and the value is the attribute value.
        """
        return RESPBulkString(
            f"role:{self.role}\n"
            f"connected_slaves:{self.connected_slaves}\n"
            f"master_replid:{self.master_replid}\n"
            f"master_repl_offset:{self.master_repl_offset}\n"
            f"second_repl_offset:{self.second_repl_offset}\n"
            f"repl_backlog_active:{1 if self.repl_backlog_active else 0}\n"
            f"repl_backlog_size:{self.repl_backlog_size}\n"
            f"repl_backlog_first_byte_offset:{self.repl_backlog_first_byte_offset}\n"
            f"repl_backlog_histlen:{self.repl_backlog_histlen}\n"
        )


class RedisServer:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        dir: str = "/tmp/redis-files",
        dbfilename: str = "dump.rdb",
        replicaof: str = None,
    ):
        self.host = host
        self.port = port
        self.resp_parser = RESPParser()

        self.__data_store = RedisDataStore(dir=dir, dbfilename=dbfilename)

        if replicaof is not None:
            if not self.__ping_master_node(replicaof):
                raise ValueError(f"Could not connect to master node {replicaof}")

        self.__repl_info = RedisReplicationInformation(
            role=RedisReplicationRole.MASTER if replicaof is None else RedisReplicationRole.SLAVE,
            connected_slaves=0,
            master_replid=self.__generate_master_replid(),
            master_repl_offset=0,
            second_repl_offset=0,
            repl_backlog_active=False,
            repl_backlog_size=0,
            repl_backlog_first_byte_offset=0,
            repl_backlog_histlen=0,
        )

    @staticmethod
    def __generate_master_replid() -> str:
        """
        Generate a unique master replication ID (pseudo random alphanumeric string of 40 characters).
        :return:
        """
        return uuid.uuid4().hex

    @staticmethod
    def __ping_master_node(master_address: str) -> bool:
        """
        Ping the master node to check if it is alive.
        :param master_address: Address of the master node.
        :return: True if the master node is alive, False otherwise.
        """
        master_host, master_port = master_address.split(" ")
        try:
            client = asyncio.open_connection(master_host, master_port)
            client.close()
            return True
        except Exception as _:
            return False

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

            case RedisCommand.CONFIG:
                method = data.value[1].value.lower()
                if method == RedisCommand.GET:
                    param = data.value[2].value.lower()
                    if param == "dir":
                        return RESPArray([RESPBulkString("dir"), RESPBulkString(self.__data_store.dir)])
                    if param == "dbfilename":
                        return RESPArray([RESPBulkString("dbfilename"), RESPBulkString(self.__data_store.dbfilename)])
                    return RESPSimpleString("ERR unknown parameter")

            case RedisCommand.KEYS:
                pattern = data.value[1].value
                return RESPArray([RESPBulkString(key) for key in self.__data_store.keys(pattern)])

            case RedisCommand.INFO:
                return self.__repl_info.serialize()

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Redis server")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host")
    parser.add_argument("--port", type=int, default=6379, help="Port")
    parser.add_argument("--dir", type=str, default="rdb", help="Directory to store data")
    parser.add_argument("--dbfilename", type=str, default="dump.rdb", help="Database filename")
    parser.add_argument("--replicaof", type=str, help="Replicate another Redis server", default=None)
    args = parser.parse_args()

    redis_server = RedisServer(dir=args.dir, dbfilename=args.dbfilename, host=args.host, port=args.port, replicaof=args.replicaof)
    asyncio.run(redis_server.start())
