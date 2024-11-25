import socket
import uuid
import asyncio
import argparse
from enum import StrEnum
from typing import Any, Dict, List
from dataclasses import dataclass

from app.resp import (
    RESPParser,
    RESPObjectType,
    RESPObject,
    RESPSimpleString,
    RESPBulkString,
    RESPArray,
    RESPBytesLength
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
            else:
                print(f"Connected to master node {replicaof}")
                self.__replicaof = replicaof

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
        self.__slave_connections = []
        self.__master_connection = None

    @staticmethod
    def __generate_master_replid() -> str:
        """
        Generate a unique master replication ID (pseudo random alphanumeric string of 40 characters).
        :return:
        """
        return uuid.uuid4().hex

    def __send_socket(self, client: socket.socket, data: RESPObject) -> RESPObject:
        """
        Send data to the client socket.
        :param client: Client socket.
        :param data: Data to send.
        """
        client.send(data.serialize())
        return self.resp_parser.parse(client.recv(1024))

    def __ping_master_node(self, master_address: str) -> bool:
        """
        Ping the master node to check if it is alive.
        :param master_address: Address of the master node.
        :return: True if the master node is alive, False otherwise.
        """
        master_host, master_port = master_address.split(" ")
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((master_host, int(master_port)))
            response = self.__send_socket(client, RESPArray([RESPBulkString("PING")]))
            if response.serialize() != b"+PONG\r\n":
                return False

            replconf_listening_port = RESPArray([RESPBulkString("REPLCONF"), RESPBulkString("listening-port"), RESPBulkString(str(self.port))])
            response = self.__send_socket(client, replconf_listening_port)
            if response.serialize() != b"+OK\r\n":
                return False

            replconf_capa_psync2 = RESPArray([RESPBulkString("REPLCONF"), RESPBulkString("capa"), RESPBulkString("psync2")])
            response = self.__send_socket(client, replconf_capa_psync2)
            if response.serialize() != b"+OK\r\n":
                return False

            psync_command = RESPArray([RESPBulkString("PSYNC"), RESPBulkString("?"), RESPBulkString("-1")])
            response = self.__send_socket(client, psync_command)
            # TODO: implement PSYNC response parsing

            self.__master_connection = client
            return True
        except Exception as _:
            return False

    def __send_to_master(self, data: RESPObject) -> RESPObject:
        """
        Send the data to the master node.
        :param data: Data to send.
        """
        master_host, master_port = self.__replicaof.split(" ")
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((master_host, int(master_port)))
            client.send(data.serialize())
            response = client.recv(1024)
            return self.resp_parser.parse(response)
        except Exception as e:
            print(f"Error sending data to master: {str(e)}")
            return RESPSimpleString("ERR error sending data to master")

    async def __propagate_to_slaves(self, data: RESPObject):
        """
        Propagate the data to all connected slaves.
        :param data: Data to propagate.
        """
        for slave_connection in self.__slave_connections:
            try:
                await self.__send_data(slave_connection, data)
            except Exception as e:
                print(f"Error sending data to slave: {str(e)}")

    async def handle_command(self, writer: asyncio.StreamWriter, data: RESPObject) -> None:
        if not isinstance(data, RESPArray):
            await self.__send_data(writer, RESPSimpleString("ERR unknown command"))

        command = data.value[0].value.lower()
        match command:
            case RedisCommand.PING:
                await self.__send_data(writer, RESPSimpleString("PONG"))

            case RedisCommand.ECHO:
                await self.__send_data(writer, RESPBulkString(data.value[1].value))

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
                        await self.__send_data(writer, RESPSimpleString("ERR syntax error"))

                await self.__send_data(writer, RESPSimpleString(self.__data_store.set(key, value, **args)))
                if self.__repl_info.role == RedisReplicationRole.MASTER:
                    await self.__propagate_to_slaves(data)

            case RedisCommand.GET:
                key = data.value[1].value
                await self.__send_data(writer, RESPBulkString(self.__data_store.get(key)))

            case RedisCommand.CONFIG:
                method = data.value[1].value.lower()
                if method == RedisCommand.GET:
                    param = data.value[2].value.lower()
                    if param == "dir":
                        await self.__send_data(writer, RESPArray([RESPBulkString("dir"), RESPBulkString(self.__data_store.dir)]))
                    if param == "dbfilename":
                        await self.__send_data(writer, RESPArray([RESPBulkString("dbfilename"), RESPBulkString(self.__data_store.dbfilename)]))

            case RedisCommand.KEYS:
                pattern = data.value[1].value
                await self.__send_data(writer, RESPArray([RESPBulkString(key) for key in self.__data_store.keys(pattern)]))

            case RedisCommand.INFO:
                await self.__send_data(writer, self.__repl_info.serialize())

            case RedisCommand.REPLCONF:
                if self.__repl_info.role == RedisReplicationRole.MASTER:
                    attr = data.value[1].value.lower()
                    if attr == "listening-port":
                        self.__slave_connections.append(writer)
                        print(f"Connected slaves: {writer.get_extra_info('peername')}")
                    elif attr == "capa":
                        capa = data.value[2].value.lower()
                        if capa == "psync2":
                            pass
                    else:
                        raise NotImplementedError(f"REPLCONF {attr} is not implemented")

                    await self.__send_data(writer, RESPSimpleString("OK"))

            case RedisCommand.PSYNC:
                repl_id = data.value[1].value
                repl_offset = int(data.value[2].value)

                if repl_id != "?" or repl_offset != -1:
                    raise NotImplementedError("Only PSYNC with ? and -1 is supported")

                rdb_content = self.__data_store.dump_to_rdb()
                await self.__send_data(writer, RESPSimpleString(f"FULLRESYNC {self.__repl_info.master_replid} {self.__repl_info.master_repl_offset}"))
                await self.__send_data(writer, RESPBytesLength(len(rdb_content)))
                await self.__send_data(writer, rdb_content)
            case _:
                await self.__send_data(writer, RESPSimpleString("ERR unknown command"))

    async def __send_data(self, writer: asyncio.StreamWriter, data: RESPObject | bytes) -> None:
        if isinstance(data, RESPObject):
            writer.write(data.serialize())
        elif isinstance(data, bytes):
            writer.write(data)
        else:
            raise ValueError(f"Invalid response type: {type(data)}")

        await writer.drain()

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
                    writer.write(response)
                    await writer.drain()
                elif data.type == RESPObjectType.ARRAY:
                    await self.handle_command(writer, data)
                else:
                    raise NotImplementedError(f"Unsupported command: {data}")

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
