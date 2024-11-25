import uuid
import asyncio
import argparse
from enum import StrEnum
from typing import Any, Dict, Set, Optional
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
        self.__slave_connections: Set[asyncio.StreamWriter] = set()
        self.__master_connection = None
        self.__master_listener = None
        self.__repl_ack_offset = 0

        self.__replicaof = replicaof

    async def initialize(self):
        """
        Perform async initialization tasks.
        """
        if self.__replicaof:
            success = await self.__ping_master_node(self.__replicaof)
            if success:
                reader, writer = self.__master_connection
                self.__master_listener = asyncio.create_task(self.handle_master_message(reader, writer))
                print(f"Connected to master: {self.__replicaof}")
            else:
                print(f"Failed to connect to master: {self.__replicaof}")

    @staticmethod
    def __generate_master_replid() -> str:
        """
        Generate a unique master replication ID (pseudo random alphanumeric string of 40 characters).
        :return:
        """
        return uuid.uuid4().hex

    async def __ping_master_node(self, master_address: str) -> bool:
        """
        Ping the master node asynchronously to check if it is alive.
        :param master_address: Address of the master node.
        :return: True if the master node is alive, False otherwise.
        """
        master_host, master_port = master_address.split(" ")

        try:
            reader, writer = await asyncio.open_connection(
                master_host,
                int(master_port)
            )

            async def send_command(command: RESPArray) -> Optional[bytes]:
                try:
                    writer.write(command.serialize())
                    await writer.drain()
                    return await reader.readline()
                except Exception as e:
                    print(f"Error sending command: {e}")
                    return None

            # Send PING
            response = await send_command(RESPArray([RESPBulkString("PING")]))
            if not response or response != b"+PONG\r\n":
                writer.close()
                await writer.wait_closed()
                return False

            # Send REPLCONF listening-port
            replconf_listening_port = RESPArray([
                RESPBulkString("REPLCONF"),
                RESPBulkString("listening-port"),
                RESPBulkString(str(self.port))
            ])
            response = await send_command(replconf_listening_port)
            if not response or response != b"+OK\r\n":
                writer.close()
                await writer.wait_closed()
                return False

            # Send REPLCONF capa psync2
            replconf_capa_psync2 = RESPArray([
                RESPBulkString("REPLCONF"),
                RESPBulkString("capa"),
                RESPBulkString("psync2")
            ])
            response = await send_command(replconf_capa_psync2)
            if not response or response != b"+OK\r\n":
                writer.close()
                await writer.wait_closed()
                return False

            # Send PSYNC command
            psync_command = RESPArray([
                RESPBulkString("PSYNC"),
                RESPBulkString("?"),
                RESPBulkString("-1")
            ])
            writer.write(psync_command.serialize())
            await writer.drain()
            recv_data = await reader.read(1024)
            recv_resp = self.resp_parser.parse(recv_data)
            print(f"PSYNC response parsed: {recv_resp}")

            for command in recv_resp:
                if command.type == RESPObjectType.ARRAY:
                    await self.handle_command(writer, command, len(recv_data), True)
                elif command.type == RESPObjectType.BULK_BYTES:
                    # TODO: Handle RDB data
                    pass
                elif command.type == RESPObjectType.SIMPLE_STRING:
                    if command.value.startswith("FULLRESYNC"):
                        parts = command.value.split(" ")
                        master_replid, master_repl_offset = parts[1:]
                        self.__repl_info.master_replid = master_replid
                        self.__repl_info.master_repl_offset = int(master_repl_offset)
                        print(f"Master replication ID: {self.__repl_info.master_replid}")
                        print(f"Master replication offset: {self.__repl_info.master_repl_offset}")

            # Store the connection
            self.__master_connection = (reader, writer)
            return True

        except Exception as e:
            print(f"Connection error: {e}")
            return False

    async def handle_master_message(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while True:
                data = await reader.read(1024)
                print(f"Received from master: {data} - length: {len(data)}")
                if not data:
                    break

                commands = self.resp_parser.parse(data)

                for cmd in commands:
                    if isinstance(cmd, RESPSimpleString) and cmd.value == "PING":
                        self.__repl_ack_offset = self.__repl_ack_offset + len(data)
                    elif cmd.type == RESPObjectType.ARRAY:
                        await self.handle_command(writer, cmd, len(data), True)
                    else:
                        raise NotImplementedError(f"Unsupported command: {cmd}")

        except Exception as e:
            print(f"Error handling master message: {str(e)}")
        finally:
            print("Closing connection to master")
            writer.close()
            await writer.wait_closed()

    async def __propagate_to_slaves(self, data: RESPObject):
        """
        Propagate the data to all connected slaves.
        :param data: Data to propagate.
        """
        for slave_connection in self.__slave_connections:
            try:
                print(f"Propagating data to slave: {slave_connection.get_extra_info('peername')} - {data}")
                await self.__send_data(slave_connection, data)
            except Exception as e:
                print(f"Error sending data to slave: {str(e)}")

    async def handle_command(self, writer: asyncio.StreamWriter, data: RESPObject, data_byte_size: int, is_master_command: bool = False) -> None:
        if not isinstance(data, RESPArray):
            await self.__send_data(writer, RESPSimpleString("ERR unknown command"))

        print(f"Handling command: {data} - is_master_command: {is_master_command}")
        command = data.value[0].value.lower()
        match command:
            case RedisCommand.PING:
                if not is_master_command:
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

                resp = RESPSimpleString(self.__data_store.set(key, value, **args))
                if not is_master_command:
                    await self.__send_data(writer, resp)

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
                attr = data.value[1].value.lower()

                if self.__repl_info.role == RedisReplicationRole.MASTER:
                    if attr.lower() == "listening-port":
                        self.__slave_connections.add(writer)
                        print(f"New connected slaves: {writer.get_extra_info('peername')} - listening port: {data.value[2].value}")
                    elif attr.lower() == "capa":
                        capa = data.value[2].value.lower()
                        if capa == "psync2":
                            pass
                    else:
                        raise NotImplementedError(f"REPLCONF {attr} is not implemented")

                    await self.__send_data(writer, RESPSimpleString("OK"))
                else:
                    if attr.lower() == "getack":
                        await self.__send_data(writer, RESPArray([RESPBulkString("REPLCONF"), RESPBulkString("ACK"), RESPBulkString(str(self.__repl_ack_offset))]))
                    else:
                        raise NotImplementedError(f"REPLCONF {attr} is not implemented")

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

        self.__repl_ack_offset = self.__repl_ack_offset + data_byte_size

    async def __send_data(self, writer: asyncio.StreamWriter, data: RESPObject | bytes) -> None:
        print(f"Sending data: {data}")
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

                commands = self.resp_parser.parse(data)
                print(f"Received {data} from {addr}")

                for cmd in commands:
                    if isinstance(cmd, RESPSimpleString) and cmd.value == "PING":
                        response = b"+PONG\r\n"
                        writer.write(response)
                        await writer.drain()
                    elif cmd.type == RESPObjectType.ARRAY:
                        await self.handle_command(writer, cmd, len(data))
                    else:
                        print(f"Received unknown command: {cmd.serialize()}")

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
        await self.initialize()

        server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )

        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')

        try:
            async with server:
                await server.serve_forever()
        finally:
            server.close()
            await server.wait_closed()

            if self.__master_listener:
                self.__master_listener.cancel()

                try:
                    await self.__master_listener
                except asyncio.CancelledError:
                    pass


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
