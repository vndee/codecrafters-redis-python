import uuid
import time
import asyncio
import argparse
from enum import StrEnum
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

from app.resp import (
    RESPParser,
    RESPObjectType,
    RESPObject,
    RESPSimpleString,
    RESPBulkString,
    RESPArray,
    RESPBytesLength,
    RESPInteger,
    RESPSimpleError,
)
from app.data import RedisCommand, RedisDataStore, RedisError
from app.commands import ARITIES, execute, validate, xread_options


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
            value=(
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
        )


class RedisServer:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        dir: str = "/tmp/redis-files",
        dbfilename: str = "dump.rdb_example",
        replicaof: str | None = None,
    ):
        self.host = host
        self.port = port
        self.resp_parser = RESPParser()

        self.__data_store = RedisDataStore(dir=dir, dbfilename=dbfilename)

        self.__repl_info = RedisReplicationInformation(
            role=RedisReplicationRole.MASTER
            if replicaof is None
            else RedisReplicationRole.SLAVE,
            connected_slaves=0,
            master_replid=self.__generate_master_replid(),
            master_repl_offset=0,
            second_repl_offset=0,
            repl_backlog_active=False,
            repl_backlog_size=0,
            repl_backlog_first_byte_offset=0,
            repl_backlog_histlen=0,
        )
        self.__master_connection: Tuple[
            asyncio.StreamReader | None, asyncio.StreamWriter | None
        ] = (
            None,
            None,
        )

        self.__master_listener = None
        self.__repl_ack_offset = 0

        self.__replicaof = replicaof
        self.__client_write_offsets: Dict[asyncio.StreamWriter, int] = {}
        self.__replica_acks: Dict[
            asyncio.StreamWriter, Tuple[asyncio.StreamReader, int]
        ] = {}
        self.__request_acks: Dict[asyncio.StreamWriter, bool] = {}

        self.__is_command_in_queue: Dict[asyncio.StreamWriter, bool] = {}
        self.__command_queue: Dict[asyncio.StreamWriter, List[RESPObject]] = {}
        self.__transaction_dirty: set[asyncio.StreamWriter] = set()
        self.__stream_changed = asyncio.Event()

    async def initialize(self):
        """
        Perform async initialization tasks.
        """
        if self.__replicaof:
            success = await self.__ping_master_node(self.__replicaof)
            if success:
                reader, writer = self.__master_connection
                self.__master_listener = asyncio.create_task(
                    self.handle_master_message(reader, writer)
                )
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
                master_host, int(master_port)
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
            response = await send_command(
                RESPArray(value=[RESPBulkString(value="PING")])
            )
            if not response or response != b"+PONG\r\n":
                writer.close()
                await writer.wait_closed()
                return False

            # Send REPLCONF listening-port
            replconf_listening_port = RESPArray(
                value=[
                    RESPBulkString(value="REPLCONF"),
                    RESPBulkString(value="listening-port"),
                    RESPBulkString(value=str(self.port)),
                ]
            )
            response = await send_command(replconf_listening_port)
            if not response or response != b"+OK\r\n":
                writer.close()
                await writer.wait_closed()
                return False

            # Send REPLCONF capa psync2
            replconf_capa_psync2 = RESPArray(
                value=[
                    RESPBulkString(value="REPLCONF"),
                    RESPBulkString(value="capa"),
                    RESPBulkString(value="psync2"),
                ]
            )
            response = await send_command(replconf_capa_psync2)
            if not response or response != b"+OK\r\n":
                writer.close()
                await writer.wait_closed()
                return False

            # Send PSYNC command
            psync_command = RESPArray(
                value=[
                    RESPBulkString(value="PSYNC", bytes_length=5),
                    RESPBulkString(value="?", bytes_length=1),
                    RESPBulkString(value="-1", bytes_length=2),
                ],
                bytes_length=11,
            )
            writer.write(psync_command.serialize())
            await writer.drain()
            recv_data = await reader.read(1024)
            recv_resp = self.resp_parser.parse(recv_data)
            print(f"PSYNC response parsed: {recv_resp}")

            for command in recv_resp:
                if command.type == RESPObjectType.ARRAY:
                    await self.handle_command(reader, writer, command, True)
                elif command.type == RESPObjectType.BULK_BYTES:
                    # TODO: Handle RDB data
                    pass
                elif command.type == RESPObjectType.SIMPLE_STRING:
                    if command.value.startswith("FULLRESYNC"):  # type: ignore[union-attr]
                        parts = command.value.split(" ")  # type: ignore[union-attr]
                        master_replid, master_repl_offset = parts[1:]
                        self.__repl_info.master_replid = master_replid
                        self.__repl_info.master_repl_offset = int(master_repl_offset)
                        print(
                            f"Master replication ID: {self.__repl_info.master_replid}"
                        )
                        print(
                            f"Master replication offset: {self.__repl_info.master_repl_offset}"
                        )

            # Store the connection
            self.__master_connection = (reader, writer)
            return True

        except Exception as e:
            print(f"Connection error: {e}")
            return False

    async def handle_master_message(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        try:
            while True:
                data = await reader.read(1024)
                print(f"Received from master: {data.decode()} - length: {len(data)}")
                if not data:
                    break

                commands = self.resp_parser.parse(data)

                for cmd in commands:
                    if isinstance(cmd, RESPSimpleString) and cmd.value == "PING":
                        self.__repl_ack_offset = self.__repl_ack_offset + len(data)
                    elif cmd.type == RESPObjectType.ARRAY:
                        await self.handle_command(reader, writer, cmd, True)
                    elif cmd.type == RESPObjectType.BULK_BYTES:
                        # TODO: Handle RDB data
                        pass
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
        Propagate the data to all connected slaves with proper error handling.
        """
        failed_replicas = []

        for replica_writer in list(self.__replica_acks.keys()):
            if not replica_writer.is_closing():
                print(
                    f"Attempting to propagate data to replica: {replica_writer.get_extra_info('peername')}"
                )
                success = await self.__send_data(replica_writer, data)
                if not success:
                    print(
                        f"Failed to send data to replica {replica_writer.get_extra_info('peername')}, marking for removal"
                    )
                    failed_replicas.append(replica_writer)
            else:
                failed_replicas.append(replica_writer)

        # Clean up failed replicas
        for failed_writer in failed_replicas:
            print(
                f"Removing failed replica: {failed_writer.get_extra_info('peername')}"
            )
            self.__replica_acks.pop(failed_writer, None)
            try:
                failed_writer.close()
                await failed_writer.wait_closed()
            except Exception as e:
                print(f"Error closing failed replica connection: {e}")

    def __execute_data(self, args, applied_sets=None):
        try:
            result = execute(self.__data_store, args, applied_sets=applied_sets)
            if args[0].lower() == "xadd":
                self.__stream_changed.set()
            return result
        except RedisError as error:
            return RESPSimpleError(value=str(error))

    async def __forward_sets(self, writer, applied_sets, is_master_command):
        # Preserve the existing teaching server's SET replication path. Other
        # data types still have no replication or persistence implementation.
        if self.__repl_info.role != RedisReplicationRole.MASTER or is_master_command:
            return
        for args in applied_sets:
            data = RESPArray(value=[RESPBulkString(value=arg) for arg in args])
            self.__repl_info.master_repl_offset += data.serialized_bytes_length
            self.__client_write_offsets[writer] = self.__repl_info.master_repl_offset
            await self.__propagate_to_slaves(data)

    async def __local_command(self, reader, writer, data, is_master_command):
        args = [item.value for item in data.value]
        command = args[0].lower()
        queued = self.__is_command_in_queue.get(writer, False)
        try:
            validate(args)
        except RedisError as error:
            if queued:
                self.__transaction_dirty.add(writer)
            return RESPSimpleError(value=str(error))
        if command == "multi":
            if queued:
                return RESPSimpleError(value="ERR MULTI calls can not be nested")
            self.__is_command_in_queue[writer] = True
            self.__command_queue[writer] = []
            self.__transaction_dirty.discard(writer)
            return RESPSimpleString(value="OK")
        if command in {"exec", "discard"}:
            if not queued:
                return RESPSimpleError(value=f"ERR {command.upper()} without MULTI")
            self.__is_command_in_queue.pop(writer, None)
            commands = self.__command_queue.pop(writer, [])
            dirty = writer in self.__transaction_dirty
            self.__transaction_dirty.discard(writer)
            if command == "discard":
                return RESPSimpleString(value="OK")
            if dirty:
                return RESPSimpleError(
                    value="EXECABORT Transaction discarded because of previous errors."
                )
            # There is deliberately no await in this execution loop. Runtime
            # errors become array elements; later queued commands still execute.
            applied_sets = []
            results = [
                self.__execute_data([part.value for part in cmd.value], applied_sets)
                for cmd in commands
            ]
            await self.__forward_sets(writer, applied_sets, is_master_command)
            return RESPArray(value=results)
        if queued:
            self.__command_queue[writer].append(data)
            return RESPSimpleString(value="QUEUED")
        if command == "xread":
            try:
                keys, ids, count, block = xread_options(self.__data_store, args)
                deadline = time.monotonic() + block / 1000 if block else None
                while True:
                    self.__stream_changed.clear()
                    result = self.__data_store.xread(keys, ids, count)
                    if result.value is not None or block is None or reader.at_eof():
                        return result
                    remaining = deadline - time.monotonic() if deadline else None
                    if remaining is not None and remaining <= 0:
                        return RESPArray(value=None)
                    try:
                        # The bounded wait also notices disconnected clients.
                        await asyncio.wait_for(
                            self.__stream_changed.wait(),
                            min(remaining, 0.1) if remaining is not None else 0.1,
                        )
                    except asyncio.TimeoutError:
                        pass
            except RedisError as error:
                return RESPSimpleError(value=str(error))
        applied_sets = []
        result = self.__execute_data(args, applied_sets)
        await self.__forward_sets(writer, applied_sets, is_master_command)
        return result

    async def handle_command(  # type: ignore[return]
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        data: RESPObject,
        is_master_command: bool = False,
        is_return_resp: bool = False,
    ) -> None | RESPObject:
        if (
            not isinstance(data, RESPArray)
            or not data.value
            or any(
                not isinstance(item, RESPBulkString) or not isinstance(item.value, str)
                for item in data.value
            )
        ):
            await self.__send_data(
                writer, RESPSimpleError(value="ERR invalid command frame")
            )
            return None

        try:
            command = data.value[0].value.lower()
            if command in ARITIES or self.__is_command_in_queue.get(writer, False):
                response = await self.__local_command(
                    reader, writer, data, is_master_command
                )
                if is_master_command:
                    self.__repl_ack_offset += data.bytes_length
                elif is_return_resp:
                    return response
                else:
                    await self.__send_data(writer, response)
                return None

            match command:
                case RedisCommand.CONFIG:
                    method = data.value[1].value.lower()  # type: ignore[index]
                    if method == RedisCommand.GET:
                        param = data.value[2].value.lower()  # type: ignore[index]
                        if param == "dir":
                            resp = RESPArray(  # type: ignore[assignment]
                                value=[
                                    RESPBulkString(value="dir"),
                                    RESPBulkString(value=self.__data_store.dir),
                                ]
                            )
                            if not is_return_resp:
                                await self.__send_data(writer, resp)
                            else:
                                return resp
                        if param == "dbfilename":
                            resp = RESPArray(  # type: ignore[assignment]
                                value=[
                                    RESPBulkString(value="dbfilename"),
                                    RESPBulkString(value=self.__data_store.dbfilename),
                                ]
                            )
                            if not is_return_resp:
                                await self.__send_data(writer, resp)
                            else:
                                return resp

                case RedisCommand.INFO:
                    resp = self.__repl_info.serialize()  # type: ignore[assignment]
                    if not is_return_resp:
                        await self.__send_data(writer, resp)
                    else:
                        return resp

                case RedisCommand.REPLCONF:
                    attr = data.value[1].value.lower()  # type: ignore[index]

                    if self.__repl_info.role == RedisReplicationRole.MASTER:
                        if attr.lower() == "listening-port":
                            self.__replica_acks[writer] = (reader, 0)
                            print(
                                f"New connected slaves: {writer.get_extra_info('peername')} - listening port: {data.value[2].value}"  # type: ignore[index]
                            )
                        elif attr.lower() == "ack":
                            self.__replica_acks[writer] = (
                                reader,
                                int(data.value[2].value),  # type: ignore[index]
                            )
                            self.__request_acks[
                                writer.get_extra_info("peername")
                            ] = False
                            return None
                        elif attr.lower() == "capa":
                            capa = data.value[2].value.lower()  # type: ignore[index]
                            if capa == "psync2":
                                pass
                        else:
                            raise NotImplementedError(
                                f"REPLCONF {attr} is not implemented"
                            )

                        await self.__send_data(writer, RESPSimpleString(value="OK"))
                    else:
                        if attr.lower() == "getack":
                            await self.__send_data(
                                writer,
                                RESPArray(
                                    value=[
                                        RESPBulkString(value="REPLCONF"),
                                        RESPBulkString(value="ACK"),
                                        RESPBulkString(
                                            value=str(self.__repl_ack_offset)
                                        ),
                                    ]
                                ),
                            )
                            self.__request_acks[
                                writer.get_extra_info("peername")
                            ] = False
                        elif attr.lower() == "ack":
                            self.__replica_acks[writer] = (
                                reader,
                                int(data.value[2].value),  # type: ignore[index]
                            )
                            self.__request_acks[
                                writer.get_extra_info("peername")
                            ] = False
                        else:
                            raise NotImplementedError(
                                f"REPLCONF {attr} is not implemented"
                            )

                case RedisCommand.PSYNC:
                    repl_id = data.value[1].value  # type: ignore[index]
                    repl_offset = int(data.value[2].value)  # type: ignore[index]

                    if repl_id != "?" or repl_offset != -1:
                        raise NotImplementedError(
                            "Only PSYNC with ? and -1 is supported"
                        )

                    rdb_content = self.__data_store.dump_to_rdb()
                    await self.__send_data(
                        writer,
                        RESPSimpleString(
                            value=f"FULLRESYNC {self.__repl_info.master_replid} {self.__repl_info.master_repl_offset}"
                        ),
                    )
                    await self.__send_data(
                        writer, RESPBytesLength(value=len(rdb_content))
                    )
                    await self.__send_data(writer, rdb_content)

                case RedisCommand.WAIT:
                    num_replicas = int(data.value[1].value)  # type: ignore[index]
                    timeout_ms = int(data.value[2].value)  # type: ignore[index]

                    print(
                        f"WAIT command received: num_replicas={num_replicas}, timeout={timeout_ms}ms, master_offset={self.__repl_info.master_repl_offset}"
                    )

                    # Get the latest write offset for this client
                    client_offset = self.__repl_info.master_repl_offset

                    print(
                        f"Waiting for {num_replicas} replicas to acknowledge offset {client_offset}"
                    )
                    start_time = time.monotonic()

                    while True:
                        # Get current acks
                        active_replicas = [
                            (w, r, o)
                            for w, (r, o) in self.__replica_acks.items()
                            if not w.is_closing()
                        ]
                        acked_replicas = sum(
                            1
                            for _, _, offset in active_replicas
                            if offset >= client_offset
                        )

                        print(
                            f"Currently have {acked_replicas}/{len(active_replicas)} replicas acknowledged"
                        )

                        if acked_replicas >= num_replicas:
                            print(f"Required replicas reached: {acked_replicas}")
                            for (
                                replica_writer,
                                replica_reader,
                                current_offset,
                            ) in active_replicas:
                                self.__request_acks[
                                    replica_writer.get_extra_info("peername")
                                ] = False

                            if not is_return_resp:
                                await self.__send_data(
                                    writer, RESPInteger(value=acked_replicas)
                                )
                                return None
                            else:
                                return RESPInteger(value=acked_replicas)

                        if timeout_ms > 0:
                            elapsed_ms = (time.monotonic() - start_time) * 1000
                            if elapsed_ms >= timeout_ms:
                                print(
                                    f"Timeout reached after {elapsed_ms}ms, returning current acks: {acked_replicas}"
                                )
                                for (
                                    replica_writer,
                                    replica_reader,
                                    current_offset,
                                ) in active_replicas:
                                    self.__request_acks[
                                        replica_writer.get_extra_info("peername")
                                    ] = False

                                if not is_return_resp:
                                    await self.__send_data(
                                        writer, RESPInteger(value=acked_replicas)
                                    )
                                    return None
                                else:
                                    return RESPInteger(value=acked_replicas)

                        # Request ACKs from unacknowledged replicas
                        for (
                            replica_writer,
                            replica_reader,
                            current_offset,
                        ) in active_replicas:
                            print(self.__request_acks)
                            repl_addr = replica_writer.get_extra_info("peername")
                            if (
                                not self.__request_acks.get(repl_addr, False)
                                and self.__replica_acks.get(replica_writer, (None, 0))[
                                    1
                                ]
                                < client_offset
                            ):
                                print(
                                    f"Requesting ACK from replica {replica_writer.get_extra_info('peername')}"
                                )
                                self.__request_acks[repl_addr] = True
                                ack_cmd = RESPArray(
                                    value=[
                                        RESPBulkString(value="REPLCONF"),
                                        RESPBulkString(value="GETACK"),
                                        RESPBulkString(value="*"),
                                    ]
                                )
                                success = await self.__send_data(
                                    replica_writer, ack_cmd
                                )
                                if not success:
                                    print(
                                        f"Failed to send GETACK to replica {replica_writer.get_extra_info('peername')}"
                                    )

                        await asyncio.sleep(0.1)

                case _:
                    await self.__send_data(
                        writer, RESPSimpleError(value="ERR unknown command")
                    )

            if is_master_command:
                self.__repl_ack_offset = self.__repl_ack_offset + data.bytes_length
        except RedisError as e:
            await self.__send_data(writer, RESPSimpleError(value=str(e)))
        except Exception as e:
            print(f"Error handling command: {e}")

    @staticmethod
    def find_index_in_list(value: Any, lst: list) -> int:
        """
        Find the index of a value in a list.
        """
        try:
            return lst.index(value)
        except ValueError:
            return -1

    async def __send_data(
        self, writer: asyncio.StreamWriter, data: RESPObject | bytes
    ) -> bool:
        """
        Send data to a client/replica with error checking.
        Returns True if data was sent successfully, False otherwise.
        """
        try:
            if writer.is_closing():
                print(
                    f"Writer to {writer.get_extra_info('peername')} is closing, cannot send data"
                )
                return False

            if isinstance(data, RESPObject):
                serialized = data.serialize()
            elif isinstance(data, bytes):
                serialized = data
            else:
                raise ValueError(f"Invalid response type: {type(data)}")

            writer.write(serialized)
            await writer.drain()
            print(
                f"Successfully sent {len(serialized)} bytes to {writer.get_extra_info('peername')}: {serialized!r}"
            )
            return True
        except ConnectionError as e:
            print(
                f"Connection error while sending to {writer.get_extra_info('peername')}: {e}"
            )
            return False
        except Exception as e:
            print(f"Error sending data to {writer.get_extra_info('peername')}: {e}")
            return False

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        addr = writer.get_extra_info("peername")
        print(f"New connection from {addr}")

        parser = RESPParser()
        try:
            while True:
                data = await reader.read(1024 * 1024)
                if not data:
                    break

                commands = parser.feed(data)
                print(f"Received {data!r} from {addr}")

                for cmd in commands:
                    if isinstance(cmd, RESPSimpleString) and cmd.value == "PING":
                        response = b"+PONG\r\n"
                        writer.write(response)
                        await writer.drain()
                    elif cmd.type == RESPObjectType.ARRAY:
                        await self.handle_command(reader, writer, cmd)
                    else:
                        print(f"Received unknown command: {cmd.serialize()!r}")

                self.__repl_ack_offset = self.__repl_ack_offset + len(data)

        except Exception as e:
            print(f"Error handling client {addr}: {str(e)}")
        finally:
            print(f"Closing connection from {addr}")
            self.__client_write_offsets.pop(writer, None)
            self.__is_command_in_queue.pop(writer, None)
            self.__command_queue.pop(writer, None)
            self.__transaction_dirty.discard(writer)
            if writer in self.__replica_acks:
                print(
                    f"Cleaning up replica connection from {writer.get_extra_info('peername')}"
                )
                self.__replica_acks.pop(writer)

            writer.close()
            try:
                await writer.wait_closed()
            except Exception as e:
                print(f"Error while closing connection: {str(e)}")

    async def start(self):
        await self.initialize()

        server = await asyncio.start_server(self.handle_client, self.host, self.port)

        addr = server.sockets[0].getsockname()
        print(f"Serving on {addr}")

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
    parser.add_argument(
        "--dir", type=str, default="rdb_example", help="Directory to store data"
    )
    parser.add_argument(
        "--dbfilename", type=str, default="dump.rdb_example", help="Database filename"
    )
    parser.add_argument(
        "--replicaof", type=str, help="Replicate another Redis server", default=None
    )
    args = parser.parse_args()

    redis_server = RedisServer(
        dir=args.dir,
        dbfilename=args.dbfilename,
        host=args.host,
        port=args.port,
        replicaof=args.replicaof,
    )
    asyncio.run(redis_server.start())
