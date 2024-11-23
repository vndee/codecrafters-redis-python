import socket  # noqa: F401
import asyncio  # noqa: F401


async def handle_client(reader, writer):
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break  # connection lost
            writer.write(b"+PONG\r\n")
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    server = await asyncio.start_server(handle_client, "localhost", "6379")
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())