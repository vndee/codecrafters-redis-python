import asyncio
import json
from datetime import datetime


class AsyncTCPServer:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.clients = set()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # Get client address
        addr = writer.get_extra_info('peername')
        print(f"New connection from {addr}")
        self.clients.add(writer)

        try:
            while True:
                # Read data from client
                data = await reader.read(1024)
                if not data:
                    break

                # Process the received data
                message = data.decode()
                try:
                    # Try to parse as JSON
                    request = json.loads(message)
                    response = await self.process_request(request)
                except json.JSONDecodeError:
                    response = {"status": "error", "message": "Invalid JSON format"}

                # Send response back to client
                response_data = json.dumps(response).encode()
                writer.write(response_data + b'\n')
                await writer.drain()

        except Exception as e:
            print(f"Error handling client {addr}: {str(e)}")
        finally:
            # Clean up when client disconnects
            self.clients.remove(writer)
            writer.close()
            await writer.wait_closed()
            print(f"Connection closed for {addr}")

    async def process_request(self, request: dict) -> dict:
        """Process different types of requests based on the 'command' field"""
        command = request.get('command', '')

        if command == 'time':
            return {
                "status": "success",
                "command": command,
                "result": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        elif command == 'echo':
            return {
                "status": "success",
                "command": command,
                "result": request.get('data', '')
            }
        else:
            return {
                "status": "error",
                "message": f"Unknown command: {command}"
            }

    async def broadcast(self, message: str):
        """Broadcast a message to all connected clients"""
        if not self.clients:
            return

        data = json.dumps({"type": "broadcast", "message": message}).encode() + b'\n'
        for client in self.clients:
            try:
                client.write(data)
                await client.drain()
            except Exception as e:
                print(f"Error broadcasting to client: {str(e)}")

    async def start(self):
        """Start the TCP server"""
        server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )

        addr = server.sockets[0].getsockname()
        print(f'Server running on {addr}')

        async with server:
            await server.serve_forever()


# Example usage
async def main():
    server = AsyncTCPServer('127.0.0.1', 8888)

    # Schedule a periodic broadcast (optional)
    async def periodic_broadcast():
        while True:
            await server.broadcast(f"Server time: {datetime.now().strftime('%H:%M:%S')}")
            await asyncio.sleep(60)  # Broadcast every minute

    # Run both the server and periodic broadcast
    await asyncio.gather(
        server.start(),
        periodic_broadcast()
    )


if __name__ == "__main__":
    asyncio.run(main())