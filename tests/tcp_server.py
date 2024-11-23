import asyncio
import socket

SERVER = "127.0.0.1"
PORT = 6379
PONG = "+PONG\r\n"
connections = 0


async def handle_client(client_socket, client_address):
    global connections
    connections += 1
    conn_no = connections
    print(f"[+] Client {conn_no} - {client_address} connected")
    loop = asyncio.get_event_loop()
    try:
        while req := await loop.sock_recv(client_socket, 1024):
            print("Received request", req, client_socket)
            await loop.sock_sendall(client_socket, PONG.encode())
    except Exception as e:
        print(f"Error handling client {conn_no}: {e}")
    finally:
        client_socket.close()
        print(f"[+] Client {conn_no} - {client_address} disconnected")


async def server_start(server_socket):
    server_socket.listen()
    print(f"[+] SERVER LISTENING on {SERVER}:{PORT}")
    server_socket.setblocking(False)
    loop = asyncio.get_event_loop()

    while True:
        client_socket, client_address = await loop.sock_accept(server_socket)
        loop.create_task(handle_client(client_socket, client_address))


def server_init():
    ADDR = (SERVER, PORT)
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(ADDR)
    return server_socket


def main():
    server_socket = server_init()
    asyncio.run(server_start(server_socket))


if __name__ == "__main__":
    main()