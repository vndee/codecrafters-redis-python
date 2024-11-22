import os
import socket  # noqa: F401
def main():
    if os.name == "posix":
        server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    else:
        server_socket = socket.create_server(("localhost", 6379), reuse_port=False)
    conn, addr = server_socket.accept()  # wait for client

    while True:
        try:
            msg = conn.recv(1024)
            conn.sendall(b"+PONG\r\n")
        except ConnectionAbortedError:
            break

if __name__ == "__main__":
    main()