import socket


def send_command(client, cmd: bytes):
    print(f"Sending: {cmd}")
    client.send(cmd)

    response = client.recv(1024)
    print(f"Received: {response}")


if __name__ == "__main__":
    commands = [
        b"+PING\r\n",
        b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n",
        b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n",
        b"*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\npx\r\n$3\r\n100\r\n",
        b"*2\r\n$3\r\nGET\r\n$3\r\nfoos\r\n",
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\ndir\r\n",
        b"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n",
    ]

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(('127.0.0.1', 6379))

    for cmd in commands:
        send_command(client, cmd)
