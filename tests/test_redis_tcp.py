import socket


def send_command():
    # Create socket
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(('127.0.0.1', 6379))

    # RESP formatted command
    # command = b"+PING\r\n"
    # command = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
    # command = b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"
    command = b"*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\npx\r\n$3\r\n100\r\n"
    print(f"Sending: {command}")

    # Send command
    client.send(command)

    # Receive response
    response = client.recv(1024)
    print(f"Received: {response}")

    command = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
    print(f"Sending: {command}")

    # Send command
    client.send(command)

    # Receive response
    response = client.recv(1024)

    print(f"Received: {response}")

    client.close()


if __name__ == "__main__":
    send_command()