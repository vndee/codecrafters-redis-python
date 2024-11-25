import socket


def send_command(client, cmd: bytes):
    print(f"Sending: {cmd}")
    client.send(cmd)

    response = client.recv(1024)
    print(f"Received: {response}")


if __name__ == "__main__":
    commands = [
        # b"+PING\r\n",
        # b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n",
        # b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n",
        # b"*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\npx\r\n$3\r\n100\r\n",
        b"*3\r\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
        # b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
        # b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\ndir\r\n",
        # b"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n",
        # b'+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n'
        b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$5\r\n10000\r\n",
    ]

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(('127.0.0.1', 6379))

    for cmd in commands:
        send_command(client, cmd)
