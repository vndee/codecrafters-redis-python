import socket
import time
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class RedisClient:
    def __init__(self, host='127.0.0.1', port=6379):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))

    def send_ping(self):
        # Format PING command in RESP format: "*1\r\n$4\r\nPING\r\n"
        command = b"*1\r\n$4\r\nPING\r\n"
        logger.debug(f"Sending bytes: {command}")
        self.socket.send(command)

        # Read response
        response = self.socket.recv(1024)
        if not response:
            raise Exception("No content received")
        logger.debug(f"Received bytes: {response}")

        # Verify response format (+PONG\r\n)
        if not response.startswith(b"+"):
            raise Exception("Expected start of a new RESP2 value (either +, -, :, $ or *)")

        return response.decode().strip()

    def close(self):
        self.socket.close()


def run_ping(logger, client, client_num):
    try:
        logger.debug(f"client-{client_num}: $ redis-cli PING")
        response = client.send_ping()
        logger.debug(f"Received: {response}")
        if response != "+PONG":
            raise Exception(f"Unexpected response: {response}")
    except Exception as e:
        logger.error(f"Error for client-{client_num}: {str(e)}")
        raise


def test_ping_pong_concurrent():
    try:
        # Create and test first client
        logger.debug("Testing client 1...")
        client1 = RedisClient()
        run_ping(logger, client1, 1)

        # Create and test second client
        logger.debug("Testing client 2...")
        client2 = RedisClient()
        run_ping(logger, client2, 2)

        # Additional pings for client1
        run_ping(logger, client1, 1)
        run_ping(logger, client1, 1)

        # Additional ping for client2
        run_ping(logger, client2, 2)

        # Close client1 and create client3
        logger.debug("client-1: Success, closing connection...")
        client1.close()

        # Test with client3
        logger.debug("Testing client 3...")
        client3 = RedisClient()
        run_ping(logger, client3, 3)

        # Close remaining connections
        logger.debug("client-2: Success, closing connection...")
        client2.close()
        logger.debug("client-3: Success, closing connection...")
        client3.close()

        logger.debug("All tests passed successfully!")
        return True

    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        return False


if __name__ == "__main__":
    test_ping_pong_concurrent()