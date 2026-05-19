import logging
import os
import socket
import time

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
MESSAGE = os.environ.get("MESSAGE", "mensaje de prueba")


def main():
    logging.basicConfig(level=logging.INFO)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        last_error = None
        for _ in range(30):
            try:
                client_socket.connect((SERVER_HOST, SERVER_PORT))
                break
            except OSError as error:
                last_error = error
                time.sleep(1)
        else:
            raise last_error

        client_socket.sendall(MESSAGE.encode("utf-8"))
        response = client_socket.recv(4096)
        print(response.decode("utf-8").strip(), flush=True)


if __name__ == "__main__":
    main()
