import logging
import os
import socket
import threading

from common import message_protocol
from common.middleware import MessageMiddlewareQueueRabbitMQ

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]


class Gateway:
    def __init__(self):
        self.output_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        self.input_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.pending_clients = {}
        self.pending_lock = threading.Lock()

    def start(self):
        result_thread = threading.Thread(target=self._consume_results, daemon=True)
        result_thread.start()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((SERVER_HOST, SERVER_PORT))
            server_socket.listen()
            logging.info("Gateway listening on %s:%s", SERVER_HOST, SERVER_PORT)

            while True:
                client_socket, _ = server_socket.accept()
                threading.Thread(
                    target=self._handle_client,
                    args=(client_socket,),
                    daemon=True,
                ).start()

    def _handle_client(self, client_socket):
        with client_socket:
            payload = client_socket.recv(4096).decode("utf-8").strip()
            if not payload:
                return

            message = message_protocol.new_message(payload)
            correlation_id = message["message_id"]
            done = threading.Event()

            with self.pending_lock:
                self.pending_clients[correlation_id] = (client_socket, done)

            logging.info("Gateway received client message %s", correlation_id)
            self.output_queue.send(message_protocol.serialize(message))

            if not done.wait(timeout=30):
                with self.pending_lock:
                    self.pending_clients.pop(correlation_id, None)
                logging.warning("Timed out waiting for result %s", correlation_id)

    def _consume_results(self):
        def _on_result(raw_message, ack, nack):
            try:
                message = message_protocol.deserialize(raw_message)
                correlation_id = message.get("message_id")

                with self.pending_lock:
                    pending_client = self.pending_clients.pop(correlation_id, None)

                if pending_client:
                    client_socket, done = pending_client
                    response = message_protocol.serialize(message)
                    client_socket.sendall(response + b"\n")
                    client_socket.shutdown(socket.SHUT_RDWR)
                    done.set()
                else:
                    logging.warning("No client waiting for %s", correlation_id)
                ack()
            except Exception:
                logging.exception("Gateway failed while returning result")
                nack()

        self.input_queue.start_consuming(_on_result)


def main():
    logging.basicConfig(level=logging.INFO)
    Gateway().start()


if __name__ == "__main__":
    main()
