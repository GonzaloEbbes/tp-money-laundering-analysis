import logging
import os
import socket
import threading
import json

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
        self.active_socket = None
        self.eofs_pending = 0
        self.finished_event = threading.Event()
        self.lock = threading.Lock()

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

                with self.lock:
                    self.active_socket = client_socket
                    self.eofs_pending = 2
                    self.finished_event.clear()
                
                threading.Thread(
                    target=self._handle_client,
                    args=(client_socket,),
                    daemon=True,
                ).start()

    def _handle_client(self, client_socket):
        sent_messages = set()
        buffer = ""

        try:
            while True:
                data = client_socket.recv(4096).decode("utf-8")
                if not data:
                    break
                
                buffer += data
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if not line.strip():
                        continue

                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    message = message_protocol.new_message(payload)
                    correlation_id = message["message_id"]

                    with self.lock:
                        self.pending_clients[correlation_id] = client_socket
                        sent_messages.add(correlation_id)

                    self.output_queue.send(message_protocol.serialize(message))

            logging.info("Cliente terminó de enviar datos. Esperando fin del pipeline...")
            self.finished_event.wait()
            logging.info("Todas las respuestas del cliente enviadas. Cerrando socket.")

        finally:
            with self.lock:
                if self.active_socket == client_socket:
                    self.active_socket = None
            client_socket.close()


    def _consume_results(self):
        def _on_result(raw_message, ack, nack):
            try:
                message = message_protocol.deserialize(raw_message)
                correlation_id = message.get("message_id")
                payload = message.get("payload", {})


                with self.lock:
                    client_socket = self.pending_clients.get(correlation_id, None)

                if client_socket:
                    is_eof = isinstance(payload, dict) and payload.get("type") == "EOF"
                    is_filtered = isinstance(payload, dict) and payload.get("status") == "FILTERED"

                    if is_eof:
                        try:
                            client_socket.sendall(b'{"status": "FINISHED"}\n')
                            logging.info("Sent FINISHED message for correlation_id %s", correlation_id)
                        except OSError:
                            pass

                        self.eofs_pending -= 1
                        if self.eofs_pending <= 0:
                            self.finished_event.set()

                    elif not is_filtered:
                        response = message_protocol.serialize(message)
                        try:
                            client_socket.sendall(response + b"\n")
                        except OSError:
                            pass
                else:
                    logging.warning("No hay ningún cliente esperando el mensaje %s", correlation_id)
                ack()
            except Exception:
                logging.exception("Gateway falló al retornar el resultado")
                nack()

        self.input_queue.start_consuming(_on_result)


def main():
    logging.basicConfig(level=logging.INFO)
    Gateway().start()


if __name__ == "__main__":
    main()
