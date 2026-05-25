import logging
import os
import socket
import csv
import signal
import queue
import threading
from common import message_protocol



SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
DATA_PATH_TRANSACTIONS = os.environ.get("DATA_PATH", "/data/dataset.csv")
DATA_PATH_ACCOUNTS = os.environ.get("DATA_PATH_ACCOUNTS", "/data/accounts.csv")

class Client:
    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.ack_queue = queue.Queue()
        self.result_queue = queue.Queue()
        self._stop_receiver = False
        self._receiver_thread = None

    def _receiver_loop(self):
        while not self._stop_receiver:
            try:
                msg = message_protocol.external.recv_msg(self.server_socket)
                msg_type, data = msg
                if msg_type == message_protocol.external.MsgType.ACK:
                    self.ack_queue.put(True)
                elif msg_type in (message_protocol.external.MsgType.QUERY_1_RESULT,
                                  message_protocol.external.MsgType.QUERY_2_RESULT,
                                  message_protocol.external.MsgType.QUERY_3_RESULT,
                                  message_protocol.external.MsgType.QUERY_4_RESULT,
                                  message_protocol.external.MsgType.QUERY_5_RESULT):
                    self.result_queue.put((msg_type, data))
                elif msg_type == message_protocol.external.MsgType.EOF:
                    self.result_queue.put(('eof', None))
                else:
                    logging.warning(f"Unexpected message type: {msg_type}")
            except socket.error:
                if not self.closed:
                    logging.error("Connection lost in receiver thread")
                break
            except Exception as e:
                logging.error(f"Receiver error: {e}")
                break

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.closed = True
        self.disconnect()

        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def connect(self, server_host, server_port):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.connect((server_host, server_port))
        self._stop_receiver = False
        self._receiver_thread = threading.Thread(target=self._receiver_loop, daemon=True)
        self._receiver_thread.start()

    def disconnect(self):
        self._stop_receiver = True
        if self._receiver_thread:
            self._receiver_thread.join(timeout=2)
        if self.server_socket:
            self.server_socket.shutdown(socket.SHUT_RDWR)

    def _send_and_wait_ack(self, msg_type, *args):
        message_protocol.external.send_msg(self.server_socket, msg_type, *args)
        try:
            self.ack_queue.get(timeout=30)
        except queue.Empty:
            raise socket.error("ACK timeout")

    def send_account_records(self, input_file):
        logging.info("Sending account records")
        with open(input_file, mode="r", newline="\n", encoding="utf-8-sig") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(csv_reader, None)
            for row in csv_reader:

                [bank_name, bank_id, account_number, entity_id, entity_name] = row
                self._send_and_wait_ack(
                    message_protocol.external.MsgType.ACCOUNT_RECORD,
                    bank_name,
                    int(bank_id),
                    account_number,
                    entity_id,
                    entity_name
                )
        logging.info("Finished sending account records, sending EOF")
        self._send_and_wait_ack(
            message_protocol.external.MsgType.EOF
        )

    def send_close(self):
        logging.info("Sending close message")
        self._send_and_wait_ack(message_protocol.external.MsgType.CLOSE)

    def send_transaction_records(self, input_file):
        logging.info("Sending transaction records")
        with open(input_file, newline="\n", encoding="utf-8-sig") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(csv_reader, None)
            for row in csv_reader:
                [timestamp, from_bank, account_origin,
                 to_bank, account_destiny, amount_received,
                 receiving_currency, amount_paid, payment_currency,
                 payment_format, _] = row
                self._send_and_wait_ack(
                    message_protocol.external.MsgType.TRANSACTION_RECORD,
                    timestamp,
                    from_bank,
                    account_origin,
                    to_bank,
                    account_destiny,
                    float(amount_received),
                    receiving_currency,
                    float(amount_paid),
                    payment_currency,
                    payment_format
                )
        logging.info("Finished sending transaction records, sending EOF")
        self._send_and_wait_ack(
            message_protocol.external.MsgType.EOF
        )


    def receive_all_results(self):
        logging.info("Waiting for results from gateway")
        result_count = 0
        while True:
            try:
                tag, data = self.result_queue.get(timeout=60)
                if tag == 'eof':
                    logging.info(f"Received EOF from gateway. Total results: {result_count}")
                    continue
                elif isinstance(tag, int):
                    result_count += 1
                if self.result_queue is queue.Empty:
                    logging.info(f"Total results received: {result_count}")
                    break
            except queue.Empty:
                raise socket.error("Timeout waiting for results")

def main():
    logging.basicConfig(level=logging.INFO)
    client = Client()
    try:
        client.connect(SERVER_HOST, SERVER_PORT)
        client.send_account_records(DATA_PATH_ACCOUNTS)
        client.send_transaction_records(DATA_PATH_TRANSACTIONS)
        client.receive_all_results()
        client.send_close()
    except socket.error:
        if not client.closed:
            logging.error("The connection with the server was lost")
            return 1
    except Exception as e:
        logging.error("An error occurred: %s", e)
        logging.error(e)
        return 2
    finally:
        if not client.closed:
            client.disconnect()

    return 0

if __name__ == "__main__":
    main()
