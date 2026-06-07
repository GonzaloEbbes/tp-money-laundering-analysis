import logging
import os
import socket
import csv
import json
import signal
import queue
import threading
import uuid
from pathlib import Path
from common import message_protocol



SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
DATA_PATH_TRANSACTIONS = os.environ.get("DATA_PATH", "/data/dataset.csv")
DATA_PATH_ACCOUNTS = os.environ.get("DATA_PATH_ACCOUNTS", "/data/accounts.csv")
EXPECTED_RESULT_EOFS = int(os.environ.get("EXPECTED_RESULT_EOFS", "1"))
RESULTS_DIR = os.environ.get("RESULTS_DIR", "/output/results")
SEND_BATCH_SIZE = int(os.environ.get("SEND_BATCH_SIZE", "40000"))
SEND_PROGRESS_INTERVAL = int(os.environ.get("SEND_PROGRESS_INTERVAL", "500000"))
RESULTS_WAIT_LOG_INTERVAL = int(os.environ.get("RESULTS_WAIT_LOG_INTERVAL", "60"))
RESULTS_IDLE_TIMEOUT = int(os.environ.get("RESULTS_IDLE_TIMEOUT", "0"))

RESULT_TYPE_NAMES = {
    message_protocol.external.MsgType.QUERY_1_RESULT: "QUERY_1_RESULT",
    message_protocol.external.MsgType.QUERY_2_RESULT: "QUERY_2_RESULT",
    message_protocol.external.MsgType.QUERY_3_RESULT: "QUERY_3_RESULT",
    message_protocol.external.MsgType.QUERY_4_RESULT: "QUERY_4_RESULT",
    message_protocol.external.MsgType.QUERY_5_RESULT: "QUERY_5_RESULT",
}

RESULT_FILES = {
    message_protocol.external.MsgType.QUERY_1_RESULT: (
        "query_1.csv",
        ["account_origin", "account_destination", "amount_received"],
    ),
    message_protocol.external.MsgType.QUERY_2_RESULT: (
        "query_2.csv",
        ["bank_name", "account_origin", "amount_received"],
    ),
    message_protocol.external.MsgType.QUERY_3_RESULT: (
        "query_3.csv",
        ["account_origin", "amount_received"],
    ),
    message_protocol.external.MsgType.QUERY_4_RESULT: (
        "query_4.csv",
        ["account"],
    ),
    message_protocol.external.MsgType.QUERY_5_RESULT: (
        "query_5.csv",
        ["cantTrx"],
    ),
}


class ResultWriter:
    def __init__(self, base_dir):
        self.run_id = str(uuid.uuid4())
        self.output_dir = Path(base_dir) / f"client-{self.run_id}"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.files = {}
        self.writers = {}

        for msg_type, (file_name, header) in RESULT_FILES.items():
            file = (self.output_dir / file_name).open("w", newline="", encoding="utf-8")
            writer = csv.writer(file)
            writer.writerow(header)
            file.flush()
            self.files[msg_type] = file
            self.writers[msg_type] = writer

    def write_result(self, msg_type, data):
        rows = self._rows_for_result(msg_type, data)
        writer = self.writers[msg_type]
        for row in rows:
            writer.writerow(row)
        self.files[msg_type].flush()

    def write_summary(self, result_counts_by_type, eof_count, result_count):
        summary = {
            "run_id": self.run_id,
            "eof_count": eof_count,
            "result_count": result_count,
            "result_counts_by_type": result_counts_by_type,
            "files": {
                RESULT_TYPE_NAMES[msg_type]: file_name
                for msg_type, (file_name, _) in RESULT_FILES.items()
            },
        }
        (self.output_dir / "summary.json").write_text(
            json.dumps(summary, indent=2) + "\n",
            encoding="utf-8",
        )

    def close(self):
        for file in self.files.values():
            file.close()

    def _rows_for_result(self, msg_type, data):
        if msg_type == message_protocol.external.MsgType.QUERY_4_RESULT:
            return [[account] for account in data]
        if msg_type == message_protocol.external.MsgType.QUERY_5_RESULT:
            return [[data]]
        return [list(data)]


class Client:
    def __init__(self):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.ack_queue = queue.Queue()
        self.eof_queue = queue.Queue()
        self._stop_receiver = False
        self._expecting_server_close = False
        self._receiver_thread = None
        self.result_writer = ResultWriter(RESULTS_DIR)
        self._results_lock = threading.Lock()
        self.result_count = 0
        self.eof_count = 0
        self.result_counts_by_type = {}
        self.q5_results = []
        logging.info("Client results directory: %s", self.result_writer.output_dir)

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
                    self._record_result(msg_type, data)
                elif msg_type == message_protocol.external.MsgType.EOF:
                    with self._results_lock:
                        self.eof_count += 1
                        eof_count = self.eof_count
                        result_count = self.result_count
                    logging.info(
                        "Received EOF from gateway (%s/%s). Total results: %s",
                        eof_count,
                        EXPECTED_RESULT_EOFS,
                        result_count,
                    )
                    self.eof_queue.put(eof_count)
                else:
                    logging.warning(f"Unexpected message type: {msg_type}")
            except socket.error as e:
                if not (self.closed or self._stop_receiver or self._expecting_server_close):
                    logging.error("Connection lost in receiver thread: %s", e)
                break
            except Exception as e:
                if self.closed or self._stop_receiver or self._expecting_server_close:
                    logging.debug("Receiver stopped after expected socket close: %s", e)
                else:
                    logging.error("Receiver error: %s", e)
                break

    def _record_result(self, msg_type, data):
        with self._results_lock:
            self.result_count += 1
            result_name = RESULT_TYPE_NAMES.get(msg_type, msg_type)
            self.result_counts_by_type[result_name] = self.result_counts_by_type.get(result_name, 0) + 1
            self.result_writer.write_result(msg_type, data)
            if msg_type == message_protocol.external.MsgType.QUERY_5_RESULT:
                self.q5_results.append(data)
                logging.info("Received QUERY_5_RESULT from gateway with data %s", data)

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

        if getattr(self, "server_socket", None):
            try:
                self.server_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

        if self._receiver_thread:
            self._receiver_thread.join(timeout=2)

        if getattr(self, "server_socket", None):
            try:
                self.server_socket.close()
            except OSError:
                pass

    def _send_and_wait_ack(self, msg_type, *args):
        message_protocol.external.send_msg(self.server_socket, msg_type, *args)
        self._wait_for_acks(1)

    def _send_without_wait_ack(self, msg_type, *args):
        message_protocol.external.send_msg(self.server_socket, msg_type, *args)

    def _wait_for_acks(self, amount):
        for _ in range(amount):
            try:
                self.ack_queue.get(timeout=30)
            except queue.Empty:
                raise socket.error("ACK timeout")

    def _flush_pending_acks(self, pending_acks):
        if pending_acks:
            self._wait_for_acks(pending_acks)
        return 0

    def _log_send_progress(self, label, records_sent):
        if records_sent and records_sent % SEND_PROGRESS_INTERVAL == 0:
            logging.info("Sent %s %s records", records_sent, label)

    def _send_record_batched(self, pending_acks, msg_type, *args):
        self._send_without_wait_ack(msg_type, *args)
        pending_acks += 1
        if pending_acks >= SEND_BATCH_SIZE:
            pending_acks = self._flush_pending_acks(pending_acks)
        return pending_acks

    def send_account_records(self, input_file):
        logging.debug("Sending account records")
        pending_acks = 0
        records_sent = 0
        with open(input_file, mode="r", newline="\n", encoding="utf-8-sig") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(csv_reader, None)
            for row in csv_reader:

                [bank_name, bank_id, account_number, entity_id, entity_name] = row
                pending_acks = self._send_record_batched(
                    pending_acks,
                    message_protocol.external.MsgType.ACCOUNT_RECORD,
                    bank_name,
                    int(bank_id),
                    account_number,
                    entity_id,
                    entity_name
                )
                records_sent += 1
                self._log_send_progress("account", records_sent)
        pending_acks = self._flush_pending_acks(pending_acks)
        logging.info("Finished sending %s account records, sending EOF", records_sent)
        self._send_and_wait_ack(
            message_protocol.external.MsgType.EOF
        )

    def send_close(self):
        logging.info("Sending close message")
        self._expecting_server_close = True
        self._send_and_wait_ack(message_protocol.external.MsgType.CLOSE)
        self._stop_receiver = True

    def send_transaction_records(self, input_file):
        logging.info("Sending transaction records")
        pending_acks = 0
        records_sent = 0
        with open(input_file, newline="\n", encoding="utf-8-sig") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(csv_reader, None)
            for row in csv_reader:
                [timestamp, from_bank, account_origin,
                 to_bank, account_destiny, amount_received,
                 receiving_currency, amount_paid, payment_currency,
                 payment_format, _] = row
                pending_acks = self._send_record_batched(
                    pending_acks,
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
                records_sent += 1
                self._log_send_progress("transaction", records_sent)
        pending_acks = self._flush_pending_acks(pending_acks)
        logging.info("Finished sending %s transaction records, sending EOF", records_sent)
        self._send_and_wait_ack(
            message_protocol.external.MsgType.EOF
        )


    def receive_all_results(self):
        logging.info("Waiting for results from gateway")
        waited_seconds = 0
        while True:
            try:
                eof_count = self.eof_queue.get(timeout=RESULTS_WAIT_LOG_INTERVAL)
                if eof_count >= EXPECTED_RESULT_EOFS:
                    break
            except queue.Empty:
                waited_seconds += RESULTS_WAIT_LOG_INTERVAL
                with self._results_lock:
                    result_count = self.result_count
                    eof_count = self.eof_count
                    result_counts_by_type = dict(self.result_counts_by_type)
                logging.info(
                    "Still waiting for results. EOFs: %s/%s. Total results: %s. Result counts by type: %s",
                    eof_count,
                    EXPECTED_RESULT_EOFS,
                    result_count,
                    result_counts_by_type,
                )
                if RESULTS_IDLE_TIMEOUT > 0 and waited_seconds >= RESULTS_IDLE_TIMEOUT:
                    raise socket.error("Timeout waiting for results")

        with self._results_lock:
            result_counts_by_type = dict(self.result_counts_by_type)
            q5_results = list(self.q5_results)
            eof_count = self.eof_count
            result_count = self.result_count
            self.result_writer.write_summary(result_counts_by_type, eof_count, result_count)

        logging.info("Result counts by type: %s", result_counts_by_type)
        logging.info("Q5 results: %s", q5_results)
        logging.info("Results written to %s", self.result_writer.output_dir)

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
        client.result_writer.close()

    return 0

if __name__ == "__main__":
    main()
