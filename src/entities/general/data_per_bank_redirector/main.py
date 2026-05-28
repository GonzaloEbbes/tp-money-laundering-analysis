import os
import logging
import signal
import threading
from common import middleware, message_protocol
from common.message_protocol.internal import InternalMessageType
from message_handler import MessageHandler as DataPerBankRedirectorMessageHandler

ID = int(os.environ.get("ID", 0))
TOTAL = int(os.environ.get("DATA_PER_BANK_REDIRECTOR_AMOUNT", 1))
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]   # data_per_bank_shuffler_queue
EOF_CONTROL_EXCHANGE = os.environ.get("EOF_CONTROL_EXCHANGE", "data_per_bank_control_exchange")
EXCHANGE_NAME = os.environ.get("EXCHANGE_NAME", "map_max_exchange")
OUTPUT_ROUTING_KEY_PREFIX = os.environ.get("OUTPUT_ROUTING_KEY_PREFIX", "map_max_partition")
TOTAL_MAPPERS = int(os.environ.get("TOTAL_MAPPERS", 1))

class DataPerBankRedirector:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.map_exchange = middleware.MessageMiddlewareExchangePublisherRabbitMQ(MOM_HOST, EXCHANGE_NAME)

        self.id = ID
        self.total = TOTAL
        self.eof_consumer = None
        self.eof_producer = None
        self._is_leader = (self.id == 0)
        if self.total > 1:
            
            all_routing_keys = [f"dpb_redirector_{i}" for i in range(self.total)]
            self.eof_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, EOF_CONTROL_EXCHANGE, all_routing_keys
            )

            self.eof_producer = middleware.MessageMiddlewareExchangePublisherRabbitMQ(
                MOM_HOST, EOF_CONTROL_EXCHANGE
            )
        self.total_eof = {}
        self._eof_lock = threading.Lock()
        self._pending = set()
        self._pending_lock = threading.Lock()
        self._finalized = set()
        self._finalized_lock = threading.Lock()
        self._inflight = {}
        self._inflight_lock = threading.Lock()
        self._stop = False
        self._stop_lock = threading.Lock()
        self._sigterm = False

    def _add_inflight(self, cid):
        with self._inflight_lock:
            self._inflight[cid] = self._inflight.get(cid, 0) + 1

    def _dec_inflight(self, cid):
        with self._inflight_lock:
            if cid in self._inflight:
                self._inflight[cid] -= 1

    def _try_finalize(self, cid):
        pending = False
        with self._pending_lock:
            pending = cid in self._pending
        if pending:
            with self._inflight_lock:
                if self._inflight.get(cid, 0) == 0:
                    self._finalize_client(cid)

    def _finalize_client(self, cid):
        with self._finalized_lock:
            if cid in self._finalized:
                return
            self._finalized.add(cid)

        if self._is_leader:
            with self._eof_lock:
                self.total_eof[cid] = self.total_eof.get(cid, 0) + 1
                logging.info(f"Redirector leader {self.id} received EOF leader message for client {cid} ({self.total_eof[cid]}/{self.total})")
                if self.total_eof[cid] == self.total:
                    eof_msg = DataPerBankRedirectorMessageHandler.serialize_eof_message(cid)
                    for i in range(TOTAL_MAPPERS):
                        routing_key = f"{OUTPUT_ROUTING_KEY_PREFIX}_{i}"
                        self.map_exchange.send(eof_msg, routing_key=routing_key)
                        logging.info(f"Redirector leader {self.id} sent EOF to mapper partition {i}")
                    del self.total_eof[cid]
        else:
            if self.eof_producer:
                self.eof_producer.send(DataPerBankRedirectorMessageHandler.serialize_eof_leader_message(cid), routing_key=f"dpb_redirector_{self.id}")
                logging.info(f"Redirector {self.id} sent EOF leader message for client {cid}")

        with self._pending_lock:
            if cid in self._pending:
                self._pending.remove(cid)

    def _process_eof_from_control_exchange(self, cid):
        with self._inflight_lock:
            if self._inflight.get(cid, 0) > 0:
                with self._pending_lock:
                    self._pending.add(cid)
                logging.debug(f"Redirector {self.id} marked client {cid} as pending (inflight >0)")
            else:
                self._finalize_client(cid)
                logging.debug(f"Redirector {self.id} finalized client {cid} immediately (inflight=0)")

    def process_message(self, raw_msg, ack, nack):
        try:
            msg = message_protocol.internal.deserialize(raw_msg)
            cid = msg.source_client_uuid
            if msg.type == InternalMessageType.EOF_GENERIC_MESSAGE:
                logging.info(f"Redirector {self.id} received EOF for client {cid}")
                self._add_inflight(cid)
                if self.eof_producer:
                    self.eof_producer.send(DataPerBankRedirectorMessageHandler.serialize_eof_message(cid), routing_key=f"dpb_redirector_{self.id}")
                self._dec_inflight(cid)
                with self._inflight_lock:
                    inflight = self._inflight.get(cid, 0)
                if inflight == 0:
                    self._finalize_client(cid)
                else:
                    with self._pending_lock:
                        self._pending.add(cid)
                ack()
                return

            if msg.type != InternalMessageType.USD_FILTER_Q1Q2_TO_DATA_PER_BANK_SHUFFLER:
                ack()
                return

            self._add_inflight(cid)
            from_bank = msg.data.get("from_bank")
            if from_bank:
                partition = hash(str(from_bank)) % TOTAL_MAPPERS
                logging.debug(f"Redirector {self.id} calculated partition {partition} for bank {from_bank}")
                routing_key = f"{OUTPUT_ROUTING_KEY_PREFIX}_{partition}"
                account_origin = msg.data.get("account_origin")
                amount_received = msg.data.get("amount_received")
                logging.debug(f"Redirector {self.id} redirecting message for client {cid} to mapper partition {partition}")
                msg = DataPerBankRedirectorMessageHandler.serialize_redirect(cid, msg.data_id, from_bank, account_origin, amount_received)
                self.map_exchange.send(msg, routing_key=routing_key)
            self._dec_inflight(cid)
            self._try_finalize(cid)
            ack()
        except Exception as e:
            logging.exception(e)
            nack()

    def control_loop(self):
        try:
            self.eof_consumer.start_consuming(self.process_control)
        except Exception as e:
            logging.error(f"Control consumer error: {e}")

    def process_control(self, raw_msg, ack, nack):
        msg = message_protocol.internal.deserialize(raw_msg)
        cid = msg.source_client_uuid
        if msg.type == InternalMessageType.EOF_GENERIC_MESSAGE:
            self._process_eof_from_control_exchange(cid)
        elif msg.type == InternalMessageType.EOF_LEADER_MESSAGE and self._is_leader:
            with self._eof_lock:
                self.total_eof[cid] = self.total_eof.get(cid, 0) + 1
                if self.total_eof[cid] == self.total:
                    eof_msg = DataPerBankRedirectorMessageHandler.serialize_eof_message(cid)
                    for i in range(TOTAL_MAPPERS):
                        routing_key = f"{OUTPUT_ROUTING_KEY_PREFIX}_{i}"
                        self.map_exchange.send(eof_msg, routing_key=routing_key)
                        logging.info(f"Redirector leader {self.id} sent final EOF to mapper partition {i}")
                    del self.total_eof[cid]
        ack()

    def start(self):
        if self.eof_consumer:
            threading.Thread(target=self.control_loop, daemon=True).start()
        self.input_queue.start_consuming(self.process_message)

    def stop(self):
        with self._stop_lock:
            if self._stop:
                return
            self._stop = True
        self.input_queue.stop_consuming()
        if self.eof_consumer:
            self.eof_consumer.stop_consuming()
        self.input_queue.close()
        self.map_exchange.close()

def main():
    logging.basicConfig(level=logging.INFO)
    w = DataPerBankRedirector()
    def _sigterm(*_):
        logging.info("SIGTERM received")
        w.stop()
    signal.signal(signal.SIGTERM, _sigterm)
    w.start()

if __name__ == "__main__":
    main()