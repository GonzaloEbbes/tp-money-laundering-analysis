# src/entities/filters/bank_filter/main.py
import os
import logging
import signal
import threading
import zlib
from common import middleware, message_protocol
from common.message_protocol.internal import InternalMessageType
from message_handler import MessageHandler as BankFilterMessageHandler

ID = int(os.environ["ID"])
TOTAL = int(os.environ.get("BANK_FILTERS_AMOUNT", 1))
MOM_HOST = os.environ["MOM_HOST"]
EXCHANGE_NAME = os.environ.get("BANK_EXCHANGE", "bank_exchange")
ROUTING_KEY_PREFIX = os.environ.get("BANK_ROUTING_KEY_PREFIX", "bank_partition")
# Nuevo: exchange de salida hacia los joins
JOIN_EXCHANGE = os.environ.get("JOIN_EXCHANGE", "query2_join_exchange")
JOIN_AMOUNT = int(os.environ.get("JOIN_AMOUNT", 1))
JOIN_ROUTING_KEY_PREFIX = os.environ.get("JOIN_ROUTING_KEY_PREFIX", "join_partition")
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]

def stable_hash(value):
    return zlib.crc32(str(value).encode())

class BankFilter:
    def __init__(self):
        self.routing_key = f"{ROUTING_KEY_PREFIX}_{ID}"
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            EXCHANGE_NAME,
            routing_keys=[self.routing_key],
            queue_name=None,     
            exclusive=True
        )
        # Publicador al exchange de join (particionado)
        self.join_exchange = middleware.MessageMiddlewareExchangePublisherRabbitMQ(
            MOM_HOST, JOIN_EXCHANGE
        )
        self.seen_banks = set()
        self.id = ID
        self.total = TOTAL
        self.eof_consumer = None
        self.eof_producer = None
        self._is_leader = (self.id == 0)
        if self.total > 1:
            self.eof_producer = middleware.MessageMiddlewareExchangePublisherRabbitMQ(
                MOM_HOST, EOF_CONTROL_EXCHANGE
            )
        
            if self._is_leader:
                all_routing_keys = [f"bank_filter_{i}" for i in range(self.total)]
                self.eof_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, EOF_CONTROL_EXCHANGE, all_routing_keys
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
        logging.info(f"BankFilter {self.id} finalizing client {cid}")
        with self._finalized_lock:
            if cid in self._finalized:
                return
            self._finalized.add(cid)
        if self._is_leader:
            with self._eof_lock:
                self.total_eof[cid] = self.total_eof.get(cid, 0) + 1
                if self.total_eof[cid] == self.total:
                    # Enviar EOF de cuentas a todas las particiones del join
                    logging.info(f"BankFilter {self.id} finalizing client {cid}")
                    logging.info(f"BankFilter {self.id} client {cid} seen banks: {len(self.seen_banks)}")
                    self.seen_banks.clear()
                    eof_bytes = BankFilterMessageHandler.serialize_eof_join_message(cid)  # data=None
                    for i in range(JOIN_AMOUNT):
                        routing_key = f"{JOIN_ROUTING_KEY_PREFIX}_{i}"
                        self.join_exchange.send(eof_bytes, routing_key=routing_key)
                    del self.total_eof[cid]
        else:
            if self.eof_producer:
                self.eof_producer.send(BankFilterMessageHandler.serialize_eof_leader_message(cid), routing_key=f"bank_filter_{self.id}")
        with self._pending_lock:
            if cid in self._pending:
                self._pending.remove(cid)
    
    def process_message(self, raw_msg, ack, nack):
        try:
            msg = message_protocol.internal.deserialize(raw_msg)
            cid = msg.source_client_uuid
            if msg.type == InternalMessageType.EOF_GENERIC_MESSAGE:
                logging.info(f"BankFilter {self.id} received EOF for client {cid}")
                self._add_inflight(cid)
                if self.eof_producer:
                    self.eof_producer.send(BankFilterMessageHandler.serialize_eof_message(cid), routing_key=f"bank_filter_{self.id}")
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
            if msg.type != InternalMessageType.GATEWAY_TO_BANK_FILTER:
                ack()
                return
            self._add_inflight(cid)
            bank_id = msg.data.get("bank_id")
            bank_name = msg.data.get("bank_name")
            if bank_id and bank_id not in self.seen_banks:
                self.seen_banks.add(bank_id)
                partition = stable_hash(bank_id) % JOIN_AMOUNT
                routing_key = f"{JOIN_ROUTING_KEY_PREFIX}_{partition}"
                serialized = BankFilterMessageHandler.serialize_join_message(cid, msg.data_id, bank_id, bank_name)
                self.join_exchange.send(serialized, routing_key=routing_key)
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
            self._add_inflight(cid)
            self._dec_inflight(cid)
            self._try_finalize(cid)
        elif msg.type == InternalMessageType.EOF_LEADER_MESSAGE and self._is_leader:
            with self._eof_lock:
                self.total_eof[cid] = self.total_eof.get(cid, 0) + 1
                logging.info(f"BankFilter leader {self.id} received EOF leader for client {cid}, count={self.total_eof[cid]}/{self.total}")
                if self.total_eof[cid] == self.total:
                    eof_bytes = BankFilterMessageHandler.serialize_eof_join_message(cid)
                    for i in range(JOIN_AMOUNT):
                        routing_key = f"{JOIN_ROUTING_KEY_PREFIX}_{i}"
                        self.join_exchange.send(eof_bytes, routing_key=routing_key)
                    del self.total_eof[cid]
        ack()

    def start(self):
        if self.eof_consumer:
            threading.Thread(target=self.control_loop, daemon=True).start()
        self.input_exchange.start_consuming(self.process_message)

    def stop(self):
        with self._stop_lock:
            if self._stop:
                return
            self._stop = True
        self.input_exchange.stop_consuming()
        if self.eof_consumer:
            self.eof_consumer.stop_consuming()
        self.input_exchange.close()
        self.join_exchange.close()

def main():
    logging.basicConfig(level=logging.INFO)
    w = BankFilter()
    def _sigterm(*_):
        logging.info("SIGTERM")
        w.stop()
    signal.signal(signal.SIGTERM, _sigterm)
    w.start()

if __name__ == "__main__":
    main()