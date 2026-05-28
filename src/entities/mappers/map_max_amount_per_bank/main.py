# src/entities/mappers/map_max_amount_per_bank/main.py
import os
import logging
import signal
import threading
import zlib
from common import middleware, message_protocol
from common.message_protocol.internal import InternalMessageType
from message_handler import MessageHandler as MapperMessageHandler

ID = int(os.environ["ID"])
TOTAL = int(os.environ.get("MAP_AMOUNT", 1))
MOM_HOST = os.environ["MOM_HOST"]
JOIN_EXCHANGE = os.environ.get("JOIN_EXCHANGE", "query2_join_exchange")
JOIN_AMOUNT = int(os.environ.get("JOIN_AMOUNT", 1))
JOIN_ROUTING_KEY_PREFIX = os.environ.get("JOIN_ROUTING_KEY_PREFIX", "join_partition")
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]

def stable_hash(value):
    return zlib.crc32(str(value).encode())

class MapMaxAmountPerBank:
    def __init__(self):
        self.id = ID
        self.total = TOTAL
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            "map_max_exchange",
            [f"map_max_partition_{self.id}"],
            queue_name=None,
            exclusive=True
        )
        self.join_exchange = middleware.MessageMiddlewareExchangePublisherRabbitMQ(
            MOM_HOST, JOIN_EXCHANGE
        )
        self.bank_max = {}

        self.eof_consumer = None
        self.eof_producer = None
        
        self._is_leader = (self.id == 0)
        if self.total > 1:
            self.eof_producer = middleware.MessageMiddlewareExchangePublisherRabbitMQ(
                MOM_HOST, EOF_CONTROL_EXCHANGE
            )
            all_routing_keys = [f"map_max_{i}" for i in range(self.total)]
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
        with self._finalized_lock:
            if cid in self._finalized:
                return
            self._finalized.add(cid)

        if self._is_leader:
            with self._eof_lock:
                self.total_eof[cid] = self.total_eof.get(cid, 0) + 1
                if self.total_eof[cid] == self.total:
                    # Enviar EOF a todas las particiones del join (solo una vez, al final)
                    eof_bytes = MapperMessageHandler.serialize_eof_message(cid)
                    logging.info(f"Mapper leader {self.id} sending final EOF for client {cid} to joiner")
                    for i in range(JOIN_AMOUNT):
                        routing_key = f"{JOIN_ROUTING_KEY_PREFIX}_{i}"
                        self.join_exchange.send(eof_bytes, routing_key=routing_key)
                    del self.total_eof[cid]
        else:
            if self.eof_producer:
                self.eof_producer.send(MapperMessageHandler.serialize_eof_leader_message(cid), routing_key=f"map_max_{self.id}")
        with self._pending_lock:
            if cid in self._pending:
                self._pending.remove(cid)

    def process_message(self, raw_msg, ack, nack):
        try:
            msg = message_protocol.internal.deserialize(raw_msg)
            cid = msg.source_client_uuid
            logging.info(f"Mapper {self.id} received message of type {msg.type} for client {cid}")

            if msg.type == InternalMessageType.EOF_GENERIC_MESSAGE:
                logging.info(f"Mapper {self.id} received EOF for client {cid}")
                self._add_inflight(cid)
                # Enviar resultados acumulados al join (solo una vez, aquí)
                for from_bank, (amount, origin) in self.bank_max.items():
                    partition = stable_hash(from_bank) % JOIN_AMOUNT
                    routing_key = f"{JOIN_ROUTING_KEY_PREFIX}_{partition}"
                    result_bytes = MapperMessageHandler.serialize_result(
                        cid, None, from_bank, amount, origin
                    )
                    self.join_exchange.send(result_bytes, routing_key=routing_key)
                    logging.info(f"Mapper {self.id} sent result for {from_bank}: {amount}")
                # Notificar a los demás (si no es líder) o auto-contabilizar (si es líder)
                if self.eof_producer:
                    self.eof_producer.send(MapperMessageHandler.serialize_eof_message(cid), routing_key=f"map_max_{self.id}")
                self._dec_inflight(cid)
                # Verificar si ya no hay inflight y finalizar
                with self._inflight_lock:
                    inflight = self._inflight.get(cid, 0)
                if inflight == 0:
                    self._finalize_client(cid)
                else:
                    with self._pending_lock:
                        self._pending.add(cid)
                ack()
                return

            if msg.type != InternalMessageType.DATA_PER_BANK_SHUFFLER_TO_MAP_MAX_AMOUNT_PER_BANK:
                ack()
                return

            self._add_inflight(cid)
            logging.debug(f"Mapper {self.id} received data message for client {cid}")
            from_bank = msg.data.get("from_bank")
            amount = msg.data.get("amount_received", 0.0)
            origin = msg.data.get("account_origin")
            if from_bank:
                current = self.bank_max.get(from_bank)
                if current is None or amount > current[0]:
                    self.bank_max[from_bank] = (amount, origin)
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
                logging.info(f"Mapper leader {self.id} received EOF leader for client {cid}, count={self.total_eof[cid]}/{self.total}")
                if self.total_eof[cid] == self.total:
                    eof_bytes = MapperMessageHandler.serialize_eof_message(cid)
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
    w = MapMaxAmountPerBank()
    def _sigterm(*_):
        logging.info("SIGTERM received")
        w.stop()
    signal.signal(signal.SIGTERM, _sigterm)
    w.start()

if __name__ == "__main__":
    main()