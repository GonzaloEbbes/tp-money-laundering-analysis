# src/entities/joiners/join_max_amount_per_bank/main.py
import os
import logging
import signal
import threading
from common import middleware, message_protocol
from common.message_protocol.internal import InternalMessageType
from message_handler import MessageHandler as JoinMessageHandler

ID = int(os.environ.get("ID", 0))
JOIN_AMOUNT = int(os.environ.get("JOIN_AMOUNT", 1))
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]

JOIN_EXCHANGE = os.environ.get("JOIN_EXCHANGE", "query2_join_exchange")
JOIN_ROUTING_KEY_PREFIX = os.environ.get("JOIN_ROUTING_KEY_PREFIX", "join_partition")
JOIN_AMOUNT = int(os.environ.get("JOIN_AMOUNT", 1))
ID = int(os.environ.get("ID", 0))
EOF_CONTROL_EXCHANGE = os.environ.get("EOF_CONTROL_EXCHANGE", "join_control_exchange")

class JoinMaxAmountPerBank:
    def __init__(self):
        
        self.id = ID
        self.routing_key = f"{JOIN_ROUTING_KEY_PREFIX}_{ID}"
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            JOIN_EXCHANGE,
            routing_keys=[self.routing_key],
            queue_name=None,
            exclusive=True  
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)

        self.bank_cache = {}
        self.pending_results = []
        self.accounts_eof = False
        self.mappers_eof = False

        self.total_instances = JOIN_AMOUNT
        self.eof_consumer = None
        self.eof_producer = None
        self._is_leader = (self.id == 0)
        if self.total_instances > 1:
            all_routing_keys = [f"join_{i}" for i in range(self.total_instances)]
            self.eof_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, EOF_CONTROL_EXCHANGE, all_routing_keys
            )
            self.eof_producer = middleware.MessageMiddlewareExchangePublisherRabbitMQ(
                MOM_HOST, EOF_CONTROL_EXCHANGE
            )

        self.total_eof_leader = {}
        self._eof_lock = threading.Lock()
        self._pending = set()
        self._pending_lock = threading.Lock()
        self._finalized = set()
        self._finalized_lock = threading.Lock()
        self._inflight = {}
        self._inflight_lock = threading.Lock()
        self._stop = False

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
                self.total_eof_leader[cid] = self.total_eof_leader.get(cid, 0) + 1
                if self.total_eof_leader[cid] == self.total_instances:
                    self.output_queue.send(JoinMessageHandler.serialize_eof_message(cid))
                    del self.total_eof_leader[cid]
        else:
            if self.eof_producer:
                self.eof_producer.send(JoinMessageHandler.serialize_eof_leader_message(cid), routing_key=f"join_{self.id}")
        with self._pending_lock:
            if cid in self._pending:
                self._pending.remove(cid)

    def process_message(self, raw_msg, ack, nack):
        try:
            msg = message_protocol.internal.deserialize(raw_msg)
            cid = msg.source_client_uuid

            if msg.type == InternalMessageType.BANK_FILTER_TO_JOINER:
                self._add_inflight(cid)
                if msg.data is None:
                    logging.info(f"Join {self.id} received EOF from accounts for client {cid}")
                    self.accounts_eof = True
                    self._try_flush(cid)
                else:
                    logging.debug(f"Join {self.id} received account message for client {cid}")
                    bank_id = msg.data.get("bank_id")
                    bank_name = msg.data.get("bank_name")
                    if bank_id:
                        self.bank_cache[bank_id] = bank_name
                self._dec_inflight(cid)
                self._try_finalize(cid)
                ack()
                return

            # Resultados de mappers
            if msg.type == InternalMessageType.MAX_AMOUNT_PER_BANK_RESULT:
                logging.debug(f"Join {self.id} received mapper result for client {cid}")
                self._add_inflight(cid)
                self.pending_results.append(msg)
                self._dec_inflight(cid)
                self._try_finalize(cid)
                ack()
                return

            if msg.type == InternalMessageType.EOF_GENERIC_MESSAGE:
                self._add_inflight(cid)
                logging.debug(f"Join {self.id} received EOF from mappers for client {cid}")
                self.mappers_eof = True
                self._try_flush(cid)
                self._dec_inflight(cid)
                self._try_finalize(cid)
                ack()
                return

            ack()
        except Exception as e:
            logging.exception(e)
            nack()

    def _try_flush(self, cid):
        logging.debug(f"Join {self.id} trying to flush results for client {cid} (accounts_eof={self.accounts_eof}, mappers_eof={self.mappers_eof})")
        if not self.accounts_eof or not self.mappers_eof:
            return

        # Combinar máximos por banco
        combined = {}
        for msg in self.pending_results:
            from_bank = msg.data.get("from_bank")
            amount = msg.data.get("amount_received")
            origin = msg.data.get("account_origin")
            if from_bank:
                current = combined.get(from_bank)
                if current is None or amount > current[0]:
                    combined[from_bank] = (amount, origin)

        # TODAS las instancias envían los resultados al gateway (duplicados)
        for from_bank, (amount, origin) in combined.items():
            bank_name = self.bank_cache.get(int(from_bank), "Unknown")
            if bank_name == "Unknown":
                logging.warning(f"Join {self.id} could not find bank name for bank_id {from_bank} in cache for client {cid}")
                continue
            self.output_queue.send(JoinMessageHandler.serialize_result(
                cid, None, bank_name, origin, amount
            ))
            logging.debug(f"Join {self.id} sent result for bank {bank_name} amount {amount}")

        self.pending_results.clear()

        # Coordinación del EOF final: solo el líder lo enviará, después de recibir notificaciones
        if self.total_instances > 1:
            logging.info(f"Join {self.id} sending EOF leader message for client {cid}")
            self.eof_producer.send(JoinMessageHandler.serialize_eof_leader_message(cid), routing_key="join_0")
        else:
            self.output_queue.send(JoinMessageHandler.serialize_eof_message(cid))


    def start(self):
        if self.eof_consumer:
            threading.Thread(target=self._control_loop, daemon=True).start()
        self.input_exchange.start_consuming(self.process_message)

    def _control_loop(self):
        try:
            self.eof_consumer.start_consuming(self._process_control)
        except Exception as e:
            logging.error(f"Join control consumer error: {e}")

    def _process_control(self, raw_msg, ack, nack):
        msg = message_protocol.internal.deserialize(raw_msg)
        cid = msg.source_client_uuid
        if msg.type == InternalMessageType.EOF_LEADER_MESSAGE and self._is_leader:
            with self._eof_lock:
                self.total_eof_leader[cid] = self.total_eof_leader.get(cid, 0) + 1
                logging.info(f"Join {self.id} received EOF leader message for client {cid}, count {self.total_eof_leader[cid]}/{self.total_instances}")
                if self.total_eof_leader[cid] == self.total_instances:
                    self.output_queue.send(JoinMessageHandler.serialize_eof_message(cid))
                    del self.total_eof_leader[cid]
        ack()

    def stop(self):
        if not self._stop:
            self._stop = True
            self.input_exchange.stop_consuming()
            if self.eof_consumer:
                self.eof_consumer.stop_consuming()
            self.input_exchange.close()
            self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    w = JoinMaxAmountPerBank()
    signal.signal(signal.SIGTERM, lambda *_: w.stop())
    w.start()

if __name__ == "__main__":
    main()
