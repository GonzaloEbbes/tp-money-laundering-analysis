# src/entities/mappers/map_max_amount_per_bank/main.py
import os
import logging
import signal
import threading
import zlib
from common import middleware, message_protocol
from common.dedup import InMemoryDeduplicator, message_dedup_key
from common.message_protocol.internal import InternalMessageType
from message_handler import MessageHandler as MapperMessageHandler

ID = int(os.environ["ID"])
MAP_AMOUNT = int(os.environ.get("MAP_AMOUNT", 1))
MOM_HOST = os.environ["MOM_HOST"]
MAP_MAX_EXCHANGE = os.environ.get("MAP_MAX_EXCHANGE", "map_max_exchange")
MAP_MAX_ROUTING_KEY_PREFIX = os.environ.get("MAP_MAX_ROUTING_KEY_PREFIX", "map_max_partition")
JOIN_EXCHANGE = os.environ.get("JOIN_EXCHANGE", "query2_join_exchange")
JOIN_AMOUNT = int(os.environ.get("JOIN_AMOUNT", 1))
JOIN_ROUTING_KEY_PREFIX = os.environ.get("JOIN_ROUTING_KEY_PREFIX", "join_partition")

def stable_hash(value):
    try:
        norm_val = int(value)
    except ValueError:
        norm_val = str(value).strip()
    return zlib.crc32(str(norm_val).encode())

class MapMaxAmountPerBank:
    def __init__(self):
        self.id = ID
        self.deduplicator = InMemoryDeduplicator()
        self.total = MAP_AMOUNT
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            MAP_MAX_EXCHANGE,
            [f"{MAP_MAX_ROUTING_KEY_PREFIX}_{self.id}"],
            queue_name=None,
            exclusive=True
        )
        self.join_exchange = middleware.MessageMiddlewareExchangePublisherRabbitMQ(
            MOM_HOST, JOIN_EXCHANGE
        )
        
        #{cid: {from_bank: (amount, origin)}}
        self.bank_max = {} 

        self._pending = set()
        self._pending_lock = threading.Lock()
        self._finalized = set()
        self._finalized_lock = threading.Lock()
        self._inflight = {}
        self._inflight_lock = threading.Lock()
        self._stop = False
        self._stop_lock = threading.Lock()
        
        self._join_exchange_lock = threading.Lock()

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
        with self._pending_lock:
            if cid in self._pending:
                self._pending.remove(cid)
                
        if cid in self.bank_max:
            del self.bank_max[cid]

    def process_message(self, raw_msg, ack, nack):
        try:
            msg = message_protocol.internal.deserialize(raw_msg)
            cid = msg.source_client_uuid

            if cid not in self.bank_max:
                self.bank_max[cid] = {}

            if msg.type == InternalMessageType.EOF_GENERIC_MESSAGE:
                logging.debug(f"Mapper {self.id} received EOF for client {cid}")
                self._add_inflight(cid)
                
                for from_bank, (amount, origin) in self.bank_max[cid].items():
                    partition = stable_hash(from_bank) % JOIN_AMOUNT
                    routing_key = f"{JOIN_ROUTING_KEY_PREFIX}_{partition}"
                    result_id = f"{self.id}:{from_bank}"
                    result_bytes = MapperMessageHandler.serialize_result(
                        cid, result_id, from_bank, amount, origin, message_id=result_id
                    )
                    with self._join_exchange_lock:
                        self.join_exchange.send(result_bytes, routing_key=routing_key)
                logging.info(f"Mapper {self.id} sent {len(self.bank_max[cid])} results for client {cid}")
                
                eof_bytes = MapperMessageHandler.serialize_eof_message(cid)
                for i in range(JOIN_AMOUNT):
                    routing_key = f"{JOIN_ROUTING_KEY_PREFIX}_{i}"
                    with self._join_exchange_lock:
                        self.join_exchange.send(eof_bytes, routing_key=routing_key)

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

            if msg.type != InternalMessageType.DATA_PER_BANK_SHUFFLER_TO_MAP_MAX_AMOUNT_PER_BANK:
                ack()
                return

            dedup_key = message_dedup_key(msg)
            if not self.deduplicator.should_process(cid, dedup_key):
                ack()
                return

            self._add_inflight(cid)
            from_bank = msg.data.get("from_bank")
            amount = msg.data.get("amount_received")
            if amount is None:
                self._dec_inflight(cid)
                self._try_finalize(cid)
                self.deduplicator.mark_processed(cid, dedup_key)
                ack()
                return
            origin = msg.data.get("account_origin")
            if from_bank is not None:
                current = self.bank_max[cid].get(int(from_bank))
                if current is None or amount > current[0]:
                    self.bank_max[cid][int(from_bank)] = (amount, origin)
                    
            self._dec_inflight(cid)
            self._try_finalize(cid)
            self.deduplicator.mark_processed(cid, dedup_key)
            ack()
            
        except Exception as e:
            logging.exception(e)
            nack()

    def start(self):
        self.input_exchange.start_consuming(self.process_message)

    def stop(self):
        with self._stop_lock:
            if self._stop:
                return
            self._stop = True
        self.input_exchange.stop_consuming()
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
