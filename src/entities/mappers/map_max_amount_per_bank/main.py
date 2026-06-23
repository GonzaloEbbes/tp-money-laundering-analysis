# src/entities/mappers/map_max_amount_per_bank/main.py
import os
import logging
import signal
import threading
import zlib
from common import middleware, message_protocol
from common.message_protocol.internal import InternalMessageType
from common.snapshots.snapshot import SnapshotManager
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
        data_dir = f"/data/snapshots/map_max_{self.id}"
        self.snapshot_manager = SnapshotManager(data_dir)
        self.state = self.snapshot_manager.recover()
        
        self.BATCH_MAX_SIZE = 100
        self.FLUSH_INTERVAL_SECONDS = 2.0
        self.batch_ops = []
        self.batch_acks = []
        self.batch_lock = threading.Lock()

        self._pending = set()
        self._pending_lock = threading.Lock()
        self._finalized = set()
        self._finalized_lock = threading.Lock()
        self._inflight = {}
        self._inflight_lock = threading.Lock()
        self._stop = False
        self._stop_lock = threading.Lock()
        
        self._join_exchange_lock = threading.Lock()
        self._stop_flush_event = threading.Event()
        self._flush_thread = threading.Thread(
            target=self._periodic_flush_loop,
            daemon=True,
            name=f"flush-mapmax-{self.id}"
        )
        self._flush_thread.start()

    def _add_inflight(self, cid):
        with self._inflight_lock:
            self._inflight[cid] = self._inflight.get(cid, 0) + 1

    def _dec_inflight(self, cid):
        with self._inflight_lock:
            if cid in self._inflight:
                self._inflight[cid] -= 1

    def _try_finalize(self, cid, ack):
        pending = False
        with self._pending_lock:
            pending = cid in self._pending
        if pending:
            with self._inflight_lock:
                if self._inflight.get(cid, 0) == 0:
                    self._finalize_client(cid, ack)

    def _finalize_client(self, cid, ack):
        self._flush_batch_thread_safe()
        with self._finalized_lock:
            if cid in self._finalized:
                ack() # Si RabbitMQ lo re-entrega pero ya limpiamos, solo confirmar
                return
            self._finalized.add(cid)

        with self.batch_lock:
            self.batch_ops.append({'type': 'delete', 'key': cid})
            self.batch_acks.append(ack)
            self._flush_batch_locked()

        with self._finalized_lock:
            if cid in self._finalized:
                return
            self._finalized.add(cid)
        with self._pending_lock:
            if cid in self._pending:
                self._pending.remove(cid)
                
        if cid in self.bank_max:
            del self.bank_max[cid]

    def _periodic_flush_loop(self):
        while not self._stop_flush_event.wait(timeout=self.FLUSH_INTERVAL_SECONDS):
            self._flush_batch_thread_safe()

    def _flush_batch_thread_safe(self):
        with self.batch_lock:
            self._flush_batch_locked()

    def _flush_batch_locked(self):
        if not self.batch_ops:
            return
            
        # 1. Escribir a disco (seguro)
        if hasattr(self.snapshot_manager, 'apply_batch'):
            self.snapshot_manager.apply_batch(self.batch_ops)
        else:
            for op in self.batch_ops:
                self.snapshot_manager.apply_operation(op)
        
        # 2. FIX: Delegar la confirmación a RabbitMQ de forma thread-safe
        for ack_func in self.batch_acks:
            if callable(ack_func):
                # IMPORTANTE: Cambia 'self.input_exchange' por 'self.input_queue' 
                # si en este worker en particular consumes desde una cola.
                self.input_exchange._connection.add_callback_threadsafe(ack_func)
                
        self.batch_ops.clear()
        self.batch_acks.clear()

    def process_message(self, raw_msg, ack, nack):
        try:
            msg = message_protocol.internal.deserialize(raw_msg)
            cid = msg.source_client_uuid

            # Enrutamiento limpio hacia handlers específicos
            if msg.type == InternalMessageType.DATA_PER_BANK_SHUFFLER_TO_MAP_MAX_AMOUNT_PER_BANK:
                self._handle_data(cid, msg, ack)
            elif msg.type == InternalMessageType.EOF_GENERIC_MESSAGE:
                self._handle_eof(cid, msg, ack)
            else:
                ack() # Ignorar otros tipos de mensaje
                
        except Exception as e:
            logging.exception(e)
            nack()

    def _handle_data(self, cid, msg, ack):
        self._add_inflight(cid)
        
        from_bank = msg.data.get("from_bank")
        amount = msg.data.get("amount_received")
        origin = msg.data.get("account_origin")

        # Early return si faltan datos clave
        if amount is None or from_bank is None:
            self._dec_inflight(cid)
            self._try_finalize(cid, ack)
            ack()
            return

        bank_id = int(from_bank)
        
        # Recuperar estado del SnapshotManager usando setdefault para inicializar limpio
        client_data = self.state.setdefault(cid, {})
        current = client_data.get(bank_id)

        if current is None or amount > current[0]:
            client_data[bank_id] = (amount, origin)
            
            op = {
                'type': 'update',
                'path': [cid, bank_id],
                'value': (amount, origin)
            }
            
            with self.batch_lock:
                self.batch_ops.append(op)
                self.batch_acks.append(ack)
                if len(self.batch_ops) >= self.BATCH_MAX_SIZE:
                    self._flush_batch_locked()
        else:
            ack()

        self._dec_inflight(cid)
        self._try_finalize(cid, ack)

    def _handle_eof(self, cid, msg, ack):
        logging.debug(f"Mapper {self.id} received EOF for client {cid}")

        self._flush_batch_thread_safe()
        
        self._add_inflight(cid)
        client_data = self.state.get(cid, {})

        for from_bank, (amount, origin) in client_data.items():
            partition = stable_hash(from_bank) % JOIN_AMOUNT
            routing_key = f"{JOIN_ROUTING_KEY_PREFIX}_{partition}"
            result_bytes = MapperMessageHandler.serialize_result(
                cid, msg.data_id, from_bank, amount, origin
            )
            with self._join_exchange_lock:
                self.join_exchange.send(result_bytes, routing_key=routing_key)
                
        logging.info(f"Mapper {self.id} sent {len(client_data)} results for client {cid}")
        
        # 3. Propagar EOF a los Joiners
        eof_bytes = MapperMessageHandler.serialize_eof_message(cid)
        for i in range(JOIN_AMOUNT):
            routing_key = f"{JOIN_ROUTING_KEY_PREFIX}_{i}"
            with self._join_exchange_lock:
                self.join_exchange.send(eof_bytes, routing_key=routing_key)

        self._dec_inflight(cid)
        
        # 4. Lógica de finalización
        with self._inflight_lock:
            inflight = self._inflight.get(cid, 0)
            
        if inflight == 0:
            self._finalize_client(cid, ack)
        else:
            with self._pending_lock:
                self._pending.add(cid)
                
        self._finalize_client(cid, ack)

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
