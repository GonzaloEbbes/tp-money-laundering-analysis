import os
import logging
import signal
import threading
from common import middleware, message_protocol
from common.logging.logging_config import configure_logging_from_env
from common.message_protocol.internal import InternalMessageType
from common.snapshots.snapshot import SnapshotManager
from message_handler import MessageHandler as JoinMessageHandler

ID = int(os.environ.get("ID", 0))
JOIN_AMOUNT = int(os.environ.get("JOIN_AMOUNT", 1))
MAP_AMOUNT = int(os.environ.get("MAP_AMOUNT", 1))
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
JOIN_EXCHANGE = os.environ.get("JOIN_EXCHANGE", "query2_join_exchange")
JOIN_ROUTING_KEY_PREFIX = os.environ.get("JOIN_ROUTING_KEY_PREFIX", "join_partition")
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

        data_dir = f"/data/snapshots/join_max_{self.id}"
        self.snapshot_manager = SnapshotManager(data_dir)
        self.state = self.snapshot_manager.recover()
        
        self.BATCH_MAX_SIZE = 100
        self.FLUSH_INTERVAL_SECONDS = 2.0
        self.batch_ops = []
        self.batch_acks = []
        self.batch_lock = threading.Lock()

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

        # 4. Locks & Concurrency state
        self.total_eof_leader = {}
        self._eof_lock = threading.Lock()
        self._pending = set()
        self._pending_lock = threading.Lock()
        self._finalized = set()
        self._finalized_lock = threading.Lock()
        self._inflight = {}
        self._inflight_lock = threading.Lock()
        self._stop = False
        self._stop_lock = threading.Lock()
        self._output_queue_lock = threading.Lock()
        self._eof_producer_lock = threading.Lock()

        self._stop_flush_event = threading.Event()
        self._flush_thread = threading.Thread(
            target=self._periodic_flush_loop,
            daemon=True,
            name=f"flush-joinmax-{self.id}"
        )
        self._flush_thread.start()

    # --- MÉTODOS DE MICRO-BATCHING ---
    def _periodic_flush_loop(self):
        while not self._stop_flush_event.wait(timeout=self.FLUSH_INTERVAL_SECONDS):
            self._flush_batch_thread_safe()

    def _flush_batch_thread_safe(self):
        with self.batch_lock:
            self._flush_batch_locked()

    def _flush_batch_locked(self):
        if not self.batch_ops:
            return
            
        if hasattr(self.snapshot_manager, 'apply_batch'):
            self.snapshot_manager.apply_batch(self.batch_ops)
        else:
            for op in self.batch_ops:
                self.snapshot_manager.apply_operation(op)
        
        for ack_func in self.batch_acks:
            if callable(ack_func):
                self.input_exchange._connection.add_callback_threadsafe(ack_func)
                
        self.batch_ops.clear()
        self.batch_acks.clear()

    def _add_to_batch(self, op, ack):
        with self.batch_lock:
            self.batch_ops.append(op)
            self.batch_acks.append(ack)
            if len(self.batch_ops) >= self.BATCH_MAX_SIZE:
                self._flush_batch_locked()

    # --- MANEJO DE INFLIGHT Y FINALIZACIÓN ---
    def _add_inflight(self, cid):
        with self._inflight_lock:
            self._inflight[cid] = self._inflight.get(cid, 0) + 1

    def _dec_inflight(self, cid):
        with self._inflight_lock:
            self._inflight[cid] -= 1
            current_inflight = self._inflight[cid]
            
        if current_inflight == 0:
            pending = False
            with self._pending_lock:
                if cid in self._pending:
                    self._pending.remove(cid)
                    pending = True
            
            # Si no quedan mensajes en vuelo y el cliente estaba pendiente de cruzar datos, hacé el join
            if pending:
                self._perform_join_and_finalize(cid)

    # --- RECEPCIÓN Y RUTEO DE MENSAJES ---
    def process_message(self, raw_msg, ack, nack):
        try:
            msg = message_protocol.internal.deserialize(raw_msg)
            cid = msg.source_client_uuid

            if msg.type == InternalMessageType.BANK_FILTER_TO_JOINER:
                if msg.data is None:
                    self._handle_accounts_eof(cid, msg, ack)
                else:
                    self._handle_bank_name(cid, msg, ack)
            elif msg.type == InternalMessageType.MAX_AMOUNT_PER_BANK_RESULT:
                self._handle_max_amount(cid, msg, ack)
            elif msg.type == InternalMessageType.EOF_GENERIC_MESSAGE:
                self._handle_mappers_eof(cid, msg, ack)
            else:
                ack()
                
        except Exception as e:
            logging.exception(e)
            nack()
    
    def _handle_bank_name(self, cid, msg, ack):
        self._add_inflight(cid)
        bank_id = int(msg.data.get("bank_id"))
        bank_name = msg.data.get("bank_name")

        names_key = f"{cid}_names"
        names_dict = self.state.setdefault(names_key, {})
        names_dict[bank_id] = bank_name

        op = {'type': 'update', 'path': [names_key, bank_id], 'value': bank_name}
        self._add_to_batch(op, ack)
        self._dec_inflight(cid)

    def _handle_max_amount(self, cid, msg, ack):
        self._add_inflight(cid)
        bank_id = int(msg.data.get("from_bank"))
        amount = msg.data.get("amount_received")
        origin = msg.data.get("account_origin")

        amounts_key = f"{cid}_amounts"
        amounts_dict = self.state.setdefault(amounts_key, {})
        current = amounts_dict.get(bank_id)

        if current is None or amount > current[0]:
            amounts_dict[bank_id] = (amount, origin)
            op = {'type': 'update', 'path': [amounts_key, bank_id], 'value': (amount, origin)}
            self._add_to_batch(op, ack)
        else:
            ack()
            
        self._dec_inflight(cid)

    def _handle_accounts_eof(self, cid, msg, ack):
        self._add_inflight(cid)
        logging.info(f"Join {self.id} received EOF from accounts for client {cid}")
        
        counters_key = f"{cid}_eof_counters"
        counters = self.state.setdefault(counters_key, {})
        counters['accounts'] = True
        
        op = {'type': 'update', 'path': [counters_key, 'accounts'], 'value': True}
        self._add_to_batch(op, ack)
        
        self._dec_inflight(cid)
        self._try_flush(cid)

    def _handle_mappers_eof(self, cid, msg, ack):
        self._add_inflight(cid)
        
        counters_key = f"{cid}_eof_counters"
        counters = self.state.setdefault(counters_key, {})
        current_mappers = counters.get('mappers', 0) + 1
        counters['mappers'] = current_mappers
        
        op = {'type': 'update', 'path': [counters_key, 'mappers'], 'value': current_mappers}
        self._add_to_batch(op, ack)
        
        logging.info(f"Join {self.id} received EOF from mapper {current_mappers}/{MAP_AMOUNT} for client {cid}")
        
        self._dec_inflight(cid)
        self._try_flush(cid)

    def _try_flush(self, cid):
        """Verifica si llegaron todos los EOFs necesarios para cruzar los datos."""
        counters = self.state.get(f"{cid}_eof_counters", {})
        accounts_eof = counters.get('accounts', False)
        mappers_eof_count = counters.get('mappers', 0)

        # Si aún faltan fuentes de datos por cerrar, no hacemos nada
        if not accounts_eof or mappers_eof_count < MAP_AMOUNT:
            return

        logging.info(f"Join {self.id} received ALL EOFs for client {cid}. Ready to flush.")
        
        with self._inflight_lock:
            inflight = self._inflight.get(cid, 0)
            
        # Si aún hay datos de este cliente encolados/procesándose, marcamos como pendiente
        if inflight > 0:
            with self._pending_lock:
                self._pending.add(cid)
            return

        # Si todo está listo, procedemos al join y limpieza
        self._perform_join_and_finalize(cid)

    def _perform_join_and_finalize(self, cid):
        # 1. Asegurar persistencia de la RAM al WAL antes del cruce final
        self._flush_batch_thread_safe()
        
        amounts_dict = self.state.get(f"{cid}_amounts", {})
        names_dict = self.state.get(f"{cid}_names", {})
        
        # 2. Realizar el Join final
        for bank_id, (amount, origin) in amounts_dict.items():
            bank_name = names_dict.get(int(bank_id), "Unknown")
            if bank_name == "Unknown":
                logging.warning(f"Join {self.id} could not find bank name for bank_id {bank_id} for client {cid}")
                
            with self._output_queue_lock:
                self.output_queue.send(JoinMessageHandler.serialize_result(
                    cid, None, bank_name, origin, amount
                ))
                
        # 3. Limpiar estado persistido para no agotar el WAL ni la RAM
        op1 = {'type': 'delete', 'key': f"{cid}_amounts"}
        op2 = {'type': 'delete', 'key': f"{cid}_names"}
        op3 = {'type': 'delete', 'key': f"{cid}_eof_counters"}
        
        with self.batch_lock:
            self.batch_ops.extend([op1, op2, op3])
            self._flush_batch_locked() # Flush limpieza inmediato
            
        # 4. Finalizar cliente informando a la red
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
                    with self._output_queue_lock:
                        self.output_queue.send(JoinMessageHandler.serialize_eof_message(cid))
                    del self.total_eof_leader[cid]
        else:
            if self.eof_producer:
                with self._eof_producer_lock:
                    self.eof_producer.send(JoinMessageHandler.serialize_eof_leader_message(cid), routing_key=f"join_{self.id}")

    # --- CONTROL LOOP Y CIERRE ---
    def start(self):
        if self.eof_consumer:
            threading.Thread(target=self._control_loop, daemon=True).start()
        self.input_exchange.start_consuming(self.process_message)
        self.input_exchange.close()
        self.output_queue.close()
        if self.eof_consumer:
            self.eof_consumer.close()

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
                if self.total_eof_leader[cid] == self.total_instances:
                    with self._output_queue_lock:
                        self.output_queue.send(JoinMessageHandler.serialize_eof_message(cid))
                    del self.total_eof_leader[cid]
        ack()

    def stop(self):
        if not self._stop_lock:
            if self._stop:
                return
            self._stop = True

        self._stop_flush_event.set()
        if hasattr(self, '_flush_thread') and self._flush_thread:
            self._flush_thread.join()
        self._flush_batch_thread_safe()
       
        self.input_exchange._connection.add_callback_threadsafe(
            self.input_exchange.stop_consuming
        )
        
        if self.eof_consumer:
            self.eof_consumer._connection.add_callback_threadsafe(
                self.eof_consumer.stop_consuming
            )

def main():
    configure_logging_from_env()
    w = JoinMaxAmountPerBank()
    signal.signal(signal.SIGTERM, lambda *_: w.stop())
    w.start()

if __name__ == "__main__":
    main()