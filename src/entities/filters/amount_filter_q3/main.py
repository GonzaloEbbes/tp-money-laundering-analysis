import os
import logging
import signal
import threading
from common import middleware, message_protocol
from common.logging.logging_config import configure_logging_from_env
from common.snapshots.snapshot import SnapshotManager
from message_handler import MessageHandler as AmountFilterQ3MessageHandler

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
USD_FILTER_Q3_QUEUE = os.environ["INPUT_QUEUE"]
AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE = os.environ["AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE"]
AMOUNT_FILTER_PREFIX = os.environ["AMOUNT_FILTER_PREFIX"]
AMOUNT_FILTER_AMOUNT = int(os.environ["AMOUNT_FILTER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]

OUTPUT_QUEUE = os.environ["GATEWAY_FINAL_QUERY_QUEUE"]

class AmountFilterQ3:

    def __init__(self):
        self.usd_filter_q3_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, USD_FILTER_Q3_QUEUE
        )
        self.average_per_pay_format_to_filter_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE, [AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE]
        )
        
        self.id = int(ID)

        self.gateway_final_query_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE
            )

        self.amount_filter_eof_exchange_consumer = None
        self.amount_filter_eof_exchange_producer = None

        # Usar UN SOLO LOCK para proteger la escritura a gateway_final_query_queue
        self.producer_lock = threading.Lock()

        data_dir = f"/data/snapshots/amount_filter_q3_{self.id}"
        self.snapshot_manager = SnapshotManager(data_dir)
        self.state = self.snapshot_manager.recover()
        
        if 'averages_received' not in self.state:
            self.snapshot_manager.apply_operation({'type': 'set', 'key': 'averages_received', 'value': {}})
        if 'averages_by_client' not in self.state:
            self.snapshot_manager.apply_operation({'type': 'set', 'key': 'averages_by_client', 'value': {}})

        self._eof_producer_lock = threading.Lock()
        self._eof_counter_by_client : dict[str, int] = {}
        self._eof_counter_lock = threading.Lock()

        if AMOUNT_FILTER_AMOUNT > 1:
            amount_filters = []
            for i in range(AMOUNT_FILTER_AMOUNT):
                if i != self.id:
                    amount_filters.append(f"{AMOUNT_FILTER_PREFIX}_{i}")
        
            self.amount_filter_eof_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST,
                    EOF_CONTROL_EXCHANGE,
                    [f"{AMOUNT_FILTER_PREFIX}_{self.id}"],
                )
            self.amount_filter_eof_exchange_producer = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST,
                    EOF_CONTROL_EXCHANGE,
                    amount_filters,
                )
            
        self.all_averages_received_for_client : dict[str, bool] = {}
        self.all_averages_received_for_client_lock = threading.Lock()
        
        self._sigterm_received = False
        self._runtime_error = False

        if self._is_leader():
            self.total_eof_received_by_client = {}
            self._leader_eof_lock = threading.Lock()

        self.BATCH_MAX_SIZE = 1000
        self.FLUSH_INTERVAL_SECONDS = 20.0
        self.batch_ops = []
        self.batch_acks = []
        self.batch_lock = threading.Lock()
        
        self._stop_flush_event = threading.Event()
        self._flush_thread = threading.Thread(
            target=self._periodic_flush_loop,
            daemon=True,
            name=f"flush-q3-{self.id}"
        )
        self._flush_thread.start()

        self._is_pending_to_finalize_client = set()
        self._is_pending_to_finalize_client_lock = threading.Lock()
        self._finalized_clients = set()
        self._finalized_clients_lock = threading.Lock()
        self._inflight_messages = {}
        self._inflight_message_lock = threading.Lock()
        self._stop_lock = threading.Lock()
        self._stopping = False
    
    def _is_leader(self):
        return self.id == 0
    
    def _run_usd_filter_q3_consumer(self):
        try:
            self.usd_filter_q3_queue.start_consuming(self.process_usd_filter_q3_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "USD filter Q3 consumer crashed")

    def _run_average_per_pay_format_aggregator(self):
        try:
            self.average_per_pay_format_to_filter_exchange_consumer.start_consuming(self.process_average_per_pay_format_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Average per pay format consumer crashed")

    def _run_control_consumer(self):
        try:
            self.amount_filter_eof_exchange_consumer.start_consuming(self.process_eof_control_message)
        except Exception as e:
            self._handle_runtime_failure(e, "Control consumer crashed")

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
        
        for conn, ack_func in self.batch_acks:
            if conn is not None and callable(ack_func):
                conn.add_callback_threadsafe(ack_func)
                
        self.batch_ops.clear()
        for conn, ack_func in self.batch_acks:
            if conn is not None and callable(ack_func):
                conn.add_callback_threadsafe(ack_func)
        self.batch_acks.clear()
    
    def process_average_per_pay_format_messages(self, message, ack, nack):
        try:
            message = message_protocol.internal.deserialize(message)
            match message.type:
                case message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_JOINER_TO_AMOUNT_FILTER_Q3:
                    client_id = message.source_client_uuid
                    self._process_average_message(message.data, client_id, ack)
                    ack()
                case message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                    client_id = message.source_client_uuid
                    self._process_eof_average_per_pay_format(client_id, ack)
                case _:
                    ack()
        except Exception as _:
            logging.exception("Error processing average message")
            nack()

    def _process_average_message(self, average_data, client_id, ack):
        average_data = average_data or {}
        average_per_pay_format = average_data.get("averages", {})
        with self.batch_lock:
            for payment_format, data in average_per_pay_format.items():
                avg_value = data["average"] * 0.01
                self.state['averages_by_client'].setdefault(client_id, {})[payment_format] = avg_value
                
                op = {
                    'type': 'update',
                    'path': ['averages_by_client', client_id, payment_format],
                    'value': avg_value
                }
                self.batch_ops.append(op)
                
            self.batch_acks.append((self.average_per_pay_format_to_filter_exchange_consumer._connection, ack))
            if len(self.batch_ops) >= self.BATCH_MAX_SIZE:
                self._flush_batch_locked()
                
    def _process_eof_average_per_pay_format(self, client_id, ack):
        self._flush_batch_thread_safe()
        self.state['averages_received'][client_id] = True
        op = {
            'type': 'update',
            'path': ['averages_received', client_id],
            'value': True
        }

        with self.batch_lock:
            self.batch_ops.append(op)
            self.batch_acks.append((self.average_per_pay_format_to_filter_exchange_consumer._connection, ack))
            self._flush_batch_locked()

        pending_key = f'pending_{client_id}'
        pending_transactions = self.state.get(pending_key, {})

        if isinstance(pending_transactions, list):
            logging.warning("Detectada lista en transacciones pendientes. Limpia los volumenes de Docker.")
            pending_transactions = {tx.get('data_id', 'unknown'): tx for tx in pending_transactions if isinstance(tx, dict)}

        # FIX 2: Se lee y procesa unicamente desde la memoria del SnapshotManager (Chau CSV)
        for data_id, transaction in pending_transactions.items():
            self._filter_data_with_averages(client_id, data_id, transaction)

        with self.all_averages_received_for_client_lock: 
            self.all_averages_received_for_client[client_id] = True
        
        with self._eof_counter_lock: 
            self._eof_counter_by_client[client_id] = self._eof_counter_by_client.get(client_id, 0) + 1
            obtenidosDosEofs = self._eof_counter_by_client[client_id] == 2
        
        if obtenidosDosEofs:
            with self._inflight_message_lock:
                if self._inflight_messages.get(client_id, 0) > 0:
                    logging.debug(f"EOF received for client {client_id} from averages but there are still inflight messages. Marking client as finalized but waiting for inflight messages to finish.")
                    with self._is_pending_to_finalize_client_lock:
                        self._is_pending_to_finalize_client.add(client_id)
                else:
                    logging.debug(f"EOF received for client {client_id} from averages and no inflight messages. Finalizing client.")
                    self._finalize_client(client_id)
                        
    def process_usd_filter_q3_messages(self, message, ack, nack):
        try:
            message = message_protocol.internal.deserialize(message)
            match message.type:
                case message_protocol.internal.InternalMessageType.USD_FILTER_Q3_TO_AMOUNT_FILTER_Q3:
                    self._add_inflight_message(message.source_client_uuid)
                    client_id = message.source_client_uuid
                    self._process_transaction(message.data, client_id, message.data_id, ack)
                    self._decrease_inflight_message(message.source_client_uuid)
                    self._check_and_finalize_client_if_pending(client_id)
                case message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                    client_id = message.source_client_uuid
                    self._process_usd_filter_q3_eof(client_id)
                    ack()
                case _:
                    ack()
        except Exception as _:
            logging.exception("Error processing transaction message")
            nack()
        
    def _process_transaction(self, transaction_data, client_id, data_id, ack):
        logging.debug(f"Received USD_FILTER_Q3_TO_AMOUNT_FILTER_Q3 for client {client_id}")
        averages_received = self.state.get('averages_received', {}).get(client_id, False)

        if not averages_received:
            pending = self.state.setdefault(f'pending_{client_id}', {})
            pending[data_id] = transaction_data
            
            op ={
                'type': 'update',
                'path': [f'pending_{client_id}', data_id],
                'value': transaction_data
            }
            with self.batch_lock:
                self.batch_ops.append(op)
                self.batch_acks.append((self.usd_filter_q3_queue._connection, ack))
                if len(self.batch_ops) >= self.BATCH_MAX_SIZE:
                    self._flush_batch_locked()
        else:
            self._filter_data_with_averages(client_id, data_id, transaction_data)
            ack()
                
    def _filter_data_with_averages(self, client_id, data_id, transaction_data):
        amount_received = float(transaction_data.get("amount_received", 0))
        
        client_averages = self.state.get('averages_by_client', {}).get(client_id, {})
        average_centesimal_to_compare = client_averages.get(transaction_data.get("payment_format"), 0)

        if amount_received > 0 and amount_received < average_centesimal_to_compare:
            # FIX 3: Usamos producer_lock (no cambies esto a eof_producer_lock)
            with self.producer_lock:
                self.gateway_final_query_queue.send(AmountFilterQ3MessageHandler.serialize_gateway_query_message(client_id, data_id, transaction_data))
            logging.debug(f"Transaction for client {client_id} sent to final gateway queue")

    def send_final_eof(self, client_id):
        # FIX 3: Protegemos el uso de gateway_final_query_queue con producer_lock para evitar el Index Error
        with self.producer_lock:
            self.gateway_final_query_queue.send(AmountFilterQ3MessageHandler.serialize_eof_message(client_id))
        logging.info(f"Sent final EOF for client {client_id} to gateway final query queue")
    
    def _process_usd_filter_q3_eof(self, client_id):
        logging.debug(f"Received EOF for client {client_id}")

        with self._eof_counter_lock:
            self._eof_counter_by_client[client_id] = self._eof_counter_by_client.get(client_id, 0) + 1
            obtenidosTodosLosEofs = self._eof_counter_by_client[client_id] >= 2

        # Propagamos a los peers si hay múltiples replicas
        if AMOUNT_FILTER_AMOUNT > 1:
            with self._eof_producer_lock:
                self.amount_filter_eof_exchange_producer.send(AmountFilterQ3MessageHandler.serialize_eof_message(client_id))

        # Si tenemos los dos EOFs de entrada, finalizamos
        if obtenidosTodosLosEofs:
            with self._inflight_message_lock:
                if self._inflight_messages.get(client_id, 0) > 0:
                    with self._is_pending_to_finalize_client_lock:
                        self._is_pending_to_finalize_client.add(client_id)
                else:
                    self._finalize_client(client_id)
    
    def _check_and_finalize_client_if_pending(self, client_id):
        should_finalize = False

        with self._is_pending_to_finalize_client_lock:
            is_pending = client_id in self._is_pending_to_finalize_client

        if is_pending:
            with self._inflight_message_lock:
                should_finalize = self._inflight_messages.get(client_id, 0) == 0

        if should_finalize:
            logging.debug(f"Finalizando cliente {client_id} que estaba pendiente")
            self._finalize_client(client_id)
                        
    def _finalize_client(self, client_id):
        with self._finalized_clients_lock:
            if client_id in self._finalized_clients:
                return
            logging.debug(f"Finalizando cliente {client_id}")
            self._finalized_clients.add(client_id)

        # Limpiar la data persistida de las transacciones en vuelo del snapshot
        with self.batch_lock:
            self.batch_ops.append({'type': 'delete', 'key': f'pending_{client_id}'})
            self.batch_acks.append((None, None))
            self._flush_batch_locked()

        if self._is_leader():
            self._leader_count_eof_for_client(client_id)
        else:
            self.send_eof_leader_message(client_id)

        with self._is_pending_to_finalize_client_lock:
            if client_id in self._is_pending_to_finalize_client:
                self._is_pending_to_finalize_client.remove(client_id)

    def send_eof_leader_message(self, client_id):
        with self._eof_producer_lock:
            self.amount_filter_eof_exchange_producer.send(AmountFilterQ3MessageHandler.serialize_eof_leader_message(client_id))
        logging.debug(f"Sent EOF_LEADER_MESSAGE for client {client_id} to leader")

    def _add_inflight_message(self, client_id):
        with self._inflight_message_lock:
            self._inflight_messages[client_id] = self._inflight_messages.get(client_id, 0) + 1
    
    def _decrease_inflight_message(self, client_id):
        with self._inflight_message_lock:
            if client_id in self._inflight_messages:
                self._inflight_messages[client_id] = self._inflight_messages.get(client_id, 0) - 1

    def process_eof_control_message(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                logging.debug(f"Received EOF_GENERIC_MESSAGE for client {message.source_client_uuid}")
                self._process_eof_from_control_exchange(message.source_client_uuid)
            case message_protocol.internal.InternalMessageType.EOF_LEADER_MESSAGE:
                if self._is_leader():
                    logging.debug(f"Received EOF_LEADER_MESSAGE for client {message.source_client_uuid}")
                    self._leader_count_eof_for_client(message.source_client_uuid)
        ack()

    def _leader_count_eof_for_client(self, client_id):
        should_send_final_eof = False
        with self._leader_eof_lock:
            self.total_eof_received_by_client[client_id] = self.total_eof_received_by_client.get(client_id, 0) + 1
            
            if self.total_eof_received_by_client[client_id] == AMOUNT_FILTER_AMOUNT:
                logging.debug(f"Leader ha recibido EOF de todos los filtros para el cliente {client_id}. Enviando EOF a la capa siguiente.")
                should_send_final_eof = True
                del self.total_eof_received_by_client[client_id]
        
        if should_send_final_eof:
            self.send_final_eof(client_id)

    def _process_eof_from_control_exchange(self, client_id):
        with self._eof_counter_lock:
            self._eof_counter_by_client[client_id] = self._eof_counter_by_client.get(client_id, 0) + 1
            if self._eof_counter_by_client[client_id] < 2:
                return

        with self._inflight_message_lock:
            if self._inflight_messages.get(client_id, 0) > 0:
                logging.debug(f"EOF received for client {client_id} but there are still inflight messages. Marking client as finalized but waiting for inflight messages to finish.")
                with self._is_pending_to_finalize_client_lock:
                    self._is_pending_to_finalize_client.add(client_id)
            else:
                logging.debug(f"EOF received for client {client_id} and no inflight messages. Finalizing client.")
                self._finalize_client(client_id)

    def stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True
        self._stop_flush_event.set()
        self._flush_thread.join()
        self._flush_batch_thread_safe()
        
        # Apagado thread-safe
        if hasattr(self, 'usd_filter_q3_queue') and self.usd_filter_q3_queue._connection:
            self.usd_filter_q3_queue._connection.add_callback_threadsafe(self.usd_filter_q3_queue.stop_consuming)
        if hasattr(self, 'average_per_pay_format_to_filter_exchange_consumer') and self.average_per_pay_format_to_filter_exchange_consumer._connection:
            self.average_per_pay_format_to_filter_exchange_consumer._connection.add_callback_threadsafe(self.average_per_pay_format_to_filter_exchange_consumer.stop_consuming)
        if self.amount_filter_eof_exchange_consumer and self.amount_filter_eof_exchange_consumer._connection:
            self.amount_filter_eof_exchange_consumer._connection.add_callback_threadsafe(self.amount_filter_eof_exchange_consumer.stop_consuming)

    def _close_resources(self):
        resources = [
            self.usd_filter_q3_queue,
            self.average_per_pay_format_to_filter_exchange_consumer,
        ]
        if self.amount_filter_eof_exchange_consumer is not None:
            resources.append(self.amount_filter_eof_exchange_consumer)
        if self.gateway_final_query_queue is not None:
            resources.append(self.gateway_final_query_queue)
        if self.amount_filter_eof_exchange_producer is not None:
            resources.append(self.amount_filter_eof_exchange_producer)

        for resource in resources:
            try:
                resource.close()
            except Exception as e:
                logging.error(f"Error closing resource: {e}")

    def notify_sigterm(self):
            self._sigterm_received = True
            self.stop()

    def _handle_runtime_failure(self, error, context):
        logging.error(f"{context}: {error}")
        self._runtime_error = True
        self.stop()
    
    def start(self):
        usd_filter_q3_thread = threading.Thread(
        target=self._run_usd_filter_q3_consumer,
        name="usd-q3-consumer-thread",
        )

        average_per_pay_format_thread = threading.Thread(
        target=self._run_average_per_pay_format_aggregator,
        name="average-per-pay-format-consumer-thread",
        )

        if AMOUNT_FILTER_AMOUNT > 1:
            control_thread = threading.Thread(
                target=self._run_control_consumer,
                name="amount-control-consumer-thread",
            )

        usd_q3_filter_thread_started = False
        average_per_pay_format_thread_started = False
        control_started = False

        try:
            usd_filter_q3_thread.start()
            usd_q3_filter_thread_started = True
            average_per_pay_format_thread.start()
            average_per_pay_format_thread_started = True
            if AMOUNT_FILTER_AMOUNT > 1:
                control_thread.start()
                control_started = True

        except Exception as e:
            logging.error(e)
            self.stop()
            self._close_resources()
            return 2

        if usd_q3_filter_thread_started:
            usd_filter_q3_thread.join()
        if average_per_pay_format_thread_started:
            average_per_pay_format_thread.join()
        if control_started:
            control_thread.join()

        self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return 1

        return 0

def main():
    configure_logging_from_env()
    amount_filter_q3 = AmountFilterQ3()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in amount filter q3")
        amount_filter_q3.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return amount_filter_q3.start()

if __name__ == "__main__":
    main()
