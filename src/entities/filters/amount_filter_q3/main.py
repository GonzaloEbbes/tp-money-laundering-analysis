import os
import logging
import signal
import sys
import threading
from common import middleware, message_protocol
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.healthcheck.recovery_controller import RecoveryController
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler
from common.dedup import InMemoryDeduplicator, message_dedup_key
from common.logging.logging_config import configure_logging_from_env
from common.snapshots.stateful_worker import StatefulWorker
from message_handler import MessageHandler as AmountFilterQ3MessageHandler

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
<<<<<<< HEAD
USD_FILTER_Q3_QUEUE = os.environ["INPUT_QUEUE"]
=======
RECOVERY_PREFIX = os.environ.get("RECOVERY_PREFIX", "recovery")
RECOVERY_AMOUNT = int(os.environ.get("RECOVERY_AMOUNT", "1"))
HEARTBEAT_EXCHANGE = os.environ.get("HEARTBEAT_EXCHANGE", "heartbeat_exchange")
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "2"))
USD_FILTER_Q3_QUEUE = os.environ["INPUT_QUEUE"] #Es la propia, que conecta con el filtro USD Q3
>>>>>>> origin/add-recovery-controller
AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE = os.environ["AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE"]
AMOUNT_FILTER_PREFIX = os.environ["AMOUNT_FILTER_PREFIX"]
AMOUNT_FILTER_AMOUNT = int(os.environ["AMOUNT_FILTER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]

OUTPUT_QUEUE = os.environ["GATEWAY_FINAL_QUERY_QUEUE"]
EXPECTED_INPUT_EOFS = int(os.environ["EXPECTED_INPUT_EOFS"]) #son 2
INPUT_PREFIX_1 = os.environ["INPUT_PREFIX_1"] #que es el prefix del usd filter usdfilter q3
INPUT_PREFIX_2 = os.environ["INPUT_PREFIX_2"] #que es el prefix del average per pay format joiner
AUXILIARY_INPUT = os.environ["AUXILIARY_INPUT"] == "true" #va en true
OUTPUT_PREFIX_1 = os.environ["OUTPUT_PREFIX_1"] 

class AmountFilterQ3(StatefulWorker):

    def __init__(self):
        super().__init__(
            data_dir=f"/data/snapshots/amount_filter_q3{ID}",
            set_keys=['averages_by_client', 'pending_transactions', 'averages_received']
            )
        self.usd_filter_q3_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, USD_FILTER_Q3_QUEUE
        )
        self.average_per_pay_format_to_filter_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE, [AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE]
        )
        
        self.id = int(ID)

<<<<<<< HEAD
=======
        self.recovery_producer_controller = RecoveryController(
            mom_host=MOM_HOST,
            heartbeat_exchange=HEARTBEAT_EXCHANGE,
            id=ID,
            prefix=AMOUNT_FILTER_PREFIX,
            recovery_prefix=RECOVERY_PREFIX,
            recovery_amount=RECOVERY_AMOUNT,
            heartbeat_interval=HEARTBEAT_INTERVAL,
        )

        # definicion de working queue exchanges de la instancia posterior
>>>>>>> origin/add-recovery-controller
        self.gateway_final_query_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE
            )
        self.producer_lock = threading.Lock()

<<<<<<< HEAD

        self.state.setdefault('averages_by_client', {})
        self.state.setdefault('pending_transactions', {})
        self.state.setdefault('averages_received', {})
=======
        self.all_averages_received_for_client : dict[str, bool] = {}
        self.all_averages_received_for_client_lock = threading.Lock()
        self.averages_by_client : dict[str, dict[str, float]] = {}
        self.averages_by_client_lock = threading.Lock()
        self.deduplicator = InMemoryDeduplicator()
>>>>>>> origin/add-recovery-controller
        
        self.eof_controller = EOFController(
            MOM_HOST, 
            self.id, 
            AMOUNT_FILTER_PREFIX, 
            AMOUNT_FILTER_AMOUNT, 
            EOF_CONTROL_EXCHANGE, 
            EXPECTED_INPUT_EOFS,
            self.on_consensus_ok_callback,
            self.on_send_eof_to_next_stage_callback, 
            self.on_clean_client_in_main_thread_callback,
            AUXILIARY_INPUT,
            self.state.setdefault('eof_state', {}),
            self.append_to_batch
        )
        self._sigterm_received = False
        self._runtime_error = False

        self._stop_lock = threading.Lock()
        self._stopping = False

    
    def _run_usd_filter_q3_consumer(self):
        try:
            self.usd_filter_q3_queue.start_consuming(self.process_usd_filter_q3_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "USD filter Q3 consumer crashed")

    def _run_average_per_pay_format_aggregator_consumer(self):
        try:
            self.average_per_pay_format_to_filter_exchange_consumer.start_consuming(self.process_average_per_pay_format_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Average per pay format consumer crashed")

    def process_average_per_pay_format_messages(self, message, ack, nack):
<<<<<<< HEAD
        try:
            message = message_protocol.internal.deserialize(message)
            client_id = message.source_client_uuid
            match message.type:
                case message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_JOINER_TO_AMOUNT_FILTER_Q3:
                    logging.debug(f"Received AVERAGE_PER_PAY_FORMAT_JOINER_TO_AMOUNT_FILTER_Q3 for client {client_id}")
                    self._process_average_message(message.data, client_id)
                case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                    self.eof_controller.on_input_queue_eof_reception(client_id, message.data)
            self.append_to_batch(None, self.average_per_pay_format_to_filter_exchange_consumer._connection, ack)
        except Exception as e:
            logging.error(f"Error processing average message: {e}")
            nack()
    
=======
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_JOINER_TO_AMOUNT_FILTER_Q3:
                if not self._should_process_message(message):
                    ack()
                    return
                logging.debug(f"Received AVERAGE_PER_PAY_FORMAT_JOINER_TO_AMOUNT_FILTER_Q3 message for client {message.source_client_uuid}")
                client_id = message.source_client_uuid
                self._process_average_message(message.data, client_id)
                self.eof_controller.on_processed_packet_by_client(client_id, INPUT_PREFIX_2)
                self.deduplicator.mark_processed(client_id, self._dedup_key(message))
            case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                client_id = message.source_client_uuid
                self.eof_controller.on_input_queue_eof_reception(client_id, message.data)
        ack()

>>>>>>> origin/add-recovery-controller
    def _process_average_message(self, average_data, client_id):
        averages = average_data.get("averages", {})
        if not averages:
            return
        for payment_format, data in averages.items():
            avg_value = data["average"] * 0.01
            self.state['averages_by_client'].setdefault(client_id, {})[payment_format] = avg_value
            self.state_update(['averages_by_client', client_id, payment_format], avg_value)
        self.state['averages_received'][client_id] = True
        self.state_update(['averages_received', client_id], True)
        self._process_pending_transactions(client_id)

    def _process_pending_transactions(self, client_id):
        """Procesa todas las transacciones pendientes de un cliente."""
        pending = self.state.get('pending_transactions', {}).get(client_id, {})
        if not pending:
            return
        
<<<<<<< HEAD
        for data_id, transaction_data in pending.items():
            self._filter_and_send(client_id, data_id, transaction_data)
        
        if client_id in self.state.get('pending_transactions', {}):
            del self.state['pending_transactions'][client_id]
            self.state_delete(['pending_transactions', client_id])
            
    def on_consensus_ok_callback(self, client_id):
        if self.state.get('averages_received', {}).get(client_id, False):
            self._process_pending_transactions(client_id)
        else:
            logging.debug(f"Consensus OK for client {client_id}, but averages not received yet")

    def process_usd_filter_q3_messages(self, message, ack, nack):
        try:
            message = message_protocol.internal.deserialize(message)
            client_id = message.source_client_uuid
            match message.type:
                case message_protocol.internal.InternalMessageType.USD_FILTER_Q3_TO_AMOUNT_FILTER_Q3:
                    self._process_transaction(message.data, client_id, message.data_id)
                    self.eof_controller.on_processed_packet_by_client(client_id, INPUT_PREFIX_1)
                case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                    self.eof_controller.on_input_queue_eof_reception(client_id, message.data)
            self.append_to_batch(None, self.usd_filter_q3_queue._connection, ack)
        except Exception as e:
            logging.error(f"Error processing transaction: {e}")
            nack()
    
    def _filter_and_send(self, client_id, data_id, transaction_data):
        amount = float(transaction_data.get("amount_received", 0))
        payment_format = transaction_data.get("payment_format")
        if not payment_format:
            return

        avg = self.state.get('averages_by_client', {}).get(client_id, {}).get(payment_format, 0)
        if amount > 0 and amount < avg:
            with self.producer_lock:
                self.gateway_final_query_queue.send(
                    AmountFilterQ3MessageHandler.serialize_gateway_query_message(
                        client_id, data_id, transaction_data
                    )
                )
                self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_1, client_id)
            logging.debug(f"Transaction for client {client_id} sent to gateway (amount={amount} < avg={avg})")
        else:
            logging.debug(f"Transaction for client {client_id} filtered out (amount={amount} >= avg={avg})")

    def _process_transaction(self, transaction_data, client_id, data_id):
        if not self.ensure_idempotent(client_id, data_id):
            logging.debug(f"Data ID {data_id} already processed for client {client_id}, skipping")
            return
=======
        # Procesar los pendientes que tenga guardados en el CSV para ese cliente
        pending_transactions = self.csv_file_manager.read_all_transactions(client_id)
        for pending_transaction, data_id, message_id in pending_transactions:
            self._filter_data_with_averages(client_id, data_id, pending_transaction, message_id)


    def process_usd_filter_q3_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.USD_FILTER_Q3_TO_AMOUNT_FILTER_Q3:
                if not self._should_process_message(message):
                    ack()
                    return
                client_id = message.source_client_uuid
                self._process_transaction(message.data, client_id, message.data_id, message.message_id)
                self.eof_controller.on_processed_packet_by_client(client_id, INPUT_PREFIX_1)
                self.deduplicator.mark_processed(client_id, self._dedup_key(message))
            case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                client_id = message.source_client_uuid
                self.eof_controller.on_input_queue_eof_reception(client_id, message.data)
        ack()
        

    def _process_transaction(self, transaction_data, client_id, data_id, message_id=None):
        logging.debug(f"Received USD_FILTER_Q3_TO_AMOUNT_FILTER_Q3 for client {client_id}")
        with self.all_averages_received_for_client_lock:
            if not self.all_averages_received_for_client.get(client_id, False):
                logging.debug(f"Aún no se han recibido todas las medias para el cliente {client_id}. Guardando transacción en CSV pendiente.")
                self.csv_file_manager.append_transaction(client_id, transaction_data, data_id, message_id)
            else:
                self._filter_data_with_averages(client_id, data_id, transaction_data, message_id)
        
    def _filter_data_with_averages(self, client_id, data_id, transaction_data, message_id=None):
        amount_received = float(transaction_data.get("amount_received", 0))
        with self.averages_by_client_lock:
            average_centesimal_to_compare = self.averages_by_client.get(client_id, {}).get(transaction_data.get("payment_format"), 0)
        if amount_received > 0 and amount_received < average_centesimal_to_compare :
            with self.producer_lock:
                self.gateway_final_query_queue.send(AmountFilterQ3MessageHandler.serialize_gateway_query_message(client_id, data_id, transaction_data, message_id=message_id))
            self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_1, client_id)
            logging.debug(f"Transaction for client {client_id} sent to final gateway queue")
>>>>>>> origin/add-recovery-controller

        if self.state.get('averages_received', {}).get(client_id, False):
            self._filter_and_send(client_id, data_id, transaction_data)
        else:
            self.state.setdefault('pending_transactions', {}).setdefault(client_id, {})[data_id] = transaction_data
            self.state_update(['pending_transactions', client_id, data_id], transaction_data)
        
    def on_send_eof_to_next_stage_callback(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        with self.producer_lock:
            self.gateway_final_query_queue.send(
                EOFMessageHandler.serialize_eof_message(
                    client_id,
                    totals_by_output.get(OUTPUT_PREFIX_1, 0),
                    origin_worker_prefix,
                    amount_origin_workers
                )
            )
        logging.info(f"Sent final EOF for client {client_id} to gateway final query queue")


    def on_clean_client_in_main_thread_callback(self, client_id):
<<<<<<< HEAD
        self.clean_client_data(client_id, ['averages_by_client', 'averages_received', 'pending_transactions'])
=======
        # Clean up CSV file after client is finalized
        self.csv_file_manager.delete_csv_file(client_id)
        with self.averages_by_client_lock:
            if client_id in self.averages_by_client:
                del self.averages_by_client[client_id]
        self.deduplicator.remove_client(client_id)

    def _dedup_key(self, message):
        return message_dedup_key(message)

    def _should_process_message(self, message):
        return self.deduplicator.should_process(
            message.source_client_uuid, self._dedup_key(message)
        )

>>>>>>> origin/add-recovery-controller

    def stop(self):
        self.stop_recoverable_worker()
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        consumers = [
            self.usd_filter_q3_queue,
            self.average_per_pay_format_to_filter_exchange_consumer,
        ]

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")
        self.stop_recoverable_worker()


    def _close_resources(self):
        resources = [
            self.usd_filter_q3_queue,
            self.average_per_pay_format_to_filter_exchange_consumer,
        ]

        if self.gateway_final_query_queue is not None:
            resources.append(self.gateway_final_query_queue)

        for resource in resources:
            try:
                resource.close()
            except Exception as e:
                logging.error(f"Error closing resource: {e}")

    def notify_sigterm(self):
        self._sigterm_received = True
        self.stop()
        self.eof_controller.on_sigterm()
        self.recovery_producer_controller.on_sigterm()

    def _handle_runtime_failure(self, error, context):
        logging.error(f"{context}: {error}")
        self._runtime_error = True
        self.stop()
        self.eof_controller.on_stop()
    
    def start(self):

        usd_filter_q3_thread = threading.Thread(
        target=self._run_usd_filter_q3_consumer,
        name="usd-q3-consumer-thread",
        )

        average_per_pay_format_thread = threading.Thread(
        target=self._run_average_per_pay_format_aggregator_consumer,
        name="average-per-pay-format-consumer-thread",
        )


        usd_q3_filter_thread_started = False
        average_per_pay_format_thread_started = False
        stop_recovery_controller_callback = None
        eof_exit_code=0
        recovery_controller_exit_code = 0

        try:
            stop_recovery_controller_callback = (
                self.recovery_producer_controller.start_recovery_producer_controller()
            )

            usd_filter_q3_thread.start()
            usd_q3_filter_thread_started = True
            average_per_pay_format_thread.start()
            average_per_pay_format_thread_started = True
            eof_exit_code = self.eof_controller.start()

        except Exception as e:
            logging.error(e)
            self.stop()
            self._close_resources()
            return max(eof_exit_code, recovery_controller_exit_code, 2)

        finally: 
            if stop_recovery_controller_callback is not None:
                recovery_controller_exit_code = stop_recovery_controller_callback()

            if usd_q3_filter_thread_started:
                usd_filter_q3_thread.join()
            if average_per_pay_format_thread_started:
                average_per_pay_format_thread.join()

            self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return max(eof_exit_code, recovery_controller_exit_code, 1)

        return max(eof_exit_code, recovery_controller_exit_code, 0)


def main():
    configure_logging_from_env()
    amount_filter_q3 = AmountFilterQ3()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in amount filter q3")
        amount_filter_q3.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return amount_filter_q3.start()


if __name__ == "__main__":
    sys.exit(main())
