import os
import logging
import signal
import sys
import threading
from common import middleware, message_protocol
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler
from common.logging.logging_config import configure_logging_from_env
from common.snapshots.stateful_worker import StatefulWorker
from message_handler import MessageHandler as AmountFilterQ3MessageHandler

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
USD_FILTER_Q3_QUEUE = os.environ["INPUT_QUEUE"]
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

        self.gateway_final_query_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE
            )
        self.producer_lock = threading.Lock()


        self.state.setdefault('averages_by_client', {})
        self.state.setdefault('pending_transactions', {})
        self.state.setdefault('averages_received', {})
        
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
    
    def _process_average_message(self, average_data, client_id, ack):
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
        self.clean_client_data(client_id, ['averages_by_client', 'averages_received', 'pending_transactions'])

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
        eof_exit_code=0

        try:
            usd_filter_q3_thread.start()
            usd_q3_filter_thread_started = True
            average_per_pay_format_thread.start()
            average_per_pay_format_thread_started = True
            eof_exit_code = self.eof_controller.start()

        except Exception as e:
            logging.error(e)
            self.stop()
            self._close_resources()
            return max(eof_exit_code, 2)

        finally: 
            if usd_q3_filter_thread_started:
                usd_filter_q3_thread.join()
            if average_per_pay_format_thread_started:
                average_per_pay_format_thread.join()

            self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return max(eof_exit_code, 1)

        return max(eof_exit_code, 0)


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
