import os
import logging
import signal
import sys
import threading

from common import middleware, message_protocol
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler
from common.logging.logging_config import configure_logging_from_env
from message_handler import MessageHandler as AmountFilterQ3MessageHandler
from csv_file_manager import CSVFileManager

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
USD_FILTER_Q3_QUEUE = os.environ["INPUT_QUEUE"] #Es la propia, que conecta con el filtro USD Q3
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

class AmountFilterQ3:

    def __init__(self):
        self.usd_filter_q3_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, USD_FILTER_Q3_QUEUE
        )
        self.average_per_pay_format_to_filter_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE, [AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE]
        )
        
        self.id = int(ID)

        # definicion de working queue exchanges de la instancia posterior
        self.gateway_final_query_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE
            )

        # CSV File Manager for persisting pending transactions
        self.csv_file_manager = CSVFileManager()
        self.producer_lock = threading.Lock()

        self.all_averages_received_for_client : dict[str, bool] = {}
        self.all_averages_received_for_client_lock = threading.Lock()
        self.averages_by_client : dict[str, dict[str, float]] = {}
        self.averages_by_client_lock = threading.Lock()
        
        self.eof_controller = EOFController(MOM_HOST, self.id, AMOUNT_FILTER_PREFIX, AMOUNT_FILTER_AMOUNT, EOF_CONTROL_EXCHANGE, EXPECTED_INPUT_EOFS,self.on_consensus_ok_callback,self.on_send_eof_to_next_stage_callback, self.on_clean_client_in_main_thread_callback,AUXILIARY_INPUT)
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
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_JOINER_TO_AMOUNT_FILTER_Q3:
                logging.debug(f"Received AVERAGE_PER_PAY_FORMAT_JOINER_TO_AMOUNT_FILTER_Q3 message for client {message.source_client_uuid}")
                client_id = message.source_client_uuid
                self._process_average_message(message.data, client_id)
                self.eof_controller.on_processed_packet_by_client(client_id, INPUT_PREFIX_2)
            case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                client_id = message.source_client_uuid
                self.eof_controller.on_input_queue_eof_reception(client_id, message.data)
        ack()

    def _process_average_message(self, average_data, client_id):
        average_per_pay_format = average_data.get("averages", {})
        for payment_format, data in average_per_pay_format.items():
            with self.averages_by_client_lock:
                self.averages_by_client.setdefault(client_id, {})[payment_format] = data["average"] * 0.01

    def on_consensus_ok_callback(self, client_id):
        with self.all_averages_received_for_client_lock: #actualizo que ya tengo todas las medias para el cliente, 
            self.all_averages_received_for_client[client_id] = True
        
        # Procesar los pendientes que tenga guardados en el CSV para ese cliente
        pending_transactions = self.csv_file_manager.read_all_transactions(client_id)
        for pending_transaction, data_id, message_id in pending_transactions:
            self._filter_data_with_averages(client_id, data_id, pending_transaction, message_id)


    def process_usd_filter_q3_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.USD_FILTER_Q3_TO_AMOUNT_FILTER_Q3:
                client_id = message.source_client_uuid
                self._process_transaction(message.data, client_id, message.data_id, message.message_id)
                self.eof_controller.on_processed_packet_by_client(client_id, INPUT_PREFIX_1)
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

    def on_send_eof_to_next_stage_callback(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        with self.producer_lock:
            self.gateway_final_query_queue.send(EOFMessageHandler.serialize_eof_message(client_id, totals_by_output.get(OUTPUT_PREFIX_1, 0), origin_worker_prefix, amount_origin_workers))
        logging.info(f"Sent final EOF for client {client_id} to gateway final query queue")


    def on_clean_client_in_main_thread_callback(self, client_id):
        # Clean up CSV file after client is finalized
        self.csv_file_manager.delete_csv_file(client_id)
        with self.averages_by_client_lock:
            if client_id in self.averages_by_client:
                del self.averages_by_client[client_id]


    def stop(self):
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
