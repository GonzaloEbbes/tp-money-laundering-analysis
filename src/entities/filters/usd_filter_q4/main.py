import os
import logging
import signal
import sys
import threading

from common import middleware, message_protocol
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler
from common.dedup import InMemoryDeduplicator
from common.logging.logging_config import configure_logging_from_env
from message_handler import MessageHandler as USDFilterMessageHandler

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"] #es el date filter
USD_FILTER_PREFIX = os.environ["USD_FILTER_PREFIX"]
USD_FILTER_AMOUNT = int(os.environ["USD_FILTER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]

AVERAGE_PER_PAY_FORMAT_MAPPER_QUEUE = os.environ["AVERAGE_PER_PAY_FORMAT_MAPPER_QUEUE"]
SCATHER_GATHER_QUEUE = os.environ["SCATHER_GATHER_QUEUE"]

EXPECTED_INPUT_EOFS = int(os.environ["EXPECTED_INPUT_EOFS"]) #1
INPUT_PREFIX_1 = os.environ["INPUT_PREFIX_1"] #que es el prefix de date filter
AUXILIARY_INPUT = os.environ["AUXILIARY_INPUT"] == "true" #va false
OUTPUT_PREFIX_1 = os.environ["OUTPUT_PREFIX_1"] #scatter gather mapper
OUTPUT_PREFIX_2 = os.environ["OUTPUT_PREFIX_2"] #average per pay format mapper

class USDFilterQ4:

    def __init__(self):
        self.date_filter_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        
        self.id = int(ID)

        self.producer_lock = threading.Lock()

        # definicion de working queue exchanges de la instancia posterior
        self.average_per_pay_format_mapper_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, AVERAGE_PER_PAY_FORMAT_MAPPER_QUEUE
            )
        self.scather_gather_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, SCATHER_GATHER_QUEUE
            )

        self._sigterm_received = False
        self._runtime_error = False

        self._stop_lock = threading.Lock()
        self._stopping = False
        self.deduplicator = InMemoryDeduplicator()

        self.eof_controller = EOFController(MOM_HOST, self.id, USD_FILTER_PREFIX, USD_FILTER_AMOUNT, EOF_CONTROL_EXCHANGE, EXPECTED_INPUT_EOFS,None,self.on_send_eof_to_next_stage_callback, None, AUXILIARY_INPUT)

    
    def _run_date_filter_consumer(self):
        try:
            self.date_filter_queue.start_consuming(self.process_date_filter_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Date filter consumer crashed")

    def process_date_filter_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.DATE_FILTER_TO_USD_FILTER_Q4:
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
        receiving_currency = transaction_data.get("receiving_currency")
        payment_currency = transaction_data.get("payment_currency")

        if receiving_currency == "US Dollar" and payment_currency == "US Dollar":
            with self.producer_lock:
                self.average_per_pay_format_mapper_queue.send(USDFilterMessageHandler.serialize_average_per_pay_format_mapper_message(client_id, data_id, transaction_data, message_id=message_id))
                self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_2, client_id)
                self.scather_gather_queue.send(USDFilterMessageHandler.serialize_scatter_gather_message(client_id, data_id, transaction_data, message_id=message_id))
                self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_1, client_id)
            logging.debug(f"Transaction for client {client_id} sent to average per pay format mapper and scatter gather mapper")

        
    def on_send_eof_to_next_stage_callback(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        with self.producer_lock:
            self.average_per_pay_format_mapper_queue.send(EOFMessageHandler.serialize_eof_message(client_id,totals_by_output.get(OUTPUT_PREFIX_2, 0), origin_worker_prefix, amount_origin_workers))
            self.scather_gather_queue.send(EOFMessageHandler.serialize_eof_message(client_id,totals_by_output.get(OUTPUT_PREFIX_1, 0), origin_worker_prefix, amount_origin_workers))
        logging.info(f"Sent final EOF for client {client_id} to all downstream queues")

    def _dedup_key(self, message):
        if message.message_id is None:
            return None
        return f"{message.type}:{message.message_id}"

    def _should_process_message(self, message):
        return self.deduplicator.should_process(
            message.source_client_uuid, self._dedup_key(message)
        )


    def stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        consumers = [self.date_filter_queue]

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.exception("Error stopping USDFilterQ4 consumer: %s", e)

    def _close_resources(self):
        resources = [self.date_filter_queue]
        if self.average_per_pay_format_mapper_queue is not None:
            resources.append(self.average_per_pay_format_mapper_queue)
        if self.scather_gather_queue is not None:
            resources.append(self.scather_gather_queue)

        for resource in resources:
            try:
                resource.close()
            except Exception as e:
                logging.exception("Error closing USDFilterQ4 resource %s: %s", type(resource).__name__, e)

    def notify_sigterm(self):
        self._sigterm_received = True
        self.stop()
        self.eof_controller.on_sigterm()

    def _handle_runtime_failure(self, error, context):
        logging.exception("%s: %s", context, error)
        self._runtime_error = True
        self.stop()
        self.eof_controller.on_stop()
    
    def start(self):

        process_thread = threading.Thread(
            target=self._run_date_filter_consumer,
            name="date-filter-consumer-thread",
        )

        processing_thread_started = False
        eof_exit_code=0

        try:
            process_thread.start()
            processing_thread_started = True
            eof_exit_code = self.eof_controller.start()

            if processing_thread_started:
                process_thread.join()

        except Exception as e:
            logging.exception("USDFilterQ4 start failed")
            self.stop()
            return max(eof_exit_code, 2)

        finally:
            self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return max(eof_exit_code, 1)

        return max(eof_exit_code, 0)


def main():
    configure_logging_from_env()
    usd_filter_q4 = USDFilterQ4()

    def _handle_sigterm(signum, frame):
        logging.debug("SIGTERM received in usd filter q4")
        usd_filter_q4.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return usd_filter_q4.start()


if __name__ == "__main__":
    sys.exit(main())
