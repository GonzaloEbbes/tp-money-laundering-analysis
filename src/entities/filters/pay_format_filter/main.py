import hashlib
import os
import logging
from random import randint
import re
import signal
import sys
import threading
from time import sleep

from common import middleware, message_protocol
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler
from common.conversions import (
    ConversionRateProviderError,
    conversion_key,
    conversion_shard,
    to_frankfurter_currency,
)
from common.logging.logging_config import configure_logging_from_env
from message_handler import MessageHandler as PayFormatFilterMessageHandler

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"] #es el date filter
PAY_FORMAT_FILTER_PREFIX = os.environ["PAY_FORMAT_FILTER_PREFIX"]
PAY_FORMAT_FILTER_AMOUNT = int(os.environ["PAY_FORMAT_FILTER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]

CONVERSION_EXCHANGE = os.environ["CONVERSION_EXCHANGE"]
CONVERSION_QUEUE_PREFIX = os.environ.get("CONVERSION_QUEUE_PREFIX", "currency_converter_queue")
CONVERSION_ROUTING_KEY_PREFIX = os.environ.get("CONVERSION_ROUTING_KEY_PREFIX", "conversion")
TOTAL_CONVERSION_WORKERS = int(os.environ["TOTAL_CONVERSION_WORKERS"])
AMOUNT_FILTER_Q5_QUEUE = os.environ["AMOUNT_FILTER_Q5_QUEUE"]
EXPECTED_INPUT_EOFS = int(os.environ["EXPECTED_INPUT_EOFS"]) #son 1
INPUT_PREFIX_1 = os.environ["INPUT_PREFIX_1"] #que es el prefix del date filter
AUXILIARY_INPUT = os.environ["AUXILIARY_INPUT"] == "true" #va en false
OUTPUT_PREFIX_1 = os.environ["OUTPUT_PREFIX_1"] #al amount filter q5
OUTPUT_PREFIX_2 = os.environ["OUTPUT_PREFIX_2"] #al currency converter

class PayFormatFilter:

    def __init__(self):
        self.date_filter_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        
        self.id = int(ID)

        self.usd_currency_converter_exchange = middleware.MessageMiddlewareExchangePublisherRabbitMQ(
                MOM_HOST,
                CONVERSION_EXCHANGE,
                bindings=[
                    (f"{CONVERSION_QUEUE_PREFIX}_{shard}", self._conversion_routing_key(shard))
                    for shard in range(TOTAL_CONVERSION_WORKERS)
                ],
            )
        self.amount_filter_q5_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, AMOUNT_FILTER_Q5_QUEUE
            )
        logging.debug(
            "PayFormatFilter wiring: input_queue=%s conversion_exchange=%s conversion_queue_prefix=%s "
            "conversion_workers=%s amount_filter_q5_queue=%s",
            INPUT_QUEUE,
            CONVERSION_EXCHANGE,
            CONVERSION_QUEUE_PREFIX,
            TOTAL_CONVERSION_WORKERS,
            AMOUNT_FILTER_Q5_QUEUE,
        )

        self._sigterm_received = False
        self._runtime_error = False

        self.producer_lock = threading.Lock()
        self._stop_lock = threading.Lock()
        self._stopping = False

        self.eof_controller = EOFController(MOM_HOST, self.id, PAY_FORMAT_FILTER_PREFIX, PAY_FORMAT_FILTER_AMOUNT, EOF_CONTROL_EXCHANGE, EXPECTED_INPUT_EOFS,None,self.on_send_eof_to_next_stage_callback, None,AUXILIARY_INPUT)
    
    def _run_date_filter_consumer(self):
        try:
            self.date_filter_queue.start_consuming(self.process_date_filter_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Date filter consumer crashed")

    
    def process_date_filter_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.DATE_FILTER_TO_PAY_FORMAT_FILTER:
                client_id = message.source_client_uuid
                self._process_transaction(message.data, client_id, message.data_id)
                self.eof_controller.on_processed_packet_by_client(client_id, INPUT_PREFIX_1)
            case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                client_id = message.source_client_uuid
                self.eof_controller.on_input_queue_eof_reception(client_id, message.data)
        ack()
        

    def _process_transaction(self, transaction_data, client_id, data_id):
        logging.debug(f"Received DATE_FILTER_TO_PAY_FORMAT_FILTER for client {client_id}")
        payment_format = transaction_data.get("payment_format")

        if payment_format in ["ACH", "Wire"]:
            try:
                currency = to_frankfurter_currency(transaction_data.get("payment_currency"))
            except ConversionRateProviderError:
                logging.exception("Could not normalize payment currency. payload=%s", transaction_data)
                return

            if currency == "USD":
                with self.producer_lock:
                    self.amount_filter_q5_queue.send(
                        PayFormatFilterMessageHandler.serialize_amount_filter_q5_queue_message(
                            client_id,
                            data_id,
                            transaction_data,
                        )
                    )
                self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_1, client_id)
                return

            try:
                key = conversion_key(currency, transaction_data.get("timestamp"))
                shard = conversion_shard(key, TOTAL_CONVERSION_WORKERS)
            except ConversionRateProviderError:
                logging.exception("Could not route currency conversion. payload=%s", transaction_data)
                return
            routing_key = self._conversion_routing_key(shard)
            transaction_data = dict(transaction_data)
            transaction_data["payment_currency"] = currency
            transaction_data["conversion_key"] = key
            transaction_data["conversion_shard"] = shard
            with self.producer_lock:
                self.usd_currency_converter_exchange.send(
                    PayFormatFilterMessageHandler.serialize_usd_currency_converter_queue_message(
                        client_id,
                        data_id,
                        transaction_data,
                    ),
                    routing_key=routing_key,
                )
            self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_2, client_id)
            logging.debug(f"Transaction for client {client_id} sent to USD Currency Converter shard {shard}")

        

    def on_send_eof_to_next_stage_callback(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        eof_message = EOFMessageHandler.serialize_eof_message(client_id, totals_by_output.get(OUTPUT_PREFIX_2, 0), origin_worker_prefix, amount_origin_workers)

        random_shard = randint(0, TOTAL_CONVERSION_WORKERS - 1)
        with self.producer_lock:
            self.usd_currency_converter_exchange.send(
                eof_message,
                routing_key=self._conversion_routing_key(random_shard),
            )
        eof_message = EOFMessageHandler.serialize_eof_message(client_id, totals_by_output.get(OUTPUT_PREFIX_1, 0), origin_worker_prefix, amount_origin_workers)
        self.amount_filter_q5_queue.send(eof_message)
        logging.info(f"Sent final EOF for client {client_id} to all downstream queues")

    def _conversion_routing_key(self, shard):
        return f"{CONVERSION_ROUTING_KEY_PREFIX}.{shard}"


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
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.date_filter_queue]
        if self.usd_currency_converter_exchange is not None:
            resources.append(self.usd_currency_converter_exchange)
        if self.amount_filter_q5_queue is not None:
            resources.append(self.amount_filter_q5_queue)

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

        process_thread = threading.Thread(
            target=self._run_date_filter_consumer,
            name="date-filter-thread",
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
            logging.error(e)
            self.stop()
            return max(eof_exit_code, 2)

        finally:
            self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return max(eof_exit_code, 1)

        return max(eof_exit_code, 0)


def main():
    configure_logging_from_env()
    usd_filter_q4 = PayFormatFilter()

    def _handle_sigterm(signum, frame):
        logging.debug("SIGTERM received in pay format filter")
        usd_filter_q4.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return usd_filter_q4.start()


if __name__ == "__main__":
    sys.exit(main())
