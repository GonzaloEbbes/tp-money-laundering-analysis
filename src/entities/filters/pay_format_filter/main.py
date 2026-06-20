import hashlib
import os
import logging
import re
import signal
import threading
from time import sleep

from common import middleware, message_protocol
from common.conversions import (
    ConversionRateProviderError,
    conversion_key,
    conversion_shard,
    to_frankfurter_currency,
)
from common.dedup import InMemoryDeduplicator, message_dedup_key
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


class PayFormatFilter:

    def __init__(self):
        self.date_filter_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        
        self.id = int(ID)
        self.deduplicator = InMemoryDeduplicator()

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
        logging.info(
            "PayFormatFilter wiring: input_queue=%s conversion_exchange=%s conversion_queue_prefix=%s "
            "conversion_workers=%s amount_filter_q5_queue=%s",
            INPUT_QUEUE,
            CONVERSION_EXCHANGE,
            CONVERSION_QUEUE_PREFIX,
            TOTAL_CONVERSION_WORKERS,
            AMOUNT_FILTER_Q5_QUEUE,
        )

        #Exchange de control EOF
        self.pay_format_filter_eof_exchange_consumer = None
        self.pay_format_filter_eof_exchange_producer = None
        if PAY_FORMAT_FILTER_AMOUNT > 1:
            pay_format_filters = []
            for i in range(PAY_FORMAT_FILTER_AMOUNT):
                if i != self.id:
                    pay_format_filters.append(f"{PAY_FORMAT_FILTER_PREFIX}_{i}")
        
            self.pay_format_filter_eof_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST,
                    EOF_CONTROL_EXCHANGE,
                    [f"{PAY_FORMAT_FILTER_PREFIX}_{self.id}"],
                )
            self._eof_producer_lock = threading.Lock()
            self.pay_format_filter_eof_exchange_producer = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST,
                    EOF_CONTROL_EXCHANGE,
                    pay_format_filters,
                )


        self._sigterm_received = False
        self._runtime_error = False

        if (self._is_leader()):
            self.total_eof_received_by_client = {}
            self._leader_eof_lock = threading.Lock()

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
    
    def _run_date_filter_consumer(self):
        try:
            self.date_filter_queue.start_consuming(self.process_date_filter_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Date filter consumer crashed")

    
    def _run_control_consumer(self):
        try:
            self.pay_format_filter_eof_exchange_consumer.start_consuming(self.process_eof_control_message)
        except Exception as e:
            self._handle_runtime_failure(e, "Control consumer crashed")
    
    def process_date_filter_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.DATE_FILTER_TO_PAY_FORMAT_FILTER:
                dedup_key = message_dedup_key(message)
                client_id = message.source_client_uuid
                if not self.deduplicator.should_process(client_id, dedup_key):
                    ack()
                    return
                self._add_inflight_message(message.source_client_uuid)
                self._process_transaction(message.data, client_id, message.data_id, message.message_id)
                self._decrease_inflight_message(message.source_client_uuid)
                self._check_and_finalize_client_if_pending(client_id)
                self.deduplicator.mark_processed(client_id, dedup_key)
            case message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                client_id = message.source_client_uuid
                self._process_datefilter_eof(client_id)
        ack()
        

    def _process_transaction(self, transaction_data, client_id, data_id, message_id=None):
        logging.debug(f"Received DATE_FILTER_TO_PAY_FORMAT_FILTER for client {client_id}")
        payment_format = transaction_data.get("payment_format")

        if payment_format in ["ACH", "Wire"]:
            try:
                currency = to_frankfurter_currency(transaction_data.get("payment_currency"))
            except ConversionRateProviderError:
                logging.exception("Could not normalize payment currency. payload=%s", transaction_data)
                return

            if currency == "USD":
                self.amount_filter_q5_queue.send(
                    PayFormatFilterMessageHandler.serialize_amount_filter_q5_queue_message(
                        client_id,
                        data_id,
                        transaction_data,
                        message_id=message_id,
                    )
                )
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
            self.usd_currency_converter_exchange.send(
                PayFormatFilterMessageHandler.serialize_usd_currency_converter_queue_message(
                    client_id,
                    data_id,
                    transaction_data,
                    message_id=message_id,
                ),
                routing_key=routing_key,
            )
            logging.debug(f"Transaction for client {client_id} sent to USD Currency Converter shard {shard}")

        

    def send_final_eof(self, client_id):
        eof_message = PayFormatFilterMessageHandler.serialize_eof_message(client_id)
        for shard in range(TOTAL_CONVERSION_WORKERS):
            logging.info(
                "Q5 route: sending EOF to converter shard client=%s exchange=%s routing_key=%s expected_converter_queue=%s",
                client_id,
                CONVERSION_EXCHANGE,
                self._conversion_routing_key(shard),
                f"{CONVERSION_QUEUE_PREFIX}_{shard}",
            )
            self.usd_currency_converter_exchange.send(
                eof_message,
                routing_key=self._conversion_routing_key(shard),
            )
        logging.info(
            "Q5 route: sending EOF directly to amount filter client=%s queue=%s",
            client_id,
            AMOUNT_FILTER_Q5_QUEUE,
        )
        self.amount_filter_q5_queue.send(PayFormatFilterMessageHandler.serialize_eof_message(client_id))
        logging.debug(f"Sent final EOF for client {client_id} to all downstream queues")

    def _conversion_routing_key(self, shard):
        return f"{CONVERSION_ROUTING_KEY_PREFIX}.{shard}"
    
    def _process_datefilter_eof(self, client_id):
        logging.debug(f"Received EOF for client {client_id}")

        if PAY_FORMAT_FILTER_AMOUNT > 1:
            with self._eof_producer_lock:
                self.pay_format_filter_eof_exchange_producer.send(PayFormatFilterMessageHandler.serialize_eof_message(client_id))
            logging.debug(f"Sent EOF for client {client_id} to other pay format filters")

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

        if self._is_leader():
            self._leader_count_eof_for_client(client_id)
        else:
            self.send_eof_leader_message(client_id)

        with self._is_pending_to_finalize_client_lock:
            if client_id in self._is_pending_to_finalize_client:
                self._is_pending_to_finalize_client.remove(client_id)
        

    def send_eof_leader_message(self, client_id):
        with self._eof_producer_lock:
            self.pay_format_filter_eof_exchange_producer.send(PayFormatFilterMessageHandler.serialize_eof_leader_message(client_id))
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
            
            if self.total_eof_received_by_client[client_id] == PAY_FORMAT_FILTER_AMOUNT:
                logging.debug(f"Leader ha recibido EOF de todos los filtros para el cliente {client_id}. Enviando EOF a la capa siguiente.")
                should_send_final_eof = True
                del self.total_eof_received_by_client[client_id]
        
        if should_send_final_eof:
            self.send_final_eof(client_id)

    def _process_eof_from_control_exchange(self, client_id):
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

        consumers = [self.date_filter_queue]
        if self.pay_format_filter_eof_exchange_consumer is not None:
            consumers.append(self.pay_format_filter_eof_exchange_consumer)

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.date_filter_queue]
        if self.pay_format_filter_eof_exchange_consumer is not None:
            resources.append(self.pay_format_filter_eof_exchange_consumer)
        if self.usd_currency_converter_exchange is not None:
            resources.append(self.usd_currency_converter_exchange)
        if self.amount_filter_q5_queue is not None:
            resources.append(self.amount_filter_q5_queue)
        if self.pay_format_filter_eof_exchange_producer is not None:
            resources.append(self.pay_format_filter_eof_exchange_producer)

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
        if PAY_FORMAT_FILTER_AMOUNT > 1:
            control_thread = threading.Thread(
                target=self._run_control_consumer,
                name="pay-format    -control-consumer-thread",
            )

        control_started = False

        try:
            if PAY_FORMAT_FILTER_AMOUNT > 1:
                control_thread.start()
                control_started = True
            self._run_date_filter_consumer()

        except Exception as e:
            logging.error(e)
            self.stop()
            self._close_resources()
            return 2

        if control_started:
            control_thread.join()

        self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return 1

        return 0


def main():
    configure_logging_from_env()
    usd_filter_q4 = PayFormatFilter()

    def _handle_sigterm(signum, frame):
        logging.debug("SIGTERM received in pay format filter")
        usd_filter_q4.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return usd_filter_q4.start()


if __name__ == "__main__":
    main()
