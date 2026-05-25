import hashlib
import os
import logging
import re
import signal
import threading
from time import sleep

from common import middleware, message_protocol, fruit_item
from entities.filters.date_filter.message_handler.message_handler import MessageHandler as DateFilterMessageHandler

logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s.%(msecs)03d - %(message)s',
            datefmt='%H:%M:%S'
        )

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
DATE_FILTER_PREFIX = int(os.environ["DATE_FILTER_AMOUNT"])
DATE_FILTER_AMOUNT = int(os.environ["DATE_FILTER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]

USD_FILTER_Q3_QUEUE = os.environ["USD_FILTER_Q3_QUEUE"]
USD_FILTER_Q4_QUEUE = os.environ["USD_FILTER_Q4_QUEUE"]
PAY_FORMAT_FILTER_QUEUE = os.environ["PAY_FORMAT_FILTER_QUEUE"]



class DateFilter:

    def __init__(self):
        self.gateway_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        
        self.id = int(ID)

        # definicion de working queue exchanges de la instancia posterior
        self.usd_filters_q3_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, USD_FILTER_Q3_QUEUE
            )

        self.usd_filters_q4_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, USD_FILTER_Q4_QUEUE
            )
        self.pay_format_filter_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, PAY_FORMAT_FILTER_QUEUE
            )

        #Exchange de control EOF
        if DATE_FILTER_AMOUNT > 1:
            date_filters = []
            for i in range(DATE_FILTER_AMOUNT):
                if i != self.id:
                    date_filters.append(f"{DATE_FILTER_PREFIX}_{i}")
        
            self.date_filter_eof_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST,
                    EOF_CONTROL_EXCHANGE,
                    date_filters,
                )

        self._sigterm_received = False
        self._runtime_error = False

        
        self._finalized_clients = set()
        self._finalized_clients_lock = threading.Lock()
        self._stop_lock = threading.Lock()
        self._stopping = False
    
    def _run_gateway_consumer(self):
        try:
            self.gateway_queue.start_consuming(self.process_gateway_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Gateway consumer crashed")

    
    def _run_control_consumer(self):
        try:
            self.date_filter_eof_exchange.start_consuming(self.process_eof_control_message)
        except Exception as e:
            self._handle_runtime_failure(e, "Control consumer crashed")
    
    def process_gateway_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.GATEWAY_TO_DATE_FILTER:
                client_id = message.source_client_uuid
                self._process_transaction(message.data, client_id, message.data_id)
                self._check_if_eof_received(client_id)
            case message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                client_id = message.source_client_uuid
                self._process_gateway_eof(client_id,message.data)
        ack()

    def _process_transaction(self, transaction_data, client_id, data_id):
        logging.info(f"Received GATEWAY_TO_DATE_FILTER for client {client_id}")
        timestamp = transaction_data.get("timestamp")

        if not re.match(r'^\d{4}/\d{2}/\d{2} \d{2}:\d{2}$', timestamp[:16]):
            logging.warning(f"Timestamp inválido para cliente {client_id}: {timestamp}")
            return
        
        (anio,mes,dia) = (timestamp[0:4], timestamp[5:7], timestamp[8:10])

        if anio == "2022" and mes == "09":
            if dia in ["01", "02", "03", "04", "05"]:
                message = {
                    "account_origin": transaction_data["account_origin"],
                    "amount_received": transaction_data["amount_received"],
                    "receiving_currency": transaction_data["receiving_currency"]
                }
                self.usd_filters_q3_queue.send(
                    DateFilterMessageHandler.serialize_usd_filter_q3_message(client_id, data_id, message)
                )
            elif dia in ["06", "07", "08", "09", "10", "11", "12", "13", "14", "15"]:
                message = {
                    "account_origin": transaction_data["account_origin"],
                    "account_destination": transaction_data["account_destination"],
                    "amount_received": transaction_data["amount_received"],
                    "receiving_currency": transaction_data["receiving_currency"],
                    "payment_format": transaction_data["payment_format"]
                }
                self.usd_filters_q4_queue.send(
                    DateFilterMessageHandler.serialize_usd_filter_q4_message(client_id, data_id, message)
                )
                message = {
                    "timestamp": transaction_data["timestamp"],
                    "amount_paid": transaction_data["amount_paid"],
                    "payment_currency": transaction_data["payment_currency"],
                    "payment_format": transaction_data["payment_format"]
                }
                self.pay_format_filter_queue.send(
                    DateFilterMessageHandler.serialize_pay_format_filter_message(client_id, data_id, message)
                )

    def _process_gateway_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")

        if DATE_FILTER_AMOUNT > 1:
            self.date_filter_eof_exchange.send(DateFilterMessageHandler.serialize_eof_message(client_id))
            logging.info(f"Sent EOF for client {client_id} to other date filters")
        
        self._finalize_client(client_id)
    

    def process_eof_control_message(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                logging.info(f"Received EOF_GENERIC_MESSAGE for client {message.source_client_uuid}")
                self._mark_client_finalized(message.source_client_uuid)
        ack()

    def _mark_client_finalized(self, client_id):
        with self._finalized_clients_lock:
            if client_id in self._finalized_clients:
                return False
            self._finalized_clients.add(client_id)
            return True

    def _finalize_client(self, client_id):
        logging.info(f"Finalizing client {client_id}")
        if not self._mark_client_finalized(client_id):
            return
        
    def _check_if_eof_received(self, client_id):
        with self._finalized_clients_lock:
            if client_id in self._finalized_clients:
                logging.info(f"EOF already received for client {client_id}")
                self._finalize_client(client_id)

    def stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        consumers = [self.gateway_queue]
        if self.date_filter_eof_exchange is not None:
            consumers.append(self.date_filter_eof_exchange)

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.gateway_queue]
        if self.date_filter_eof_exchange is not None:
            resources.append(self.date_filter_eof_exchange)
        if self.usd_filters_q3_queue is not None:
            resources.append(self.usd_filters_q3_queue)
        if self.usd_filters_q4_queue is not None:
            resources.append(self.usd_filters_q4_queue)
        if self.pay_format_filter_queue is not None:
            resources.append(self.pay_format_filter_queue)

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

        gateway_thread = threading.Thread(
        target=self._run_gateway_consumer,
        name="gateway-consumer-thread",
        )


        if DATE_FILTER_AMOUNT > 1:
            control_thread = threading.Thread(
                target=self._run_control_consumer,
                name="date-control-consumer-thread",
            )

        gateway_started = False
        control_started = False

        try:
            gateway_thread.start()
            gateway_started = True
            if DATE_FILTER_AMOUNT > 1:
                control_thread.start()
                control_started = True

        except Exception as e:
            logging.error(e)
            self.stop()
            self._close_resources()
            return 2

        if gateway_started:
            gateway_thread.join()
        if control_started:
            control_thread.join()

        self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return 1

        return 0


def main():
    logging.basicConfig(level=logging.INFO)
    date_filter = DateFilter()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in date filter")
        date_filter.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return date_filter.start()


if __name__ == "__main__":
    main()
