import hashlib
import os
import logging
import re
import signal
import threading
from time import sleep

from common import middleware, message_protocol
from common.logging.logging_config import configure_logging_from_env
from message_handler import MessageHandler as USDFilterMessageHandler


ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
USD_FILTER_PREFIX = os.environ["USD_FILTER_PREFIX"]
USD_FILTER_AMOUNT = int(os.environ["USD_FILTER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]

AMOUNT_FILTER_Q1_QUEUE = os.environ["AMOUNT_FILTER_Q1_QUEUE"]
DATA_PER_BANK_SHUFFLER_QUEUE = os.environ["DATA_PER_BANK_SHUFFLER_QUEUE"]



class USDFilterQ1Q2:

    def __init__(self):
        self.gateway_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        
        self.id = int(ID)

        # definicion de working queue exchanges de la instancia posterior
        self.amount_filter_q1_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, AMOUNT_FILTER_Q1_QUEUE
            )

        self.data_per_bank_shuffler_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, DATA_PER_BANK_SHUFFLER_QUEUE
            )

        #Exchange de control EOF
        self.usd_filter_eof_exchange_consumer = None
        self.usd_filter_eof_exchange_producer = None
        if USD_FILTER_AMOUNT > 1:
            usd_filters = []
            for i in range(USD_FILTER_AMOUNT):
                if i != self.id:
                    usd_filters.append(f"{USD_FILTER_PREFIX}_{i}")
        
            self.usd_filter_eof_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST,
                    EOF_CONTROL_EXCHANGE,
                    [f"{USD_FILTER_PREFIX}_{self.id}"],
                )
            
            self._eof_producer_lock = threading.Lock()
            self.usd_filter_eof_exchange_producer = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST,
                    EOF_CONTROL_EXCHANGE,
                    usd_filters,
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
    
    def _run_gateway_consumer(self):
        try:
            self.gateway_queue.start_consuming(self.process_gateway_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Gateway consumer crashed")

    
    def _run_control_consumer(self):
        try:
            self.usd_filter_eof_exchange_consumer.start_consuming(self.process_eof_control_message)
        except Exception as e:
            self._handle_runtime_failure(e, "Control consumer crashed")
    
    def process_gateway_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.GATEWAY_TO_USD_FILTER_Q1Q2:
                self._add_inflight_message(message.source_client_uuid)
                client_id = message.source_client_uuid
                self._process_transaction(message.data, client_id, message.data_id)
                self._decrease_inflight_message(message.source_client_uuid)
                self._check_and_finalize_client_if_pending(client_id)
            case message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                client_id = message.source_client_uuid
                self._process_gateway_eof(client_id)
        ack()
        

    def _process_transaction(self, transaction_data, client_id, data_id):
        logging.debug(f"Received GATEWAY_TO_USD_FILTER_Q1Q2 for client {client_id}")
        payment_currency = transaction_data.get("payment_currency")
        receiving_currency = transaction_data.get("receiving_currency")
        
        if payment_currency == "US Dollar" and receiving_currency == "US Dollar":
            self.amount_filter_q1_queue.send(USDFilterMessageHandler.serialize_amount_filter_q1_message(client_id, data_id, transaction_data))
            self.data_per_bank_shuffler_queue.send(USDFilterMessageHandler.serialize_data_per_bank_shuffler_message(client_id, data_id, transaction_data))
            logging.debug(f"Transaction for client {client_id} sent to amount filter and data per bank shuffler")
        

    def send_final_eof(self, client_id):
        self.amount_filter_q1_queue.send(USDFilterMessageHandler.serialize_eof_message(client_id))
        self.data_per_bank_shuffler_queue.send(USDFilterMessageHandler.serialize_eof_message(client_id))
        logging.debug(f"Sent final EOF for client {client_id} to all downstream queues")
    
    def _process_gateway_eof(self, client_id):
        logging.debug(f"Received EOF for client {client_id}")

        if USD_FILTER_AMOUNT > 1:
            with self._eof_producer_lock:
                self.usd_filter_eof_exchange_producer.send(USDFilterMessageHandler.serialize_eof_message(client_id))
                logging.debug(f"Sent EOF for client {client_id} to other usd filters")

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
            self.usd_filter_eof_exchange_producer.send(USDFilterMessageHandler.serialize_eof_leader_message(client_id))
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
            
            if self.total_eof_received_by_client[client_id] == USD_FILTER_AMOUNT:
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

        consumers = [self.gateway_queue]
        if self.usd_filter_eof_exchange_consumer is not None:
            consumers.append(self.usd_filter_eof_exchange_consumer)

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.gateway_queue]
        if self.usd_filter_eof_exchange_consumer is not None:
            resources.append(self.usd_filter_eof_exchange_consumer)
        if self.amount_filter_q1_queue is not None:
            resources.append(self.amount_filter_q1_queue)
        if self.data_per_bank_shuffler_queue is not None:
            resources.append(self.data_per_bank_shuffler_queue)
        if self.usd_filter_eof_exchange_producer is not None:
            resources.append(self.usd_filter_eof_exchange_producer)

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


        if USD_FILTER_AMOUNT > 1:
            control_thread = threading.Thread(
                target=self._run_control_consumer,
                name="usd-control-consumer-thread",
            )

        gateway_started = False
        control_started = False

        try:
            gateway_thread.start()
            gateway_started = True
            if USD_FILTER_AMOUNT > 1:
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
    configure_logging_from_env()
    usd_filter_q1q2 = USDFilterQ1Q2()

    def _handle_sigterm(signum, frame):
        logging.debug("SIGTERM received in usd filter q1q2")
        usd_filter_q1q2.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return usd_filter_q1q2.start()


if __name__ == "__main__":
    main()
