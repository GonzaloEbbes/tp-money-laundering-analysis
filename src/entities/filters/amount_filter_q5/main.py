import hashlib
import os
import logging
import re
import signal
import threading
from time import sleep
import uuid

from common import middleware, message_protocol
from common.dedup import InMemoryDeduplicator, message_dedup_key
from common.logging.logging_config import configure_logging_from_env
from message_handler import MessageHandler as AmountFilterQ1MessageHandler

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
PAY_FORMAT_FILTER_AND_CURRENCY_CONVERTER_QUEUE = os.environ["INPUT_QUEUE"] #Es la propia, que conecta con ambos dos filtros
AMOUNT_FILTER_PREFIX = os.environ["AMOUNT_FILTER_PREFIX"]
AMOUNT_FILTER_AMOUNT = int(os.environ["AMOUNT_FILTER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]
EXPECTED_INPUT_EOFS = int(os.environ.get("EXPECTED_INPUT_EOFS", "2"))

OUTPUT_QUEUE = os.environ["GATEWAY_FINAL_QUERY_QUEUE"]


class AmountFilterQ1:

    def __init__(self):
        self.pay_format_filter_and_currency_converter_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, PAY_FORMAT_FILTER_AND_CURRENCY_CONVERTER_QUEUE
        )
        logging.info(
            "AmountFilterQ5 wiring: input_queue=%s output_queue=%s amount_filter_prefix=%s "
            "amount_filter_amount=%s expected_input_eofs=%s",
            PAY_FORMAT_FILTER_AND_CURRENCY_CONVERTER_QUEUE,
            OUTPUT_QUEUE,
            AMOUNT_FILTER_PREFIX,
            AMOUNT_FILTER_AMOUNT,
            EXPECTED_INPUT_EOFS,
        )
        
        self.id = int(ID)
        self.deduplicator = InMemoryDeduplicator()

        # definicion de working queue exchanges de la instancia posterior
        self.gateway_final_query_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE
            )

        #Exchange de control EOF
        self.amount_filter_eof_exchange_consumer = None
        self.amount_filter_eof_exchange_producer = None
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
            self._eof_producer_lock = threading.Lock()
            self.amount_filter_eof_exchange_producer = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST,
                    EOF_CONTROL_EXCHANGE,
                    amount_filters,
                )
            self._eof_count_by_client = {}
            self._eof_count_lock = threading.Lock()
            
        self.cant_trx_lock = threading.Lock()
        self.cant_trx_by_client = {}

        self._sigterm_received = False
        self._runtime_error = False

        if (self._is_leader()):
            self.total_eof_leader_received_by_client = {}
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
    
    def _run_pay_format_filter_and_currency_converter_consumer(self):
        try:
            logging.debug(
                "AmountFilterQ5 consuming combined Q5 queue=%s",
                PAY_FORMAT_FILTER_AND_CURRENCY_CONVERTER_QUEUE,
            )
            self.pay_format_filter_and_currency_converter_queue.start_consuming(self.process_pay_format_and_currency_converter_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Pay format filter and currency converter consumer crashed")

    
    def _run_control_consumer(self):
        try:
            self.amount_filter_eof_exchange_consumer.start_consuming(self.process_eof_control_message)
        except Exception as e:
            self._handle_runtime_failure(e, "Control consumer crashed")
    
    def process_pay_format_and_currency_converter_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.USD_CURRENCY_CONVERTER_TO_AMOUNT_FILTER_Q5:
                dedup_key = message_dedup_key(message)
                client_id = message.source_client_uuid
                if not self.deduplicator.should_process(client_id, dedup_key):
                    ack()
                    return
                self._add_inflight_message(message.source_client_uuid)
                self._process_usd_currency_converter_message(message.data, client_id, message.data_id)
                self._decrease_inflight_message(message.source_client_uuid)
                self._check_and_finalize_client_if_pending(client_id)
                self.deduplicator.mark_processed(client_id, dedup_key)
            case message_protocol.internal.InternalMessageType.PAY_FORMAT_FILTER_TO_AMOUNT_FILTER_Q5:
                dedup_key = message_dedup_key(message)
                client_id = message.source_client_uuid
                if not self.deduplicator.should_process(client_id, dedup_key):
                    ack()
                    return
                self._add_inflight_message(message.source_client_uuid)
                self._process_pay_format_message(message.data, client_id, message.data_id)
                self._decrease_inflight_message(message.source_client_uuid)
                self._check_and_finalize_client_if_pending(client_id)
                self.deduplicator.mark_processed(client_id, dedup_key)
            case message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                client_id = message.source_client_uuid
                self._process_input_queue_eof(client_id)
        ack()
        

    def _process_pay_format_message(self, transaction_data, client_id, data_id):
        amount_paid = float(transaction_data.get("amount_paid"))

        if amount_paid > 0 and amount_paid < 1:
            with self.cant_trx_lock:
                self.cant_trx_by_client[client_id] = self.cant_trx_by_client.get(client_id, 0) + 1
        

    def _process_usd_currency_converter_message(self, transaction_data, client_id, data_id): 
        amount_paid = float(transaction_data.get("amount_paid"))

        if amount_paid > 0 and amount_paid < 1:
            with self.cant_trx_lock:
                self.cant_trx_by_client[client_id] = self.cant_trx_by_client.get(client_id, 0) + 1

    def send_final_eof(self, client_id):
        data_id = f"{ID}:q5-result"
        with self.cant_trx_lock:
            cant_trx = self.cant_trx_by_client.get(client_id, 0)
            self.gateway_final_query_queue.send(
                AmountFilterQ1MessageHandler.serialize_gateway_query_message(
                    client_id,
                    data_id,
                    {"cantTrx": cant_trx},
                    message_id=data_id,
                )
            )
        logging.info("Q5 final result for client %s: cantTrx=%s", client_id, cant_trx)
        self.gateway_final_query_queue.send(AmountFilterQ1MessageHandler.serialize_eof_message(client_id))
        logging.info(f"Sent final EOF for client {client_id} to gateway final query queue")
    
    def _process_input_queue_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")
        self._register_eof_for_client(client_id)
        if AMOUNT_FILTER_AMOUNT > 1:
            with self._eof_producer_lock:
                self.amount_filter_eof_exchange_producer.send(AmountFilterQ1MessageHandler.serialize_eof_message(client_id))
            logging.info(f"Sent EOF for client {client_id} to other amount filters")
        self._try_finalize_client(client_id)
    
    def _check_and_finalize_client_if_pending(self, client_id):

        with self._is_pending_to_finalize_client_lock:
            is_pending = client_id in self._is_pending_to_finalize_client

        if is_pending:
            self._try_finalize_client(client_id)
                        
    
    def _finalize_client(self, client_id):

        with self._finalized_clients_lock:
            if client_id in self._finalized_clients:
                return
            logging.info(f"Finalizando cliente {client_id}")
            self._finalized_clients.add(client_id)

        if self._is_leader():
            self._leader_count_eof_for_client(client_id)
        else:
            self.send_eof_leader_message(client_id)

        with self._is_pending_to_finalize_client_lock:
            if client_id in self._is_pending_to_finalize_client:
                self._is_pending_to_finalize_client.remove(client_id)
        
    #Si tiene la cantidad de EOFs necesarios y no tiene infligth, finalizo. Si tiene inflight, lo marco como pendiente a finalizar y se finaliza cuando el inflight llegue a 0
    def _try_finalize_client(self, client_id):
        if not self._has_required_eofs_for_client(client_id):
            return

        with self._inflight_message_lock:
            has_inflight = self._inflight_messages.get(client_id, 0) > 0

        if has_inflight:
            with self._is_pending_to_finalize_client_lock:
                self._is_pending_to_finalize_client.add(client_id)
            logging.info(f"There are inflight messages for client {client_id}. Marking client as finalized but waiting for inflight messages to finish.")
            return
        logging.info(f"Required EOFs for client {client_id} and no inflight messages. Finalizing client.")
        self._finalize_client(client_id)

    def send_eof_leader_message(self, client_id):
        with self.cant_trx_lock:
            local_count = self.cant_trx_by_client.get(client_id, 0)
        data = { "cantTrx": local_count }
        
        with self._eof_producer_lock:
            self.amount_filter_eof_exchange_producer.send(AmountFilterQ1MessageHandler.serialize_eof_leader_message(client_id,data))
        logging.info(f"Sent EOF_LEADER_MESSAGE for client {client_id} to leader")

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
                logging.info(f"Received EOF_GENERIC_MESSAGE for client {message.source_client_uuid}")
                self._process_eof_from_control_exchange(message.source_client_uuid)
            case message_protocol.internal.InternalMessageType.EOF_LEADER_MESSAGE:
                if self._is_leader():
                    logging.info(f"Received EOF_LEADER_MESSAGE for client {message.source_client_uuid}")
                    self._leader_count_eof_for_client(message.source_client_uuid,message.data)
                
        ack()
    def _leader_count_eof_for_client(self, client_id, partial_data=None):
        should_send_final_eof = False

        if partial_data is not None: #Suma los datos parciales de las otras instancias
            partial_count = int(partial_data.get("cantTrx", 0))
            with self.cant_trx_lock:
                self.cant_trx_by_client[client_id] = self.cant_trx_by_client.get(client_id, 0) + partial_count

        with self._leader_eof_lock:
            self.total_eof_leader_received_by_client[client_id] = self.total_eof_leader_received_by_client.get(client_id, 0) + 1
            
            if self.total_eof_leader_received_by_client[client_id] == AMOUNT_FILTER_AMOUNT:
                logging.debug(f"Leader ha recibido EOF de todos los filtros para el cliente {client_id}. Enviando EOF a la capa siguiente.")
                should_send_final_eof = True
                del self.total_eof_leader_received_by_client[client_id]
        
        if should_send_final_eof:
            self.send_final_eof(client_id)

    def _process_eof_from_control_exchange(self, client_id):
        self._register_eof_for_client(client_id)
        self._try_finalize_client(client_id)

    def _register_eof_for_client(self, client_id):
        with self._eof_count_lock:
            count = self._eof_count_by_client.get(client_id, 0) + 1
            self._eof_count_by_client[client_id] = count

        logging.info(f"EOF count for client {client_id}: {count}/{EXPECTED_INPUT_EOFS}")
        return count >= EXPECTED_INPUT_EOFS


    def _has_required_eofs_for_client(self, client_id):
        with self._eof_count_lock:
            return self._eof_count_by_client.get(client_id, 0) >= EXPECTED_INPUT_EOFS

    def stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        consumers = [self.pay_format_filter_and_currency_converter_queue]
        if self.amount_filter_eof_exchange_consumer is not None:
            consumers.append(self.amount_filter_eof_exchange_consumer)

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.pay_format_filter_and_currency_converter_queue]
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
        if AMOUNT_FILTER_AMOUNT > 1:
            control_thread = threading.Thread(
                target=self._run_control_consumer,
                name="amount-control-consumer-thread",
            )

        control_started = False

        try:
            if AMOUNT_FILTER_AMOUNT > 1:
                control_thread.start()
                control_started = True
            self._run_pay_format_filter_and_currency_converter_consumer()

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
    amount_filter_q1 = AmountFilterQ1()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in amount filter q1")
        amount_filter_q1.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return amount_filter_q1.start()


if __name__ == "__main__":
    main()
