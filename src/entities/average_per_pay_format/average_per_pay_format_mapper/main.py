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
from common.message_protocol.internal import TransactionData
from message_handler import MessageHandler as AveragePerPayFormatMapperMessageHandler

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
USD_FILTER_Q4_TO_AVERAGE_PER_PAY_FORMAT_MAPPER_QUEUE = os.environ["INPUT_QUEUE"] #Es la propia, que conecta con ambos dos filtros
MAPPER_FILTER_PREFIX = os.environ["MAPPER_FILTER_PREFIX"]
MAPPER_FILTER_AMOUNT = int(os.environ["MAPPER_FILTER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]
EXPECTED_INPUT_EOFS = int(os.environ.get("EXPECTED_INPUT_EOFS", "1"))

OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"] #average_per_pay_format_mapper_to_average_per_pay_format_aggregator_queue


class AveragePerPayFormatMapper:

    def __init__(self):
        self.usd_filter_q4_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, USD_FILTER_Q4_TO_AVERAGE_PER_PAY_FORMAT_MAPPER_QUEUE
        )
        
        self.id = int(ID)
        self.deduplicator = InMemoryDeduplicator()

        # definicion de working queue exchanges de la instancia posterior
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE
            )

        #Exchange de control EOF
        self.average_mapper_eof_exchange_consumer = None
        self.average_mapper_eof_exchange_producer = None

        self._eof_producer_lock = threading.Lock()
        if MAPPER_FILTER_AMOUNT > 1:
            avg_mappers = []
            for i in range(MAPPER_FILTER_AMOUNT):
                if i != self.id:
                    avg_mappers.append(f"{MAPPER_FILTER_PREFIX}_{i}")
        
            self.average_mapper_eof_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST,
                    EOF_CONTROL_EXCHANGE,
                    [f"{MAPPER_FILTER_PREFIX}_{self.id}"],
                )
            
            self.average_mapper_eof_exchange_producer = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST,
                    EOF_CONTROL_EXCHANGE,
                    avg_mappers,
                )
        self._eof_count_by_client = {}
        self._eof_count_lock = threading.Lock()

        self._sigterm_received = False
        self._runtime_error = False

        if (self._is_leader()):
            self.total_eof_leader_received_by_client = {}
            self._leader_eof_lock = threading.Lock()

        self.averages_per_client : dict[str, dict[str, dict[str,float]]] = {}
        self.averages_per_client_lock = threading.Lock()

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
    
    def _run_usd_filter_q4_consumer(self):
        try:
            logging.debug(
                "AveragePerPayFormatMapper consuming combined Q5 queue=%s",
                USD_FILTER_Q4_TO_AVERAGE_PER_PAY_FORMAT_MAPPER_QUEUE,
            )
            self.usd_filter_q4_queue.start_consuming(self.process_usd_filter_q4_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "usd filter q4 consumer crashed")

    
    def _run_control_consumer(self):
        try:
            self.average_mapper_eof_exchange_consumer.start_consuming(self.process_eof_control_message)
        except Exception as e:
            self._handle_runtime_failure(e, "Control consumer crashed")
    
    def process_usd_filter_q4_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.USD_FILTER_Q4_TO_AVERAGE_PER_PAY_FORMAT_MAPPER:
                dedup_key = message_dedup_key(message)
                client_id = message.source_client_uuid
                if not self.deduplicator.should_process(client_id, dedup_key):
                    ack()
                    return
                self._add_inflight_message(message.source_client_uuid)
                self._process_usd_filter_q4_message(message.data, client_id, message.data_id)
                self._decrease_inflight_message(message.source_client_uuid)
                self._check_and_finalize_client_if_pending(client_id)
                self.deduplicator.mark_processed(client_id, dedup_key)
            case message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                client_id = message.source_client_uuid
                self._process_input_queue_eof(client_id)
        ack()
        
    def _process_usd_filter_q4_message(self, transaction_data, client_id, data_id): 
        payment_format = transaction_data.get("payment_format")
        if not client_id or not payment_format or client_id in self._finalized_clients:
            return

        try:
            amount = float(transaction_data.get("amount_received", 0))
        except (TypeError, ValueError):
            return
        
        with self.averages_per_client_lock:
            if client_id not in self.averages_per_client:
                self.averages_per_client[client_id] = {}
            if payment_format not in self.averages_per_client[client_id]:
                self.averages_per_client[client_id][payment_format] = {"sum_total": 0, "count": 0}
            self.averages_per_client[client_id][payment_format]["sum_total"] += amount
            self.averages_per_client[client_id][payment_format]["count"] += 1

    def send_eof_final_message(self, client_id):
        if (self._is_leader()):
            with self._eof_producer_lock:
                self.output_queue.send(AveragePerPayFormatMapperMessageHandler.serialize_eof_message(client_id))
            logging.info(f"Sent final EOF for client {client_id} to average per pay format joiner")

    def _send_data_to_joiner(self, client_id, data_id):
        with self.averages_per_client_lock:
            averages_in_client = self.averages_per_client.get(client_id, {})
        for payment_format, values in averages_in_client.items():
            
            with self._eof_producer_lock:
                self.output_queue.send(
                    AveragePerPayFormatMapperMessageHandler.serialize_average_per_pay_joiner_message(client_id, data_id, payment_format, values)
                )
    def _process_input_queue_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")
        self._register_eof_for_client(client_id)
        if MAPPER_FILTER_AMOUNT > 1:
            with self._eof_producer_lock:
                self.average_mapper_eof_exchange_producer.send(AveragePerPayFormatMapperMessageHandler.serialize_eof_message(client_id))
            logging.info(f"Sent EOF for client {client_id} to other average mappers")
        self._try_finalize_client(client_id)
    
    def _check_and_finalize_client_if_pending(self, client_id):

        with self._is_pending_to_finalize_client_lock:
            is_pending = client_id in self._is_pending_to_finalize_client

        if is_pending:
            self._try_finalize_client(client_id)       
    
    def _finalize_client(self, client_id):
        
        data_id = str(uuid.uuid4())
        self._send_data_to_joiner(client_id, data_id) 

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

        with self.averages_per_client_lock:
            self.averages_per_client.pop(client_id, None)
        
        
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
        with self._eof_producer_lock:
            self.average_mapper_eof_exchange_producer.send(AveragePerPayFormatMapperMessageHandler.serialize_eof_leader_message(client_id))
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

        with self._leader_eof_lock:
            self.total_eof_leader_received_by_client[client_id] = self.total_eof_leader_received_by_client.get(client_id, 0) + 1
            
            if self.total_eof_leader_received_by_client[client_id] == MAPPER_FILTER_AMOUNT:
                logging.debug(f"Leader ha recibido EOF de todos los filtros para el cliente {client_id}. Enviando EOF a la capa siguiente.")
                should_send_final_eof = True
                del self.total_eof_leader_received_by_client[client_id]
        
        if should_send_final_eof:
            self.send_eof_final_message(client_id)

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

        consumers = [self.usd_filter_q4_queue]
        if self.average_mapper_eof_exchange_consumer is not None:
            consumers.append(self.average_mapper_eof_exchange_consumer)

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.usd_filter_q4_queue]
        if self.average_mapper_eof_exchange_consumer is not None:
            resources.append(self.average_mapper_eof_exchange_consumer)
        if self.output_queue is not None:
            resources.append(self.output_queue)
        if self.average_mapper_eof_exchange_producer is not None:
            resources.append(self.average_mapper_eof_exchange_producer)

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
        if MAPPER_FILTER_AMOUNT > 1:
            control_thread = threading.Thread(
                target=self._run_control_consumer,
                name="amount-control-consumer-thread",
            )

        control_started = False

        try:
            if MAPPER_FILTER_AMOUNT > 1:
                control_thread.start()
                control_started = True
            self._run_usd_filter_q4_consumer()

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
    average_per_pay_format_mapper = AveragePerPayFormatMapper()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in average per pay format mapper")
        average_per_pay_format_mapper.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return average_per_pay_format_mapper.start()


if __name__ == "__main__":
    main()
