import hashlib
import os
import logging
import re
import signal
import threading
from time import sleep
import uuid

from common import middleware, message_protocol
from common.logging.logging_config import configure_logging_from_env
from message_handler import MessageHandler as AveragePerPayFormatJoinerMessageHandler

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"] #Es la que conecta con los mappers
JOINER_PREFIX = os.environ["JOINER_PREFIX"]
JOINER_AMOUNT = int(os.environ["JOINER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]
EXPECTED_INPUT_EOFS = int(os.environ.get("EXPECTED_INPUT_EOFS", 1))
OUTPUT_EXCHANGE = os.environ.get("AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE")

class AveragePerPayFormatJoiner:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        logging.info(
            "AveragePerPayFormatJoiner wiring: input_queue=%s output_queue=%s joiner_prefix=%s "
            "joiner_amount=%s expected_input_eofs=%s",
            INPUT_QUEUE,
            OUTPUT_EXCHANGE,
            JOINER_PREFIX,
            JOINER_AMOUNT,
            EXPECTED_INPUT_EOFS,
        )
        
        self.id = int(ID)

        # definicion de working queue exchanges de la instancia posterior
        self.output_exchange = middleware.MessageMiddlewareExchangePublisherRabbitMQ(
                MOM_HOST, OUTPUT_EXCHANGE
            ) 
        
        self.averages_by_client = {}
        self.averages_lock = threading.Lock()

        #Exchange de control EOF
        self._eof_exchange_consumer = None
        self._eof_exchange_producer = None
        if JOINER_AMOUNT > 1:
            joiners = []
            for i in range(JOINER_AMOUNT):
                if i != self.id:
                    joiners.append(f"{JOINER_PREFIX}_{i}")
        
            self._eof_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST,
                    EOF_CONTROL_EXCHANGE,
                    [f"{JOINER_PREFIX}_{self.id}"],
                )
            self._eof_producer_lock = threading.Lock()
            self._eof_exchange_producer = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST,
                    EOF_CONTROL_EXCHANGE,
                    joiners,
                )
        self._eof_count_by_client = {}
        self._eof_count_lock = threading.Lock()

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
    
    def _is_leader(self): #Para cuando sea amount  = 1 se pone SIEMPRE al 0 como líder
        return self.id == 0
    
    def _run_average_per_pay_format_mapper_consumer(self):
        try:
            logging.debug(
                "AveragePerPayFormatJoiner consuming average per pay format mapper messages=%s",
                INPUT_QUEUE,
            )
            self.input_queue.start_consuming(self.process_average_per_pay_format_mapper_messagges)
        except Exception as e:
            self._handle_runtime_failure(e, "Average per pay format mapper consumer crashed")

    
    def _run_control_consumer(self):
        try:
            self._eof_exchange_consumer.start_consuming(self.process_eof_control_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Control consumer crashed")
    
    def process_average_per_pay_format_mapper_messagges(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_MAPPER_TO_AVERAGE_PER_PAY_FORMAT_JOINER:
                self._add_inflight_message(message.source_client_uuid)
                client_id = message.source_client_uuid
                self._process_average_per_pay_format_mapper_message(message.data, client_id, message.data_id)
                self._decrease_inflight_message(message.source_client_uuid)
                self._check_and_finalize_client_if_pending(client_id)
            case message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                client_id = message.source_client_uuid
                self._process_input_queue_eof(client_id)
        ack()
        

    def _process_average_per_pay_format_mapper_message(self, transaction_data, client_id, data_id): 
        logging.debug("Received averages from mapper for client=%s", client_id)

        payment_format = transaction_data.get("PaymentFormat")
        if not payment_format:
            return None


        sum_total = float(transaction_data.get("sum_total", 0))
        count = int(transaction_data.get("count", 0))

        with self.averages_lock:
            client_averages = self.averages_by_client.setdefault(client_id, {})
            values = client_averages.setdefault(payment_format, {
                "sum_total": 0.0,
                "count": 0,
            })
            values["sum_total"] += sum_total
            values["count"] += count
    
    def _build_average_payload(self, client_id):
        result = {}
        with self.averages_lock:
            client_averages = self.averages_by_client.get(client_id, {})
        for payment_format, values in client_averages.items():
            count = values["count"]
            if count <= 0:
                continue
            sum_total = values["sum_total"]
            result[payment_format] = {
                "sum_total": sum_total,
                "count": count,
                "average": sum_total / count,
            }
        return result

    def send_final_eof(self, client_id):
        data_id = str(uuid.uuid4()) 
        averages = self._build_average_payload(client_id)
        self.output_exchange.send(AveragePerPayFormatJoinerMessageHandler.serialize_amount_filter_q3_exchange_message(averages, client_id, data_id), OUTPUT_EXCHANGE)
        self.output_exchange.send(AveragePerPayFormatJoinerMessageHandler.serialize_eof_message(client_id), OUTPUT_EXCHANGE)
        logging.info(f"Sent final EOF for client {client_id} to average per pay format joiner")
    
    def _process_input_queue_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")
        self._register_eof_for_client(client_id)
        if JOINER_AMOUNT > 1:
            with self._eof_producer_lock:
                self._eof_exchange_producer.send(AveragePerPayFormatJoinerMessageHandler.serialize_eof_message(client_id))
            logging.info(f"Sent EOF for client {client_id} to other average per pay format joiners")
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

        with self.averages_lock:
            self.averages_by_client.pop(client_id, None)

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
        with self._eof_producer_lock:
            self._eof_exchange_producer.send(AveragePerPayFormatJoinerMessageHandler.serialize_eof_leader_message(client_id))
        logging.info(f"Sent EOF_LEADER_MESSAGE for client {client_id} to leader")

    def _add_inflight_message(self, client_id):
        with self._inflight_message_lock:
            self._inflight_messages[client_id] = self._inflight_messages.get(client_id, 0) + 1
    
    def _decrease_inflight_message(self, client_id):
        with self._inflight_message_lock:
            if client_id in self._inflight_messages:
                self._inflight_messages[client_id] = self._inflight_messages.get(client_id, 0) - 1

    def process_eof_control_messages(self, message, ack, nack):
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
            
            if self.total_eof_leader_received_by_client[client_id] == JOINER_AMOUNT:
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

        consumers = [self.input_queue]
        if self._eof_exchange_consumer is not None:
            consumers.append(self._eof_exchange_consumer)

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.input_queue]
        if self._eof_exchange_consumer is not None:
            resources.append(self._eof_exchange_consumer)
        if self.output_exchange is not None:
            resources.append(self.output_exchange)
        if self._eof_exchange_producer is not None:
            resources.append(self._eof_exchange_producer)

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
        if JOINER_AMOUNT > 1:
            control_thread = threading.Thread(
                target=self._run_control_consumer,
                name="average-per-pay-format-control-consumer-thread",
            )

        control_started = False

        try:
            if JOINER_AMOUNT > 1:
                control_thread.start()
                control_started = True
            self._run_average_per_pay_format_mapper_consumer()

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
    average_per_pay_format_joiner = AveragePerPayFormatJoiner()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in average per pay format joiner")
        average_per_pay_format_joiner.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return average_per_pay_format_joiner.start()


if __name__ == "__main__":
    raise SystemExit(main())