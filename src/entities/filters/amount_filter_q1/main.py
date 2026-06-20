import os
import logging
import signal
import threading
from time import sleep

from common import middleware, message_protocol
from common.dedup import InMemoryDeduplicator, message_dedup_key
from message_handler import MessageHandler as AmountFilterQ1MessageHandler
from common.logging import configure_logging_from_env


ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
USD_FILTER_Q1Q2_QUEUE = os.environ["INPUT_QUEUE"] #Es la propia, que conecta con el filtro USD q1q2
AMOUNT_FILTER_PREFIX = os.environ["AMOUNT_FILTER_PREFIX"]
AMOUNT_FILTER_AMOUNT = int(os.environ["AMOUNT_FILTER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]

OUTPUT_QUEUE = os.environ["GATEWAY_FINAL_QUERY_QUEUE"]


class AmountFilterQ1:

    def __init__(self):
        self.usd_filter_q1q2_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, USD_FILTER_Q1Q2_QUEUE
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
    
    def _run_usd_filter_q1q2_consumer(self):
        try:
            self.usd_filter_q1q2_queue.start_consuming(self.process_usd_filter_q1q2_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "USD filter Q1Q2 consumer crashed")

    
    def _run_control_consumer(self):
        try:
            self.amount_filter_eof_exchange_consumer.start_consuming(self.process_eof_control_message)
        except Exception as e:
            self._handle_runtime_failure(e, "Control consumer crashed")
    
    def process_usd_filter_q1q2_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.USD_FILTER_Q1Q2_TO_AMOUNT_FILTER_Q1:
                client_id = message.source_client_uuid
                dedup_key = message_dedup_key(message)

                if self.deduplicator.should_process(client_id, dedup_key):
                    self._add_inflight_message(message.source_client_uuid)
                    self._process_transaction(message.data, client_id, message.data_id, message.message_id)
                    self._decrease_inflight_message(message.source_client_uuid)
                    self._check_and_finalize_client_if_pending(client_id)
                    # TODO: Make send-to-next-queue, dedup mark, and RabbitMQ ack/nack atomic.
                    self.deduplicator.mark_processed(client_id, dedup_key)
            case message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                client_id = message.source_client_uuid
                self._process_usd_filter_q1q2_eof(client_id)
        ack()
        

    def _process_transaction(self, transaction_data, client_id, data_id, message_id=None):
        logging.debug(f"Received USD_FILTER_Q1Q2_TO_AMOUNT_FILTER_Q1 for client {client_id}")
        amount_received = float(transaction_data.get("amount_received"))

        if amount_received > 0 and amount_received < 50:
            self.gateway_final_query_queue.send(AmountFilterQ1MessageHandler.serialize_gateway_query_message(client_id, data_id, transaction_data, message_id=message_id))
            logging.debug(f"Transaction for client {client_id} sent to final gateway queue")
        

    def send_final_eof(self, client_id):
        self.gateway_final_query_queue.send(AmountFilterQ1MessageHandler.serialize_eof_message(client_id))
        logging.info(f"Sent final EOF for client {client_id} to gateway final query queue")
    
    def _process_usd_filter_q1q2_eof(self, client_id):
        logging.debug(f"Received EOF for client {client_id}")

        if AMOUNT_FILTER_AMOUNT > 1:
            with self._eof_producer_lock:
                self.amount_filter_eof_exchange_producer.send(AmountFilterQ1MessageHandler.serialize_eof_message(client_id))
            logging.debug(f"Sent EOF for client {client_id} to other amount filters")

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
            self.amount_filter_eof_exchange_producer.send(AmountFilterQ1MessageHandler.serialize_eof_leader_message(client_id))
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
            
            if self.total_eof_received_by_client[client_id] == AMOUNT_FILTER_AMOUNT:
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

        consumers = [self.usd_filter_q1q2_queue]
        if self.amount_filter_eof_exchange_consumer is not None:
            consumers.append(self.amount_filter_eof_exchange_consumer)

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.usd_filter_q1q2_queue]
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

        usd_filter_q1q2_thread = threading.Thread(
        target=self._run_usd_filter_q1q2_consumer,
        name="usd-q1q2-consumer-thread",
        )


        if AMOUNT_FILTER_AMOUNT > 1:
            control_thread = threading.Thread(
                target=self._run_control_consumer,
                name="amount-control-consumer-thread",
            )

        usd_q1q2_filter_thread_started = False
        control_started = False

        try:
            usd_filter_q1q2_thread.start()
            usd_q1q2_filter_thread_started = True
            if AMOUNT_FILTER_AMOUNT > 1:
                control_thread.start()
                control_started = True

        except Exception as e:
            logging.error(e)
            self.stop()
            self._close_resources()
            return 2

        if usd_q1q2_filter_thread_started:
            usd_filter_q1q2_thread.join()
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
