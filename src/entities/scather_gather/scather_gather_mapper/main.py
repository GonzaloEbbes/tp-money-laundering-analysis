import hashlib
import os
import logging
import signal
import threading

from common import middleware, message_protocol
from common.logging.logging_config import configure_logging_from_env
from message_handler import MessageHandler as ScatherGatherMessageHandler

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
USD_FILTER_Q4_QUEUE = os.environ["INPUT_QUEUE"] #Es la propia, que conecta con el filtro USD q4
SCATHER_GATHER_MAPPER_PREFIX = os.environ["SCATHER_GATHER_MAPPER_PREFIX"]
SCATHER_GATHER_MAPPER_AMOUNT = int(os.environ["SCATHER_GATHER_MAPPER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]

SCATHER_GATHER_AGGREGATOR_AMOUNT = int(os.environ["SCATHER_GATHER_AGGREGATOR_AMOUNT"])
SCATHER_GATHER_AGGREGATOR_PREFIX = os.environ["SCATHER_GATHER_AGGREGATOR_PREFIX"]


class ScatherGatherMapper:

    def __init__(self):
        self.usd_filter_q4_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, USD_FILTER_Q4_QUEUE
        )
        
        self.id = int(ID)

        # definicion de exchanges para enviar a los agregadores
        self.scather_gather_aggregator_exchanges = []
        self.scather_gather_aggregator_producer_lock = threading.Lock()
        for i in range(SCATHER_GATHER_AGGREGATOR_AMOUNT):
            scather_gather_aggregator_exchanges = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SCATHER_GATHER_AGGREGATOR_PREFIX, [f"{SCATHER_GATHER_AGGREGATOR_PREFIX}_{i}"]
            )
            self.scather_gather_aggregator_exchanges.append(scather_gather_aggregator_exchanges)


        #Exchange de control EOF
        self.scather_gather_eof_exchange_consumer = None
        self.scather_gather_eof_exchange_producer = None
        if SCATHER_GATHER_MAPPER_AMOUNT > 1:
            scather_gather_mappers = []
            for i in range(SCATHER_GATHER_MAPPER_AMOUNT):
                if i != self.id:
                    scather_gather_mappers.append(f"{SCATHER_GATHER_MAPPER_PREFIX}_{i}")
        
            self.scather_gather_eof_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST,
                    EOF_CONTROL_EXCHANGE,
                    [f"{SCATHER_GATHER_MAPPER_PREFIX}_{self.id}"],
                )
            
            self._eof_producer_lock = threading.Lock()
            self.scather_gather_eof_exchange_producer = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST,
                    EOF_CONTROL_EXCHANGE,
                    scather_gather_mappers,
                )
            
        self.partial_dicts_lock = threading.Lock()
        self.partial_fanout_by_client : dict[str, dict[str, set[str]]] = {}
        self.partial_fanin_by_client : dict[str, dict[str, set[str]]] = {}


        #Control de shutdown y estado de clientes
        self._sigterm_received = False
        self._runtime_error = False
        self._is_pending_to_finalize_client = set()
        self._is_pending_to_finalize_client_lock = threading.Lock()
        self._finalized_clients = set()
        self._finalized_clients_lock = threading.Lock()
        self._inflight_messages = {}
        self._inflight_message_lock = threading.Lock()
        self._stop_lock = threading.Lock()
        self._stopping = False

    
    def _run_usd_filter_q4_consumer(self):
        try:
            self.usd_filter_q4_queue.start_consuming(self.process_usd_filter_q4_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "USD filter Q4 consumer crashed")

    
    def _run_control_consumer(self):
        try:
            self.scather_gather_eof_exchange_consumer.start_consuming(self.process_eof_control_message)
        except Exception as e:
            self._handle_runtime_failure(e, "Control consumer crashed")
    
    def process_usd_filter_q4_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.USD_FILTER_Q4_TO_SCATHER_GATHER_MAPPER:
                client_id = message.source_client_uuid
                self._add_inflight_message(message.source_client_uuid)
                self._process_transaction(message.data, client_id, message.data_id)
                self._decrease_inflight_message(message.source_client_uuid)
                self._check_and_finalize_client_if_pending(client_id)
            case message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                client_id = message.source_client_uuid
                self._process_usd_filter_q4_eof(client_id)
        ack()
        

    def _process_transaction(self, transaction_data, client_id, data_id):
        logging.debug(f"Received USD_FILTER_Q4_TO_SCATHER_GATHER_MAPPER for client {client_id}")
        origin = transaction_data.get("account_origin")
        destination = transaction_data.get("account_destination")

        with self.partial_dicts_lock:
            self.partial_fanin_by_client.setdefault(client_id, {}).setdefault(destination, set()).add(origin)
            self.partial_fanout_by_client.setdefault(client_id, {}).setdefault(origin, set()).add(destination)


    def _send_eof_to_aggregators(self, client_id):
        eof_message = ScatherGatherMessageHandler.serialize_eof_message(client_id)
        for exchange in self.scather_gather_aggregator_exchanges:
            with self.scather_gather_aggregator_producer_lock:
                exchange.send(eof_message)
        logging.info(f"Sent final EOFs for client {client_id} to scather gather aggregators")

    def _send_data_to_aggregators(self, client_id):
        #extraigo los datos del cliente,
        with self.partial_dicts_lock:
            fanout_data = {
                origen: list(destinos)
                for origen, destinos in self.partial_fanout_by_client.get(client_id, {}).items()
            }
            fanin_data = {
                destino: list(origenes)
                for destino, origenes in self.partial_fanin_by_client.get(client_id, {}).items()
            }
        
        for (origen,destinos) in fanout_data.items():
            aggregation_worker = self._worker_to_send_data_to_aggregator(origen)
            self.scather_gather_aggregator_exchanges[aggregation_worker].send(ScatherGatherMessageHandler.serialize_scather_gather_mapper_message_fanout(client_id, origen, destinos))
            logging.debug(f"Sent fanout data for origin {origen} of client {client_id} to aggregator worker {aggregation_worker}")
        del fanout_data

        for (destino,origenes) in fanin_data.items():
            aggregation_worker = self._worker_to_send_data_to_aggregator(destino)
            self.scather_gather_aggregator_exchanges[aggregation_worker].send(ScatherGatherMessageHandler.serialize_scather_gather_mapper_message_fanin(client_id, destino, origenes))
            logging.debug(f"Sent fanin data for destination {destino} of client {client_id} to aggregator worker {aggregation_worker}")
        del fanin_data

    def _worker_to_send_data_to_aggregator(self, clave_fanin_fanout):
        key=(clave_fanin_fanout).encode("utf-8")
        digest=hashlib.sha256(key).digest()
        value = int.from_bytes(digest, byteorder="big")
        return value % SCATHER_GATHER_AGGREGATOR_AMOUNT
    
    def _process_usd_filter_q4_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")

        if SCATHER_GATHER_MAPPER_AMOUNT > 1:
            with self._eof_producer_lock:
                self.scather_gather_eof_exchange_producer.send(ScatherGatherMessageHandler.serialize_eof_message(client_id))
            logging.info(f"Sent EOF for client {client_id} to other scather gather mappers")

        self._send_data_to_aggregators(client_id)
        self._send_eof_to_aggregators(client_id)
        self._finalize_client(client_id)
    
    def _check_and_finalize_client_if_pending(self, client_id):
        should_finalize = False

        with self._is_pending_to_finalize_client_lock:
            is_pending = client_id in self._is_pending_to_finalize_client

        if is_pending:
            with self._inflight_message_lock:
                should_finalize = self._inflight_messages.get(client_id, 0) == 0

        if should_finalize:
            logging.info(f"Finalizando cliente {client_id} que estaba pendiente")
            
            self._send_data_to_aggregators(client_id)
            self._send_eof_to_aggregators(client_id)
            self._finalize_client(client_id)
                        
    def _finalize_client(self, client_id):
        with self._finalized_clients_lock:
            if client_id in self._finalized_clients:
                return
            logging.info(f"Finalizando cliente {client_id}")
            self._finalized_clients.add(client_id)

        with self.partial_dicts_lock:
            if client_id in self.partial_fanout_by_client:
                del self.partial_fanout_by_client[client_id]
            if client_id in self.partial_fanin_by_client:
                del self.partial_fanin_by_client[client_id]


        with self._is_pending_to_finalize_client_lock:
            if client_id in self._is_pending_to_finalize_client:
                self._is_pending_to_finalize_client.remove(client_id)

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
                
        ack()

    def _process_eof_from_control_exchange(self, client_id):
        with self._inflight_message_lock:
            if self._inflight_messages.get(client_id, 0) > 0:
                logging.info(f"EOF received for client {client_id} but there are still inflight messages. Marking client as finalized but waiting for inflight messages to finish.")
                with self._is_pending_to_finalize_client_lock:
                    self._is_pending_to_finalize_client.add(client_id)
            else:
                logging.info(f"EOF received for client {client_id} and no inflight messages. Finalizing client.")
                self._send_data_to_aggregators(client_id)
                self._send_eof_to_aggregators(client_id)
                self._finalize_client(client_id)

    def stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        consumers = [self.usd_filter_q4_queue]
        if self.scather_gather_eof_exchange_consumer is not None:
            consumers.append(self.scather_gather_eof_exchange_consumer)

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.usd_filter_q4_queue]

        resources.extend(self.scather_gather_aggregator_exchanges)

        if self.scather_gather_eof_exchange_consumer is not None:
            resources.append(self.scather_gather_eof_exchange_consumer)
        if self.scather_gather_eof_exchange_producer is not None:
            resources.append(self.scather_gather_eof_exchange_producer)

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

        usd_filter_q4_thread = threading.Thread(
        target=self._run_usd_filter_q4_consumer,
        name="usd-q4-consumer-thread",
        )


        if SCATHER_GATHER_MAPPER_AMOUNT > 1:
            control_thread = threading.Thread(
                target=self._run_control_consumer,
                name="scather-gather-control-consumer-thread",
            )

        usd_q4_filter_thread_started = False
        control_started = False

        try:
            usd_filter_q4_thread.start()
            usd_q4_filter_thread_started = True
            if SCATHER_GATHER_MAPPER_AMOUNT > 1:
                control_thread.start()
                control_started = True

        except Exception as e:
            logging.error(e)
            self.stop()
            self._close_resources()
            return 2

        if usd_q4_filter_thread_started:
            usd_filter_q4_thread.join()
        if control_started:
            control_thread.join()

        self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return 1

        return 0


def main():
    configure_logging_from_env()
    scather_gather_mapper = ScatherGatherMapper()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in scather gather mapper, stopping consumers...")
        scather_gather_mapper.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return scather_gather_mapper.start()


if __name__ == "__main__":
    main()
