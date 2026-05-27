import hashlib
import os
import logging
import signal
import threading

from common import middleware, message_protocol
from message_handler import MessageHandler as ScatherGatherMessageHandler

logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s.%(msecs)03d - %(message)s',
            datefmt='%H:%M:%S'
        )

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
SCATHER_GATHER_AGG_AMOUNT = int(os.environ["SCATHER_GATHER_AGG_AMOUNT"])
SCATHER_GATHER_JOIN_PREFIX = os.environ["SCATHER_GATHER_JOIN_PREFIX"]
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]

SCATHER_GATHER_JOINER_AMOUNT = int(os.environ["SCATHER_GATHER_JOINER_AMOUNT"])
SCATHER_GATHER_JOINER_PREFIX = os.environ["SCATHER_GATHER_JOINER_PREFIX"]


OUTPUT_QUEUE = os.environ["GATEWAY_FINAL_QUERY_QUEUE"]
FANIN_FANOUT_THRESHOLD = 5

class ScatherGatherJoiner:

    def __init__(self):
        self.scather_gather_join_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SCATHER_GATHER_JOIN_PREFIX, [f"{SCATHER_GATHER_JOIN_PREFIX}_{ID}"]
        )
        
        self.id = int(ID)

        # definicion de exchanges para enviar a los agregadores
        self.gateway_final_query_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        self.gateway_final_query_queue_producer_lock = threading.Lock()


        self.dicts_lock = threading.Lock()
        self.fanout_by_client : dict[str, dict[tuple[str], set[str]]] = {}
        self.fanin_by_client : dict[str, dict[tuple[str], set[str]]] = {}

        self.eof_count_by_client : dict[str, int] = {}
        self._eof_count_lock = threading.Lock()

        #Exchange de control EOF
        self.scather_gather_eof_exchange_consumer = None
        self.scather_gather_eof_exchange_producer = None
        if SCATHER_GATHER_JOINER_AMOUNT > 1:
            scather_gather_joiners = []
            for i in range(SCATHER_GATHER_JOINER_AMOUNT):
                if i != self.id:
                    scather_gather_joiners.append(f"{SCATHER_GATHER_JOINER_PREFIX}_{i}")
        
            self.scather_gather_eof_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST,
                    EOF_CONTROL_EXCHANGE,
                    [f"{SCATHER_GATHER_JOINER_PREFIX}_{self.id}"],
                )
            
            self._eof_producer_lock = threading.Lock()
            self.scather_gather_eof_exchange_producer = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST,
                    EOF_CONTROL_EXCHANGE,
                    scather_gather_joiners,
                )
            
            if (self._is_leader()):
                self.total_eof_received_by_client = {}
                self._leader_eof_lock = threading.Lock()

        #Control de shutdown y estado de clientes
        self._sigterm_received = False
        self._runtime_error = False
        #self._is_pending_to_finalize_client = set()
        #self._is_pending_to_finalize_client_lock = threading.Lock()
        self._finalized_clients = set()
        self._finalized_clients_lock = threading.Lock()
        #self._inflight_messages = {}
        #self._inflight_message_lock = threading.Lock()
        self._stop_lock = threading.Lock()
        self._stopping = False

    def _is_leader(self):
        return self.id == 0
    
    def _run_input_exchange_consumer(self):
        try:
            self.scather_gather_join_input_exchange.start_consuming(self.process_scather_gather_agg_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Scather Gather joiner consumer crashed")
    
    def _run_control_consumer(self):
        try:
            self.scather_gather_eof_exchange_consumer.start_consuming(self.process_eof_control_message)
        except Exception as e:
            self._handle_runtime_failure(e, "Control consumer crashed")
    
    def process_scather_gather_agg_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.SCATHER_GATHER_AGGREGATOR_TO_SCATHER_GATHER_JOINER:
                client_id = message.source_client_uuid
                self._process_transaction(message.data, client_id, message.data_id)
            case message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                client_id = message.source_client_uuid
                self._process_scather_gather_agg_eofs(client_id)
        ack()
    

    def _process_transaction(self, transaction_data, client_id, data_id):
        type = transaction_data.get("type")
        key = transaction_data.get("key")
        value = transaction_data.get("value")
        if type == "FANIN":
            logging.info(f"Received FANIN message for client {client_id}")
            self._process_fanin_transaction(client_id, key, value)
        elif type == "FANOUT":
            logging.info(f"Received FANOUT message for client {client_id}")
            self._process_fanout_transaction(client_id, key, value)
        else:
            logging.warning(f"Received unknown transaction type {type} for client {client_id}")

    def process_eof_control_message(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.EOF_LEADER_MESSAGE:
                if self._is_leader():
                    logging.info(f"Received EOF_LEADER_MESSAGE for client {message.source_client_uuid}")
                    self._leader_count_eof_for_client(message.source_client_uuid)
                
        ack()

    def _process_fanin_transaction(self, client_id, destination, origins):
        with self.dicts_lock:
            #se transforma el str[] origins en tuple str para usarlos de clave
            self.fanin_by_client.setdefault(client_id, {}).setdefault(tuple(origins), set()).add(destination)
        pass

    def _process_fanout_transaction(self, client_id, origin, destinations):
        with self.dicts_lock:
            #se transforma el str[] destinations en tuple str para usarlos de clave
            self.fanout_by_client.setdefault(client_id, {}).setdefault(tuple(destinations), set()).add(origin)
        pass


    def _send_data_to_gateway(self, client_id):
        with self.dicts_lock:
            fanout_data = {
                destinos: origen
                for destinos, origen  in self.fanout_by_client.get(client_id, {}).items()
            }
            fanin_data = {
                origenes: destino
                for origenes, destino in self.fanin_by_client.get(client_id, {}).items()
            }

        for fanout_destinations, fanout_origin in fanout_data.items():
            
            if fanin_data.get(fanout_destinations) is None:
                logging.warning(f"No se encontraron datos de FANIN para las FANOUT destinations {fanout_destinations} con cuenta origen {fanout_origin} del cliente {client_id}.")
                continue

            scather_gather_flux = []
            scather_gather_flux.extend(fanout_origin)
            scather_gather_flux.extend(fanout_destinations)
            scather_gather_flux.extend(fanin_data[fanout_destinations])
            
            message = ScatherGatherMessageHandler._serialize_scather_gather_final_message(client_id, scather_gather_flux)
            with self.gateway_final_query_queue_producer_lock:
                self.gateway_final_query_queue.send(message)
        
        logging.info(f"Sent all final data for client {client_id} to gateway final query queue")

    def _process_scather_gather_agg_eofs(self, client_id):
        logging.info(f"Received EOF for client {client_id}")
        should_finalize = False
        with self._eof_count_lock:
            self.eof_count_by_client[client_id] = self.eof_count_by_client.get(client_id, 0) + 1
            if self.eof_count_by_client[client_id] == SCATHER_GATHER_AGG_AMOUNT:
                should_finalize = True
        
        if (should_finalize):
            self._send_data_to_gateway(client_id)
            self._finalize_client(client_id)

    def _leader_count_eof_for_client(self, client_id):
        should_send_final_eof = False
        with self._leader_eof_lock:
            self.total_eof_received_by_client[client_id] = self.total_eof_received_by_client.get(client_id, 0) + 1
            
            if self.total_eof_received_by_client[client_id] == SCATHER_GATHER_JOINER_AMOUNT:
                logging.info(f"Leader ha recibido EOF de todos los filtros para el cliente {client_id}. Enviando EOF a la capa siguiente.")
                should_send_final_eof = True
                del self.total_eof_received_by_client[client_id]
        
        if should_send_final_eof:
            self.send_final_eof(client_id)

    def send_final_eof(self, client_id):
        with self.gateway_final_query_queue_producer_lock:
            self.gateway_final_query_queue.send(ScatherGatherMessageHandler.serialize_eof_message(client_id))
        logging.info(f"Sent final EOF for client {client_id} to gateway final query queue")

    #tambien envía eof al líder
    def _finalize_client(self, client_id):
        with self._finalized_clients_lock:
            if client_id in self._finalized_clients:
                return
            logging.info(f"Finalizando cliente {client_id}")
            self._finalized_clients.add(client_id)

        if SCATHER_GATHER_JOINER_AMOUNT == 1:
            self.send_final_eof(client_id)
        elif self._is_leader():
            self._leader_count_eof_for_client(client_id)
        else:
            self.send_eof_leader_message(client_id)
        
        with self.dicts_lock:
            self.fanout_by_client.pop(client_id, None)
            self.fanin_by_client.pop(client_id, None)

        with self._eof_count_lock:
            self.eof_count_by_client.pop(client_id, None)

    def send_eof_leader_message(self, client_id):
        with self._eof_producer_lock:
            self.scather_gather_eof_exchange_producer.send(ScatherGatherMessageHandler.serialize_eof_leader_message(client_id))
        logging.info(f"Sent EOF_LEADER_MESSAGE for client {client_id} to leader")

    def stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        consumers = [self.scather_gather_join_input_exchange]

        if self.scather_gather_eof_exchange_consumer is not None:
            consumers.append(self.scather_gather_eof_exchange_consumer)

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [
            self.scather_gather_join_input_exchange,
            self.gateway_final_query_queue,
        ]
        
        if SCATHER_GATHER_JOINER_AMOUNT > 1:
            resources.append(self.scather_gather_eof_exchange_consumer)
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

        input_exchange_thread = threading.Thread(
        target=self._run_input_exchange_consumer,
        name="input-exchange-consumer-thread",
        )

        if SCATHER_GATHER_JOINER_AMOUNT > 1:
            control_thread = threading.Thread(
                target=self._run_control_consumer,
                name="scather-gather-control-consumer-thread",
            )

        input_exchange_thread_started = False
        control_started = False

        try:
            input_exchange_thread.start()
            input_exchange_thread_started = True
            if SCATHER_GATHER_JOINER_AMOUNT > 1:
                control_thread.start()
                control_started = True

        except Exception as e:
            logging.error(e)
            self.stop()
            self._close_resources()
            return 2

        if input_exchange_thread_started:
            input_exchange_thread.join()

        if control_started:
            control_thread.join()

        self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return 1

        return 0


def main():
    logging.basicConfig(level=logging.INFO)
    scather_gather_aggregator = ScatherGatherJoiner()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in scather gather joiner, stopping consumers...")
        scather_gather_aggregator.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return scather_gather_aggregator.start()


if __name__ == "__main__":
    main()
