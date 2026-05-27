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
SCATHER_GATHER_AGGREGATOR_AMOUNT = int(os.environ["SCATHER_GATHER_AGGREGATOR_AMOUNT"])
SCATHER_GATHER_PAIR_JOINER_PREFIX = os.environ["SCATHER_GATHER_PAIR_JOINER_PREFIX"]

SCATHER_GATHER_JOINER_AMOUNT = int(os.environ["SCATHER_GATHER_JOINER_AMOUNT"])
SCATHER_GATHER_JOINER_PREFIX = os.environ["SCATHER_GATHER_JOINER_PREFIX"]
FANIN_FANOUT_THRESHOLD = 5

class ScatherGatherPairJoiner:

    def __init__(self):
        self.scather_gather_pair_joiner_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SCATHER_GATHER_PAIR_JOINER_PREFIX, [f"{SCATHER_GATHER_PAIR_JOINER_PREFIX}_{ID}"]
        )
        
        self.id = int(ID)

        # definicion de exchanges para enviar a los agregadores
        self.scather_gather_joiner_exchanges = []
        self.scather_gather_joiner_producer_lock = threading.Lock()

        for i in range(SCATHER_GATHER_JOINER_AMOUNT):
            scather_gather_joiner_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SCATHER_GATHER_JOINER_PREFIX, [f"{SCATHER_GATHER_JOINER_PREFIX}_{i}"]
            )
            self.scather_gather_joiner_exchanges.append(scather_gather_joiner_exchange)



        self.dicts_lock = threading.Lock()
        self.middle_accounts_by_client : dict[str, dict[str, (set[str],set[str])]] = {} 
        #(set[str],set[str]) donde el primer elemento es origen y el segundo es destino

        self.eof_count_by_client = {}

        #Control de shutdown y estado de clientes
        self._sigterm_received = False
        self._runtime_error = False
        self._finalized_clients = set()
        self._finalized_clients_lock = threading.Lock()
        self._stop_lock = threading.Lock()
        self._stopping = False

    
    def _run_input_exchange_consumer(self):
        try:
            self.scather_gather_pair_joiner_input_exchange.start_consuming(self.process_scather_gather_mapper_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Scather Gather pair joiner consumer crashed")
    
    def process_scather_gather_mapper_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.SCATHER_GATHER_PAIR_JOINER_TO_SCATHER_GATHER_JOINER:
                client_id = message.source_client_uuid
                self._process_transaction(message.data, client_id, message.data_id)
            case message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                client_id = message.source_client_uuid
                self._process_scather_gather_mapper_eofs(client_id)
        ack()
        

    def _process_transaction(self, transaction_data, client_id, data_id):
        type = transaction_data.get("type")
        key = transaction_data.get("key")
        value = transaction_data.get("value")
        if type == "FANIN_MIDDLE":
            logging.info(f"Received FANIN MIDDLE message for client {client_id}")
            self._process_fanin_transaction_in_middle_structure(client_id, key, value)
        elif type == "FANOUT_MIDDLE":
            logging.info(f"Received FANOUT MIDDLE message for client {client_id}")
            self._process_fanout_transaction_in_middle_structure(client_id, key, value)
        else:
            logging.warning(f"Received unknown transaction type {type} for client {client_id}")


    def _process_fanin_transaction_in_middle_structure(self, client_id, destination, middle_account):
        with self.dicts_lock:
            self.middle_accounts_by_client.setdefault(client_id, {}).setdefault(middle_account, (set(), set()))[1].add(destination)

    def _process_fanout_transaction_in_middle_structure(self, client_id, origin, middle_account):
        with self.dicts_lock:
            self.middle_accounts_by_client.setdefault(client_id, {}).setdefault(middle_account, (set(), set()))[0].add(origin)

    def _send_eof_to_joiners(self, client_id):
        eof_message = ScatherGatherMessageHandler.serialize_eof_message(client_id)
        for exchange in self.scather_gather_joiner_exchanges:
            with self.scather_gather_joiner_producer_lock:
                exchange.send(eof_message)
        logging.info(f"Sent final EOFs for client {client_id} to scather gather joiners")

    def _send_data_to_joiners(self, client_id):
        with self.dicts_lock:
            middle_data = { #Descarta si origen o destino son vacíos
                middle_account: (origenes, destinos)
                for middle_account, (origenes, destinos) in self.middle_accounts_by_client.get(client_id, {}).items()
                if origenes and destinos
            }
        
        for middle_account, (origenes, destinos) in middle_data.items():
            for origen in origenes:
                for destino in destinos:
                    msg = ScatherGatherMessageHandler.serialize_scather_gather_pair_middle_message(client_id, origen, destino, middle_account)
                    joiner_index = self._worker_to_send_data_to_joiners(origen, destino)
                    with self.scather_gather_joiner_producer_lock:
                        self.scather_gather_joiner_exchanges[joiner_index].send(msg)

        logging.info(f"Sent all data for client {client_id} to scather gather joiners")
        

    def _worker_to_send_data_to_joiners(self, origen, destino):

        hashkey=("".join([origen, destino])).encode("utf-8")
        digest=hashlib.sha256(hashkey).digest()
        value = int.from_bytes(digest, byteorder="big")
        return value % SCATHER_GATHER_JOINER_AMOUNT

    def _process_scather_gather_mapper_eofs(self, client_id):
        logging.info(f"Received EOF for client {client_id}")
        self.eof_count_by_client[client_id] = self.eof_count_by_client.get(client_id, 0) + 1

        if self.eof_count_by_client[client_id] == SCATHER_GATHER_AGGREGATOR_AMOUNT:
            self._send_data_to_joiners(client_id)
            self._send_eof_to_joiners(client_id)
            self._finalize_client(client_id)

    
    def _finalize_client(self, client_id):
        with self._finalized_clients_lock:
            if client_id in self._finalized_clients:
                return
            logging.info(f"Finalizando cliente {client_id}")
            self._finalized_clients.add(client_id)

        with self.dicts_lock:
            if client_id in self.middle_accounts_by_client:
                del self.middle_accounts_by_client[client_id]
        
        if client_id in self.eof_count_by_client:
            del self.eof_count_by_client[client_id]

    def stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        consumers = [self.scather_gather_pair_joiner_input_exchange]

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.scather_gather_pair_joiner_input_exchange]

        resources.extend(self.scather_gather_joiner_exchanges)

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

        input_exchange_thread_started = False

        try:
            input_exchange_thread.start()
            input_exchange_thread_started = True

        except Exception as e:
            logging.error(e)
            self.stop()
            self._close_resources()
            return 2

        if input_exchange_thread_started:
            input_exchange_thread.join()

        self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return 1

        return 0


def main():
    logging.basicConfig(level=logging.INFO)
    scather_gather_aggregator = ScatherGatherPairJoiner()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in scather gather aggregator, stopping consumers...")
        scather_gather_aggregator.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return scather_gather_aggregator.start()


if __name__ == "__main__":
    main()
