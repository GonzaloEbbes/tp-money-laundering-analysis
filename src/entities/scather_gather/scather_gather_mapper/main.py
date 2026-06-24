import hashlib
import os
import logging
from random import randint
import signal
import sys
import threading

from common import middleware, message_protocol
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler
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
EXPECTED_INPUT_EOFS = int(os.environ["EXPECTED_INPUT_EOFS"]) #1
INPUT_PREFIX_1 = os.environ["INPUT_PREFIX_1"] #que es el prefix del usd filter q4
AUXILIARY_INPUT = os.environ["AUXILIARY_INPUT"] == "true" #false
OUTPUT_PREFIX_1 = os.environ["OUTPUT_PREFIX_1"] #al aggregator

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

        self.producer_lock = threading.Lock()

        self.partial_dicts_lock = threading.Lock()
        self.partial_fanout_by_client : dict[str, dict[str, set[str]]] = {}
        self.partial_fanin_by_client : dict[str, dict[str, set[str]]] = {}


        #Control de shutdown y estado de clientes
        self._sigterm_received = False
        self._runtime_error = False
        self._stop_lock = threading.Lock()
        self._stopping = False

        self.eof_controller = EOFController(MOM_HOST, self.id, SCATHER_GATHER_MAPPER_PREFIX, SCATHER_GATHER_MAPPER_AMOUNT, EOF_CONTROL_EXCHANGE, EXPECTED_INPUT_EOFS,self.on_consensus_ok_callback,self.on_send_eof_to_next_stage_callback, self.on_clean_client_callback, AUXILIARY_INPUT)


    def _run_usd_filter_q4_consumer(self):
        try:
            self.usd_filter_q4_queue.start_consuming(self.process_usd_filter_q4_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "USD filter Q4 consumer crashed")

    def process_usd_filter_q4_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.USD_FILTER_Q4_TO_SCATHER_GATHER_MAPPER:
                client_id = message.source_client_uuid
                self._process_transaction(message.data, client_id, message.data_id)
                self.eof_controller.on_processed_packet_by_client(client_id, INPUT_PREFIX_1)
            case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                client_id = message.source_client_uuid
                self.eof_controller.on_input_queue_eof_reception(client_id, message.data)
        ack()
        

    def _process_transaction(self, transaction_data, client_id, data_id):
        logging.debug(f"Received USD_FILTER_Q4_TO_SCATHER_GATHER_MAPPER for client {client_id}")
        origin = transaction_data.get("account_origin")
        destination = transaction_data.get("account_destination")

        with self.partial_dicts_lock:
            self.partial_fanin_by_client.setdefault(client_id, {}).setdefault(destination, set()).add(origin)
            self.partial_fanout_by_client.setdefault(client_id, {}).setdefault(origin, set()).add(destination)

    def on_send_eof_to_next_stage_callback(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        eof_message = EOFMessageHandler.serialize_eof_message(client_id, totals_by_output.get(OUTPUT_PREFIX_1, 0), origin_worker_prefix, amount_origin_workers)
        with self.producer_lock:
            #EL EOF SE ENVIA A UNA INSTANCIA ALEATORIA
            eof_random_instance = randint(0, SCATHER_GATHER_AGGREGATOR_AMOUNT - 1)
            self.scather_gather_aggregator_exchanges[eof_random_instance].send(eof_message)
        logging.info(f"Sent final EOF for client {client_id} to scather gather aggregators")

    def on_consensus_ok_callback(self, client_id):
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
            with self.producer_lock:
                self.scather_gather_aggregator_exchanges[aggregation_worker].send(ScatherGatherMessageHandler.serialize_scather_gather_mapper_message_fanout(client_id, origen, destinos))
                self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_1, client_id)
            logging.debug(f"Sent fanout data for origin {origen} of client {client_id} to aggregator worker {aggregation_worker}")
        del fanout_data

        for (destino,origenes) in fanin_data.items():
            aggregation_worker = self._worker_to_send_data_to_aggregator(destino)
            with self.producer_lock:
                self.scather_gather_aggregator_exchanges[aggregation_worker].send(ScatherGatherMessageHandler.serialize_scather_gather_mapper_message_fanin(client_id, destino, origenes))
                self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_1, client_id)
            logging.debug(f"Sent fanin data for destination {destino} of client {client_id} to aggregator worker {aggregation_worker}")
        del fanin_data

    def _worker_to_send_data_to_aggregator(self, clave_fanin_fanout):
        key=(clave_fanin_fanout).encode("utf-8")
        digest=hashlib.sha256(key).digest()
        value = int.from_bytes(digest, byteorder="big")
        return value % SCATHER_GATHER_AGGREGATOR_AMOUNT

    def on_clean_client_callback(self, client_id):

        with self.partial_dicts_lock:
            if client_id in self.partial_fanout_by_client:
                del self.partial_fanout_by_client[client_id]
            if client_id in self.partial_fanin_by_client:
                del self.partial_fanin_by_client[client_id]

    def stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        consumers = [self.usd_filter_q4_queue]

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.usd_filter_q4_queue]

        resources.extend(self.scather_gather_aggregator_exchanges)

        for resource in resources:
            try:
                resource.close()
            except Exception as e:
                logging.error(f"Error closing resource: {e}")

    def notify_sigterm(self):
        self._sigterm_received = True
        self.stop()
        self.eof_controller.on_sigterm()

    def _handle_runtime_failure(self, error, context):
        logging.error(f"{context}: {error}")
        self._runtime_error = True
        self.stop()
        self.eof_controller.on_stop()
    
    def start(self):

        process_thread = threading.Thread(
        target=self._run_usd_filter_q4_consumer,
        name="usd-q4-consumer-thread",
        )

        processing_thread_started = False
        eof_exit_code=0

        try:
            process_thread.start()
            processing_thread_started = True
            eof_exit_code = self.eof_controller.start()

            if processing_thread_started:
                process_thread.join()

        except Exception as e:
            logging.error(e)
            self.stop()
            return max(eof_exit_code, 2)

        finally:
            self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return max(eof_exit_code, 1)

        return max(eof_exit_code, 0)



def main():
    configure_logging_from_env()
    scather_gather_mapper = ScatherGatherMapper()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in scather gather mapper, stopping consumers...")
        scather_gather_mapper.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return scather_gather_mapper.start()


if __name__ == "__main__":
    sys.exit(main())
