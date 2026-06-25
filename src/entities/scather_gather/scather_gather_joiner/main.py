import hashlib
import os
import logging
import signal
import sys
import threading

from common import middleware, message_protocol
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler
from common.dedup import InMemoryDeduplicator
from common.logging.logging_config import configure_logging_from_env
from message_handler import MessageHandler as ScatherGatherMessageHandler

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]

SCATHER_GATHER_JOINER_AMOUNT = int(os.environ["SCATHER_GATHER_JOINER_AMOUNT"])
SCATHER_GATHER_JOINER_PREFIX = os.environ["SCATHER_GATHER_JOINER_PREFIX"]
OUTPUT_QUEUE = os.environ["GATEWAY_FINAL_QUERY_QUEUE"]

MINIMUM_FANIN_FANOUT_THRESHOLD = 5
EXPECTED_INPUT_EOFS = int(os.environ["EXPECTED_INPUT_EOFS"]) #1
INPUT_PREFIX_1 = os.environ["INPUT_PREFIX_1"] #que es el prefix del pair joiner
AUXILIARY_INPUT = os.environ["AUXILIARY_INPUT"] == "true" #false
OUTPUT_PREFIX_1 = os.environ["OUTPUT_PREFIX_1"] #al gateway


class ScatherGatherJoiner:

    def __init__(self):
        self.scather_gather_join_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SCATHER_GATHER_JOINER_PREFIX, [f"{SCATHER_GATHER_JOINER_PREFIX}_{ID}"]
        )
        
        self.id = int(ID)

        # definicion de exchanges para enviar a los agregadores
        self.gateway_final_query_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        self.producer_lock = threading.Lock()

        self.dicts_lock = threading.Lock()
        self.scather_gather_accounts : dict[str, dict[tuple[str], set[str]]] = {}
        self.deduplicator = InMemoryDeduplicator()

        #Control de shutdown y estado de clientes
        self._sigterm_received = False
        self._runtime_error = False
        self._stop_lock = threading.Lock()
        self._stopping = False

        self.eof_controller = EOFController(MOM_HOST, self.id, SCATHER_GATHER_JOINER_PREFIX, SCATHER_GATHER_JOINER_AMOUNT, EOF_CONTROL_EXCHANGE, EXPECTED_INPUT_EOFS,self.on_consensus_ok_callback,self.on_send_eof_to_next_stage_callback, self.on_clean_client_callback, AUXILIARY_INPUT)
    
    def _run_input_exchange_consumer(self):
        try:
            self.scather_gather_join_input_exchange.start_consuming(self.process_scather_gather_pair_joiner_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Scather Gather joiner consumer crashed")

    def process_scather_gather_pair_joiner_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.SCATHER_GATHER_PAIR_JOINER_TO_SCATHER_GATHER_JOINER:
                if not self._should_process_message(message):
                    ack()
                    return
                client_id = message.source_client_uuid
                self._process_transaction(message.data, client_id, message.data_id)
                self.eof_controller.on_processed_packet_by_client(client_id, INPUT_PREFIX_1)
                self.deduplicator.mark_processed(client_id, self._dedup_key(message))
            case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                client_id = message.source_client_uuid
                self.eof_controller.on_input_queue_eof_reception(client_id, message.data)
        ack()
    

    def _process_transaction(self, transaction_data, client_id, data_id):
        type = transaction_data.get("type")
        value = transaction_data.get("value")
        
        if type == "PAIR_MIDDLE":
            logging.debug(f"Received PAIR_MIDDLE message for client {client_id}")
            [origen, destino, middle_account] = value
            self._process_pair_middle_transaction(client_id, origen, destino, middle_account)
        else:
            logging.warning(f"Received unknown transaction type {type} for client {client_id}")

    def _process_pair_middle_transaction(self, client_id, origen, destino, middle_account):
        with self.dicts_lock:
            self.scather_gather_accounts.setdefault(client_id, {}).setdefault(tuple([origen, destino]), set()).add(middle_account)


    def on_consensus_ok_callback(self, client_id):
        with self.dicts_lock:
            final_data = {
                (origen, destino): middle_accounts
                for (origen, destino), middle_accounts in self.scather_gather_accounts.get(client_id, {}).items()
                if origen != destino
            }

        for (origen, destino), middle_accounts in final_data.items():
            if len(middle_accounts)>=MINIMUM_FANIN_FANOUT_THRESHOLD:
                message = ScatherGatherMessageHandler._serialize_scather_gather_final_message(client_id, origen, destino)
                with self.producer_lock:
                    self.gateway_final_query_queue.send(message)
                    self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_1, client_id)
                logging.debug(f"Sent final data for client {client_id} to gateway final query queue for pair ({origen}, {destino}) with middle accounts {middle_accounts}")
        logging.info(f"Sent all final data for client {client_id} to gateway final query queue")

    def on_send_eof_to_next_stage_callback(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        with self.producer_lock:
            self.gateway_final_query_queue.send(EOFMessageHandler.serialize_eof_message(client_id, totals_by_output.get(OUTPUT_PREFIX_1, 0), origin_worker_prefix, amount_origin_workers))
        logging.info(f"Sent final EOF for client {client_id} to gateway final query queue")

    def on_clean_client_callback(self, client_id):
        with self.dicts_lock:
            self.scather_gather_accounts.pop(client_id, None)
        self.deduplicator.remove_client(client_id)

    def _dedup_key(self, message):
        if message.message_id is None:
            return None
        return f"{message.type}:{message.message_id}"

    def _should_process_message(self, message):
        return self.deduplicator.should_process(
            message.source_client_uuid, self._dedup_key(message)
        )


    def stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        consumers = [self.scather_gather_join_input_exchange]

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
            target=self._run_input_exchange_consumer,
            name="input-exchange-consumer-thread",
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
    scather_gather_joiner = ScatherGatherJoiner()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in scather gather joiner, stopping consumers...")
        scather_gather_joiner.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return scather_gather_joiner.start()


if __name__ == "__main__":
    sys.exit(main()) 
