import zlib
import os
import logging
from random import randint
import signal
import sys
import threading

from common import middleware, message_protocol
from common.snapshots.stateful_worker import StatefulWorker
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.healthcheck.recovery_controller import RecoveryController
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler
from common.dedup import InMemoryDeduplicator, message_dedup_key
from common.logging.logging_config import configure_logging_from_env
from message_handler import MessageHandler as ScatherGatherMessageHandler

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
RECOVERY_PREFIX = os.environ.get("RECOVERY_PREFIX", "recovery")
RECOVERY_AMOUNT = int(os.environ.get("RECOVERY_AMOUNT", "1"))
HEARTBEAT_EXCHANGE = os.environ.get("HEARTBEAT_EXCHANGE", "heartbeat_exchange")
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "2"))

SCATHER_GATHER_PAIR_JOINER_PREFIX = os.environ["SCATHER_GATHER_PAIR_JOINER_PREFIX"]
SCATHER_GATHER_PAIR_JOINER_AMOUNT = int(os.environ["SCATHER_GATHER_PAIR_JOINER_AMOUNT"])
SCATHER_GATHER_JOINER_AMOUNT = int(os.environ["SCATHER_GATHER_JOINER_AMOUNT"])
SCATHER_GATHER_JOINER_PREFIX = os.environ["SCATHER_GATHER_JOINER_PREFIX"]
FANIN_FANOUT_THRESHOLD = 5

EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]
EXPECTED_INPUT_EOFS = int(os.environ["EXPECTED_INPUT_EOFS"]) #1
INPUT_PREFIX_1 = os.environ["INPUT_PREFIX_1"] #que es el prefix del aggregator
AUXILIARY_INPUT = os.environ["AUXILIARY_INPUT"] == "true" #false
OUTPUT_PREFIX_1 = os.environ["OUTPUT_PREFIX_1"] #al joiner


class ScatherGatherPairJoiner(StatefulWorker):

    def __init__(self):
        super().__init__(
            data_dir=f"/data/snapshots/scather_gather_pair_joiner_{ID}",
            set_keys=['middle_accounts_by_client']
        )

        self.scather_gather_pair_joiner_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SCATHER_GATHER_PAIR_JOINER_PREFIX, [f"{SCATHER_GATHER_PAIR_JOINER_PREFIX}_{ID}"]
        )
        
        self.id = int(ID)

        self.recovery_producer_controller = RecoveryController(
            mom_host=MOM_HOST,
            heartbeat_exchange=HEARTBEAT_EXCHANGE,
            id=ID,
            prefix=SCATHER_GATHER_PAIR_JOINER_PREFIX,
            recovery_prefix=RECOVERY_PREFIX,
            recovery_amount=RECOVERY_AMOUNT,
            heartbeat_interval=HEARTBEAT_INTERVAL,
        )

        # definicion de exchanges para enviar a los agregadores
        self.scather_gather_joiner_exchanges : list[middleware.MessageMiddlewareExchangeRabbitMQ] = []
        self.producer_lock = threading.Lock()
        
        for i in range(SCATHER_GATHER_JOINER_AMOUNT):
            scather_gather_joiner_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SCATHER_GATHER_JOINER_PREFIX, [f"{SCATHER_GATHER_JOINER_PREFIX}_{i}"]
            )
            self.scather_gather_joiner_exchanges.append(scather_gather_joiner_exchange)


<<<<<<< HEAD
        self.middle_accounts_by_client = self.state.setdefault('middle_accounts_by_client', {})
        self.processed_ids = self.state.setdefault('processed_ids', {})

        for cid, middle_dict in self.middle_accounts_by_client.items():
            for middle_account, (origenes, destinos) in middle_dict.items():
                if isinstance(origenes, list):
                    middle_dict[middle_account] = (set(origenes), set(destinos) if not isinstance(destinos, set) else destinos)
                if isinstance(destinos, list):
                    middle_dict[middle_account] = (set(origenes) if not isinstance(origenes, set) else origenes, set(destinos))

        for cid, ids in self.processed_ids.items():
            if isinstance(ids, list):
                self.processed_ids[cid] = set(ids)

=======
        self.dicts_lock = threading.Lock()
        self.middle_accounts_by_client : dict[str, dict[str, (set[str],set[str])]] = {} 
        self.deduplicator = InMemoryDeduplicator()
>>>>>>> origin/add-recovery-controller

        #Control de shutdown y estado de clientes
        self._sigterm_received = False
        self._runtime_error = False
        self._stop_lock = threading.Lock()
        self._stopping = False

        self.eof_controller = EOFController(
            MOM_HOST,
            self.id,
            SCATHER_GATHER_PAIR_JOINER_PREFIX,
            SCATHER_GATHER_PAIR_JOINER_AMOUNT,
            EOF_CONTROL_EXCHANGE,
            EXPECTED_INPUT_EOFS,
            self.on_consensus_ok_callback,
            self.on_send_eof_to_next_stage_callback,
            self.on_clean_client_callback,
            AUXILIARY_INPUT,
            self.state.setdefault('eof_state', {}),
            self.append_to_batch
        )

    
    def _run_input_exchange_consumer(self):
        try:
            self.scather_gather_pair_joiner_input_exchange.start_consuming(self.process_scather_gather_agg_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Scather Gather pair joiner consumer crashed")
    
    def process_scather_gather_agg_messages(self, message, ack, nack):
<<<<<<< HEAD
        try:
            message = message_protocol.internal.deserialize(message)
            client_id = message.source_client_uuid
            match message.type:
                case message_protocol.internal.InternalMessageType.SCATHER_GATHER_AGGREGATOR_TO_SCATHER_GATHER_PAIR_JOINER:
                    self._process_transaction(message.data, client_id, message.data_id)
                case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                    self.eof_controller.on_input_queue_eof_reception(client_id, message.data)
            self.append_to_batch(None, self.scather_gather_pair_joiner_input_exchange._connection, ack)
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            nack()
=======
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.SCATHER_GATHER_AGGREGATOR_TO_SCATHER_GATHER_PAIR_JOINER:
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
>>>>>>> origin/add-recovery-controller
        
    def _process_transaction(self, transaction_data, client_id, data_id):
        if not self.ensure_idempotent(client_id, data_id):
            logging.debug(f"Data ID {data_id} already processed for client {client_id}, skipping")
            return

        type = transaction_data.get("type")
        key = transaction_data.get("key")
        value = transaction_data.get("value")
        if type == "FANIN_MIDDLE":
            logging.debug(f"Received FANIN_MIDDLE message for client {client_id}")
            self._process_fanin_transaction_in_middle_structure(client_id, key, value)
        elif type == "FANOUT_MIDDLE":
            logging.debug(f"Received FANOUT_MIDDLE message for client {client_id}")
            self._process_fanout_transaction_in_middle_structure(client_id, key, value)
        else:
            logging.warning(f"Received unknown transaction type {type} for client {client_id}")
        self.eof_controller.on_processed_packet_by_client(client_id, INPUT_PREFIX_1)

    def _process_fanin_transaction_in_middle_structure(self, client_id, destination, middle_account):
        middle_dict = self.middle_accounts_by_client.setdefault(client_id, {})
        origenes, destinos = middle_dict.setdefault(middle_account, (set(), set()))
        if destination not in destinos:
            destinos.add(destination)
            self.state_add_to_set(['middle_accounts_by_client', client_id, middle_account, 'destinos'], destination)

    def _process_fanout_transaction_in_middle_structure(self, client_id, origin, middle_account):
        middle_dict = self.middle_accounts_by_client.setdefault(client_id, {})
        origenes, destinos = middle_dict.setdefault(middle_account, (set(), set()))
        if origin not in origenes:
            origenes.add(origin)
            self.state_add_to_set(['middle_accounts_by_client', client_id, middle_account, 'origenes'], origin)

    def on_send_eof_to_next_stage_callback(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        eof_message = EOFMessageHandler.serialize_eof_message(client_id, totals_by_output.get(OUTPUT_PREFIX_1, 0), origin_worker_prefix, amount_origin_workers)
        with self.producer_lock:
            #EL EOF SE ENVIA A UNA INSTANCIA ALEATORIA
            eof_random_instance = randint(0, SCATHER_GATHER_JOINER_AMOUNT - 1)
            self.scather_gather_joiner_exchanges[eof_random_instance].send(eof_message)
        logging.info(f"Sent final EOF for client {client_id} to scather gather joiners")

    def on_consensus_ok_callback(self, client_id):
        client_data = self.middle_accounts_by_client.get(client_id, {})
        for middle_account, data in client_data.items():
            origenes = data.get('origenes', set())
            destinos = data.get('destinos', set())
            if not origenes or not destinos:
                continue
            for origen in origenes:
                for destino in destinos:
                    msg = ScatherGatherMessageHandler.serialize_scather_gather_pair_middle_message(
                        client_id, origen, destino, middle_account
                    )
                    joiner_index = self._worker_to_send_data_to_joiners(origen, destino)
                    with self.producer_lock:
                        self.scather_gather_joiner_exchanges[joiner_index].send(msg)
                        self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_1, client_id)
                    logging.debug(f"Sent PAIR_MIDDLE for client {client_id}: {origen} -> {destino} via {middle_account}")
        logging.info(f"Sent all data for client {client_id} to scather gather joiners")
        

    def _worker_to_send_data_to_joiners(self, origen, destino):
        hashkey=("".join([origen, destino])).encode("utf-8")
        value = zlib.crc32(str(hashkey).encode("utf-8"))
        return value % SCATHER_GATHER_JOINER_AMOUNT

    def on_clean_client_callback(self, client_id):
<<<<<<< HEAD
        self.clean_client_data(client_id, ['middle_accounts_by_client'])
=======

        with self.dicts_lock:
            if client_id in self.middle_accounts_by_client:
                del self.middle_accounts_by_client[client_id]
        self.deduplicator.remove_client(client_id)

    def _dedup_key(self, message):
        return message_dedup_key(message)

    def _should_process_message(self, message):
        return self.deduplicator.should_process(
            message.source_client_uuid, self._dedup_key(message)
        )

>>>>>>> origin/add-recovery-controller

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
        self.stop_recoverable_worker()


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
        self.eof_controller.on_sigterm()
        self.recovery_producer_controller.on_sigterm()

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
        stop_recovery_controller_callback = None
        eof_exit_code=0
        recovery_controller_exit_code = 0

        try:
            stop_recovery_controller_callback = (
                self.recovery_producer_controller.start_recovery_producer_controller()
            )

            process_thread.start()
            processing_thread_started = True
            eof_exit_code = self.eof_controller.start()

            if processing_thread_started:
                process_thread.join()

        except Exception as e:
            logging.error(e)
            self.stop()
            return max(eof_exit_code, recovery_controller_exit_code, 2)

        finally:
            if stop_recovery_controller_callback is not None:
                recovery_controller_exit_code = stop_recovery_controller_callback()

            self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return max(eof_exit_code, recovery_controller_exit_code, 1)

        return max(eof_exit_code, recovery_controller_exit_code, 0)


def main():
    configure_logging_from_env()
    scather_gather_pair_joiner = ScatherGatherPairJoiner()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in scather gather pair joiner, stopping consumers...")
        scather_gather_pair_joiner.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return scather_gather_pair_joiner.start()


if __name__ == "__main__":
    sys.exit(main())
