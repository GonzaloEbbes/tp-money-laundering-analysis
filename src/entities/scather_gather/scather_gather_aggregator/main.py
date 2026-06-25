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
SCATHER_GATHER_AGG_PREFIX = os.environ["SCATHER_GATHER_AGG_PREFIX"]
SCATHER_GATHER_AGG_AMOUNT = int(os.environ["SCATHER_GATHER_AGG_AMOUNT"])
SCATHER_GATHER_PAIR_JOINER_AMOUNT = int(os.environ["SCATHER_GATHER_PAIR_JOINER_AMOUNT"])
SCATHER_GATHER_PAIR_JOINER_PREFIX = os.environ["SCATHER_GATHER_PAIR_JOINER_PREFIX"]
FANIN_FANOUT_THRESHOLD = 5
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]
EXPECTED_INPUT_EOFS = int(os.environ["EXPECTED_INPUT_EOFS"]) #1
INPUT_PREFIX_1 = os.environ["INPUT_PREFIX_1"] #que es el prefix del mapper
AUXILIARY_INPUT = os.environ["AUXILIARY_INPUT"] == "true" #false
OUTPUT_PREFIX_1 = os.environ["OUTPUT_PREFIX_1"] #al pair joiner


class ScatherGatherAggregator(StatefulWorker):

    def __init__(self):
        super().__init__(
            data_dir=f"/data/snapshots/scather_gather_aggregator_{ID}",
            set_keys=[
                'posible_fanin_by_client',
                'posible_fanout_by_client',
                'fanin_by_client',
                'fanout_by_client'
            ]
        )

        self.scather_gather_agg_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SCATHER_GATHER_AGG_PREFIX, [f"{SCATHER_GATHER_AGG_PREFIX}_{ID}"]
        )
        
        self.id = int(ID)

        self.recovery_producer_controller = RecoveryController(
            mom_host=MOM_HOST,
            heartbeat_exchange=HEARTBEAT_EXCHANGE,
            id=ID,
            prefix=SCATHER_GATHER_AGG_PREFIX,
            recovery_prefix=RECOVERY_PREFIX,
            recovery_amount=RECOVERY_AMOUNT,
            heartbeat_interval=HEARTBEAT_INTERVAL,
        )

        # definicion de exchanges para enviar a los agregadores
        self.scather_gather_pair_joiner_exchanges = []
        self.producer_lock = threading.Lock()

        for i in range(SCATHER_GATHER_PAIR_JOINER_AMOUNT):
            scather_gather_pair_joiner_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SCATHER_GATHER_PAIR_JOINER_PREFIX, [f"{SCATHER_GATHER_PAIR_JOINER_PREFIX}_{i}"]
            )
            self.scather_gather_pair_joiner_exchanges.append(scather_gather_pair_joiner_exchange)


<<<<<<< HEAD
        self.posible_fanin_by_client = self.state.setdefault('posible_fanin_by_client', {})
        self.posible_fanout_by_client = self.state.setdefault('posible_fanout_by_client', {})
        self.fanin_by_client = self.state.setdefault('fanin_by_client', {})
        self.fanout_by_client = self.state.setdefault('fanout_by_client', {})
        self.processed_ids = self.state.setdefault('processed_ids', {})  # {client_id: set(data_id)}

=======
        self.dicts_lock = threading.Lock()
        self.posible_fanin_by_client: dict[str, dict[str, set[str]]] = {}
        self.posible_fanout_by_client: dict[str, dict[str, set[str]]] = {}
        self.fanout_by_client : dict[str, dict[str, set[str]]] = {}
        self.fanin_by_client : dict[str, dict[str, set[str]]] = {}
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
            SCATHER_GATHER_AGG_PREFIX,
            SCATHER_GATHER_AGG_AMOUNT,
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
            self.scather_gather_agg_input_exchange.start_consuming(self.process_scather_gather_mapper_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Scather Gather aggregator consumer crashed")
    
    def process_scather_gather_mapper_messages(self, message, ack, nack):
<<<<<<< HEAD
        try:
            message = message_protocol.internal.deserialize(message)
            client_id = message.source_client_uuid
            match message.type:
                case message_protocol.internal.InternalMessageType.SCATHER_GATHER_MAPPER_TO_SCATHER_GATHER_AGGREGATOR:
                    self._process_transaction(message.data, client_id, message.data_id)
                    
                case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                    self.eof_controller.on_input_queue_eof_reception(client_id, message.data)
            self.append_to_batch(None, self.scather_gather_agg_input_exchange._connection, ack)
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            nack()
=======
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.SCATHER_GATHER_MAPPER_TO_SCATHER_GATHER_AGGREGATOR:
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
        if type == "FANIN":
            logging.debug(f"Received FANIN message for client {client_id}")
            self._process_fanin_transaction(client_id, key, value)
        elif type == "FANOUT":
            logging.debug(f"Received FANOUT message for client {client_id}")
            self._process_fanout_transaction(client_id, key, value)
        else:
            logging.warning(f"Received unknown transaction type {type} for client {client_id}")
        self.eof_controller.on_processed_packet_by_client(client_id, INPUT_PREFIX_1)


    def _process_fanin_transaction(self, client_id, destination, new_origins):
        dest_dict = self.posible_fanin_by_client.setdefault(client_id, {})
        origins_set = dest_dict.setdefault(destination, set())
        for origin in new_origins:
            if origin not in origins_set:
                origins_set.add(origin)
                self.state_add_to_set(['posible_fanin_by_client', client_id, destination], origin)

        if len(origins_set) >= FANIN_FANOUT_THRESHOLD:
            self.fanin_by_client.setdefault(client_id, {})[destination] = set(origins_set)
            self.state_update(['fanin_by_client', client_id, destination], list(origins_set))

    def _process_fanout_transaction(self, client_id, origin, new_destinations):
        origin_dict = self.posible_fanout_by_client.setdefault(client_id, {})
        destination_set = origin_dict.setdefault(origin, set())
        if not isinstance(new_destinations, (list, set, tuple)):
            new_destinations = [new_destinations]
        
        for destino in new_destinations:
            if destino not in destination_set:
                destination_set.add(destino)
                self.state_add_to_set(['posible_fanout_by_client', client_id, origin], destino)
        
        if len(destination_set) >= FANIN_FANOUT_THRESHOLD:
            self.fanout_by_client.setdefault(client_id, {})[origin] = set(destination_set)
            self.state_update(['fanout_by_client', client_id, origin], list(destination_set))

    def _save_in_final_fanin(self, client_id, destination, origins):
        self.fanin_by_client.setdefault(client_id, {})[destination] = set(origins)

    def _save_in_final_fanout(self, client_id, origin, destinations):
        self.fanout_by_client.setdefault(client_id, {})[origin] = set(destinations)

    def on_send_eof_to_next_stage_callback(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        eof_message = EOFMessageHandler.serialize_eof_message(client_id, totals_by_output.get(OUTPUT_PREFIX_1, 0), origin_worker_prefix, amount_origin_workers)
        with self.producer_lock:
            #EL EOF SE ENVIA A UNA INSTANCIA ALEATORIA
            eof_random_instance = randint(0, SCATHER_GATHER_PAIR_JOINER_AMOUNT - 1)
            self.scather_gather_pair_joiner_exchanges[eof_random_instance].send(eof_message)
        logging.info(f"Sent final EOF for client {client_id} to scather gather pair joiners")

    def _send_data_to_pair_joiners(self, client_id):
        fanout_data = {
            origen: sorted(destinos)
            for origen, destinos in self.fanout_by_client.get(client_id, {}).items()
        }
        fanin_data = {
            destino: sorted(origenes)
            for destino, origenes in self.fanin_by_client.get(client_id, {}).items()
        }
        #buclea por las cuentas del medio del fanout para enviar un mensaje por cada middle account a los pair joiners correspondientes
        for (origen,destinos) in fanout_data.items():
            for destino_middle in destinos: 
                joiner_worker = self._worker_to_send_data_to_pair_joiners(destino_middle)
                with self.producer_lock:
                    self.scather_gather_pair_joiner_exchanges[joiner_worker].send(ScatherGatherMessageHandler.serialize_scather_gather_middle_message_fanout(client_id, origen, destino_middle))
                    self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_1, client_id)
                logging.debug(f"Sent FANOUT message for client {client_id} with origin {origen} and middle destination {destino_middle} to pair joiner worker {joiner_worker}")
        del fanout_data
        
        for (destino,origenes) in fanin_data.items():
            for origen_middle in origenes:
                joiner_worker = self._worker_to_send_data_to_pair_joiners(origen_middle)
                with self.producer_lock:
                    self.scather_gather_pair_joiner_exchanges[joiner_worker].send(ScatherGatherMessageHandler.serialize_scather_gather_middle_message_fanin(client_id, destino, origen_middle))
                    self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_1, client_id)
                logging.debug(f"Sent FANIN message for client {client_id} with destination {destino} and middle origin {origen_middle} to pair joiner worker {joiner_worker}")
        del fanin_data


    def _worker_to_send_data_to_pair_joiners(self, middle_account : str):
        hashkey=middle_account.encode("utf-8")
        value = zlib.crc32(str(hashkey).encode("utf-8"))
        return value % SCATHER_GATHER_PAIR_JOINER_AMOUNT

    def on_consensus_ok_callback(self, client_id):
        self._discard_below_threshold_candidates(client_id)
        self._send_data_to_pair_joiners(client_id)


    def _discard_below_threshold_candidates(self, client_id):
        self.clean_client_data(client_id, ['posible_fanin_by_client', 'posible_fanout_by_client'])
    
    def on_clean_client_callback(self, client_id):
<<<<<<< HEAD
        self.clean_client_data(client_id, [
        'posible_fanin_by_client',
        'posible_fanout_by_client',
        'fanin_by_client',
        'fanout_by_client'
        ])
=======

        with self.dicts_lock:
            if client_id in self.posible_fanout_by_client:
                del self.posible_fanout_by_client[client_id]
            if client_id in self.posible_fanin_by_client:
                del self.posible_fanin_by_client[client_id]
            if client_id in self.fanout_by_client:
                del self.fanout_by_client[client_id]
            if client_id in self.fanin_by_client:
                del self.fanin_by_client[client_id]
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

        consumers = [self.scather_gather_agg_input_exchange]

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.scather_gather_agg_input_exchange]

        resources.extend(self.scather_gather_pair_joiner_exchanges)

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
    scather_gather_aggregator = ScatherGatherAggregator()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in scather gather aggregator, stopping consumers...")
        scather_gather_aggregator.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return scather_gather_aggregator.start()


if __name__ == "__main__":
    sys.exit(main())