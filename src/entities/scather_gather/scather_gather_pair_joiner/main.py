import hashlib
import os
import logging
import signal
import threading

from common import middleware, message_protocol
from common.snapshots.snapshot import SnapshotManager
from common.logging.logging_config import configure_logging_from_env
from message_handler import MessageHandler as ScatherGatherMessageHandler

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

        data_dir = f"/data/snapshots/sg_pair_{self.id}"
        self.snapshot_manager = SnapshotManager(data_dir)
        self.state = self.snapshot_manager.recover()

        self.BATCH_MAX_SIZE = 100
        self.FLUSH_INTERVAL_SECONDS = 2.0
        self.batch_ops = []
        self.batch_acks = []
        self.batch_lock = threading.Lock()

        for key, data in self.state.items():
            if key.startswith('txs_'):
                cid = key[4:]
                for tx in data:
                    self._populate_ram(tx, cid)
            elif key.startswith('eofs_'):
                cid = key[5:]
                self.eof_count_by_client[cid] = data

        #Control de shutdown y estado de clientes
        self._sigterm_received = False
        self._runtime_error = False
        self._finalized_clients = set()
        self._finalized_clients_lock = threading.Lock()
        self._stop_lock = threading.Lock()
        self._stopping = False

        self._stop_flush_event = threading.Event()
        self._flush_thread = threading.Thread(
            target=self._periodic_flush_loop, daemon=True, name=f"flush-sg-pair-{self.id}"
        )
        self._flush_thread.start()

        for cid, count in list(self.eof_count_by_client.items()):
            if count >= SCATHER_GATHER_AGGREGATOR_AMOUNT:
                self._send_data_to_joiners(cid)
                self._send_eof_to_joiners(cid)
                self._finalize_client(cid)

    def _periodic_flush_loop(self):
        while not self._stop_flush_event.wait(timeout=self.FLUSH_INTERVAL_SECONDS):
            self._flush_batch_thread_safe()

    def _flush_batch_thread_safe(self):
        with self.batch_lock: 
            self._flush_batch_locked()

    def _flush_batch_locked(self):
        # 1. Procesar la data del Snapshot si hay algo
        if self.batch_ops:
            if hasattr(self.snapshot_manager, 'apply_batch'):
                self.snapshot_manager.apply_batch(self.batch_ops)
            else:
                for op in self.batch_ops:
                    self.snapshot_manager.apply_operation(op)
            self.batch_ops.clear()
            
        # 2. Los acks SIEMPRE se despachan, aunque no haya habido escrituras a disco
        for conn, ack_func in self.batch_acks:
            if conn and callable(ack_func):
                conn.add_callback_threadsafe(ack_func)
        self.batch_acks.clear()

    def _run_input_exchange_consumer(self):
        try:
            self.scather_gather_pair_joiner_input_exchange.start_consuming(self.process_scather_gather_agg_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Scather Gather pair joiner consumer crashed")
    
    def process_scather_gather_agg_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.SCATHER_GATHER_AGGREGATOR_TO_SCATHER_GATHER_PAIR_JOINER:
                client_id = message.source_client_uuid
                self._process_transaction(message.data, client_id, message.data_id, ack)
            case message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                client_id = message.source_client_uuid
                self._process_scather_gather_agg_eofs(client_id, ack)
            case _:
                ack()

    def _populate_ram(self, tx, client_id):
        type = tx.get("type")
        key = tx.get("key")
        value = tx.get("value")
        if type == "FANIN_MIDDLE":
            logging.debug(f"Received FANIN MIDDLE message for client {client_id}")
            self._process_fanin_transaction_in_middle_structure(client_id, key, value)
        elif type == "FANOUT_MIDDLE":
            logging.debug(f"Received FANOUT MIDDLE message for client {client_id}")
            self._process_fanout_transaction_in_middle_structure(client_id, key, value)
        else:
            logging.warning(f"Received unknown transaction type {type} for client {client_id}")

    def _process_transaction(self, transaction_data, client_id, data_id, ack):
        self._populate_ram(transaction_data, client_id)
        op = {'type': 'append', 'key': f'txs_{client_id}', 'value': transaction_data}
        with self.batch_lock:
            self.batch_ops.append(op)
            self.batch_acks.append((self.scather_gather_pair_joiner_input_exchange._connection, ack))
            if len(self.batch_ops) >= self.BATCH_MAX_SIZE:
                self._flush_batch_locked()

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
                    logging.debug(f"Sent message for client {client_id} with origin {origen}, destination {destino} and middle account {middle_account} to joiner worker {joiner_index}")

        logging.info(f"Sent all data for client {client_id} to scather gather joiners")
        

    def _worker_to_send_data_to_joiners(self, origen, destino):
        hashkey=("".join([origen, destino])).encode("utf-8")
        digest=hashlib.sha256(hashkey).digest()
        value = int.from_bytes(digest, byteorder="big")
        return value % SCATHER_GATHER_JOINER_AMOUNT

    def _process_scather_gather_agg_eofs(self, client_id, ack):
        logging.info(f"Received EOF for client {client_id}")
        self._flush_batch_thread_safe()
        
        self.eof_count_by_client[client_id] = self.eof_count_by_client.get(client_id, 0) + 1
        count = self.eof_count_by_client[client_id]

        op = {'type': 'set', 'key': f'eofs_{client_id}', 'value': count}
        with self.batch_lock:
            self.batch_ops.append(op)
            self.batch_acks.append((self.scather_gather_pair_joiner_input_exchange._connection, ack))
            self._flush_batch_locked() 

        if count == SCATHER_GATHER_AGGREGATOR_AMOUNT:
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
        
        with self.batch_lock:
            self.batch_ops.extend([
                {'type': 'delete', 'key': f'txs_{client_id}'},
                {'type': 'delete', 'key': f'eofs_{client_id}'}
            ])
            self.batch_acks.append((None, None))
            self._flush_batch_locked()

    def stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        self._stop_flush_event.set()
        if hasattr(self, '_flush_thread'):
            self._flush_thread.join()
        self._flush_batch_thread_safe()

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
    configure_logging_from_env()
    scather_gather_pair_joiner = ScatherGatherPairJoiner()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in scather gather pair joiner, stopping consumers...")
        scather_gather_pair_joiner.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return scather_gather_pair_joiner.start()


if __name__ == "__main__":
    main()
