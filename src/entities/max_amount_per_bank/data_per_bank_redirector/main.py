import os
import logging
import signal
import threading
import zlib
import sys
from common import middleware, message_protocol
from common.snapshots.recoverable_worker import RecoverableWorker
from common.logging.logging_config import configure_logging_from_env
from common.message_protocol.internal import InternalMessageType
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.eof_controller.message_handler import EOFMessageHandler
from message_handler import MessageHandler as DataPerBankRedirectorMessageHandler

ID = int(os.environ.get("ID", 0))
DATA_PER_BANK_REDIRECTOR_AMOUNT = int(os.environ.get("DATA_PER_BANK_REDIRECTOR_AMOUNT", 1))
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
EOF_CONTROL_EXCHANGE = os.environ.get("EOF_CONTROL_EXCHANGE", "data_per_bank_control_exchange")
MAP_MAX_EXCHANGE = os.environ.get("MAP_MAX_EXCHANGE", "map_max_exchange")
MAP_MAX_ROUTING_KEY_PREFIX = os.environ.get("MAP_MAX_ROUTING_KEY_PREFIX", "map_max_partition")
TOTAL_MAPPERS = int(os.environ.get("TOTAL_MAPPERS", 1))

PREFIX_WORKER = os.environ.get("PREFIX_WORKER", "data_per_bank_redirector")
INPUT_PREFIX = os.environ.get("INPUT_PREFIX", "usd_filter_q1q2")
EXPECTED_INPUT_EOFS = int(os.environ.get("EXPECTED_INPUT_EOFS", 1))
MAPPER_PREFIX = os.environ.get("MAPPER_PREFIX", "mapper")


def stable_hash(value):
    try:
        norm_val = int(value)
    except ValueError:
        norm_val = str(value).strip()
    return zlib.crc32(str(norm_val).encode())

class DataPerBankRedirector(RecoverableWorker):
    def __init__(self):
        super().__init__(data_dir=f"/data/snapshots/redirector_{ID}")
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.map_exchange = middleware.MessageMiddlewareExchangePublisherRabbitMQ(MOM_HOST, MAP_MAX_EXCHANGE)
        self.id = ID
        self._sigterm_received = False
        self._map_exchange_lock = threading.Lock()

        self.eof_controller = EOFController(
            mom_host=MOM_HOST,
            id_worker=self.id,
            prefix_worker=PREFIX_WORKER,
            amount_workers=DATA_PER_BANK_REDIRECTOR_AMOUNT,
            eof_control_exchange_name=EOF_CONTROL_EXCHANGE,
            input_eofs_quantities=EXPECTED_INPUT_EOFS,
            on_consensus_ok_callback=None, 
            on_send_eof_to_next_stage_callback=self._on_send_eof_to_mappers,
            on_clean_client_in_main_thread_callback=None,
            recovered_state=self.state.setdefault('eof_state', {}),
            append_batch_callback=self.append_to_batch
        )

    def _on_send_eof_to_mappers(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        total_sent_to_mappers = totals_by_output.get(MAPPER_PREFIX, 0)
        
        eof_bytes = EOFMessageHandler.serialize_eof_message(
            client_id, total_sent_to_mappers, origin_worker_prefix, amount_origin_workers
        )
        
        for i in range(TOTAL_MAPPERS):
            routing_key = f"{MAP_MAX_ROUTING_KEY_PREFIX}_{i}"
            with self._map_exchange_lock:
                self.map_exchange.send(eof_bytes, routing_key=routing_key)
        logging.info(f"Redirector worker {self.id} (Líder) envió EOF final a mappers para cliente {client_id}")

    def _run_input_consumer(self):
        self.input_queue.start_consuming(self.process_message)

    def process_message(self, raw_msg, ack, nack):
        try:
            msg = message_protocol.internal.deserialize(raw_msg)
            cid = msg.source_client_uuid
            match msg.type:
                case InternalMessageType.EOF_MESSAGE | InternalMessageType.EOF_FINAL_MESSAGE:
                    self._handle_eof_message(cid, msg.data)
                case InternalMessageType.USD_FILTER_Q1Q2_TO_DATA_PER_BANK_SHUFFLER:
                    self._handle_data_message(cid, msg)
                case _:
                    logging.debug(f"Redirector {self.id} ignorando mensaje de tipo no soportado o de control en la cola de datos: {msg.type}")

            self.append_to_batch(None, self.input_queue._connection, ack)
        except Exception as e:
            logging.exception(f"Error procesando mensaje: {e}")
            nack()

    def _handle_eof_message(self, cid, data):
        """Maneja la recepción de mensajes de Fin de Flujo (EOF) delegándolo al controlador."""
        logging.debug(f"Redirector {self.id} recibió mensaje EOF para el cliente {cid}")
        self.eof_controller.on_input_queue_eof_reception(cid, data)

    def _handle_data_message(self, cid, msg):
        if not self.ensure_idempotent(cid, msg.data_id):
            return
        from_bank = None
        raw_bank_id = msg.data.get("from_bank")
        if raw_bank_id is not None:
            from_bank = str(int(raw_bank_id))
        
        if from_bank is not None:
            partition = stable_hash(from_bank) % TOTAL_MAPPERS
            routing_key = f"{MAP_MAX_ROUTING_KEY_PREFIX}_{partition}"
            account_origin = msg.data.get("account_origin")
            amount_received = msg.data.get("amount_received")
            
            redirect_msg = DataPerBankRedirectorMessageHandler.serialize_redirect(
                cid, msg.data_id, from_bank, account_origin, amount_received
            )
            
            with self._map_exchange_lock:
                self.map_exchange.send(redirect_msg, routing_key=routing_key)
            
            self.eof_controller.on_packet_sent_by_client_to(MAPPER_PREFIX, cid)

        self.eof_controller.on_processed_packet_by_client(cid, INPUT_PREFIX)

    def start(self):
        input_thread = threading.Thread(
            target=self._run_input_consumer, 
            name=f"redirector-{self.id}-input-consumer"
        )
        input_thread.start()
        eof_exit_code = self.eof_controller.start()
        input_thread.join()

        self.input_queue.close()
        if hasattr(self, 'map_exchange'):
            self.map_exchange.close()
        
        return eof_exit_code

    def stop(self):
        self._sigterm_received = True
        try:
            self.input_queue._connection.add_callback_threadsafe(
                self.input_queue.stop_consuming
            )
        except Exception as e:
            logging.error(f"Error al detener consumidor: {e}")
            
        self.eof_controller.on_sigterm()
        self.stop_recoverable_worker()

def main():
    configure_logging_from_env()
    w = DataPerBankRedirector()
    def _sigterm(*_):
        logging.info("SIGTERM received")
        w.stop()
    signal.signal(signal.SIGTERM, _sigterm)
    return w.start()

if __name__ == "__main__":
    sys.exit(main())

