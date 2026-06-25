# src/entities/mappers/map_max_amount_per_bank/main.py
import os
import logging
import signal
import sys
import threading
import zlib

from common import middleware, message_protocol
from common.snapshots.stateful_worker import StatefulWorker
from common.message_protocol.internal import InternalMessageType
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler
from message_handler import MessageHandler as MapperMessageHandler

ID = int(os.environ["ID"])
MAP_AMOUNT = int(os.environ.get("MAP_AMOUNT", 1))
MOM_HOST = os.environ["MOM_HOST"]
MAP_MAX_EXCHANGE = os.environ.get("MAP_MAX_EXCHANGE", "map_max_exchange")
MAP_MAX_ROUTING_KEY_PREFIX = os.environ.get("MAP_MAX_ROUTING_KEY_PREFIX", "map_max_partition")
JOIN_EXCHANGE = os.environ.get("JOIN_EXCHANGE", "query2_join_exchange")
JOIN_AMOUNT = int(os.environ.get("JOIN_AMOUNT", 1))
JOIN_ROUTING_KEY_PREFIX = os.environ.get("JOIN_ROUTING_KEY_PREFIX", "join_partition")

PREFIX_WORKER = os.environ.get("PREFIX_WORKER", "map_max_amount_per_bank")
INPUT_PREFIX = os.environ.get("INPUT_PREFIX", "data_per_bank_redirector") 
EXPECTED_INPUT_EOFS = int(os.environ.get("EXPECTED_INPUT_EOFS", 1)) 
EOF_CONTROL_EXCHANGE = os.environ.get("EOF_CONTROL_EXCHANGE", "mapper_control_exchange")
NEXT_STAGE_PREFIX = os.environ.get("NEXT_STAGE_PREFIX", "query2_joiner")

def stable_hash(value):
    try:
        norm_val = int(value)
    except ValueError:
        norm_val = str(value).strip()
    return zlib.crc32(str(norm_val).encode())

class MapMaxAmountPerBank(StatefulWorker):
    def __init__(self):
        super().__init__(
            data_dir=f"/data/snapshots/map_max_{ID}",
            set_keys=['bank_max'] 
            )
        self.id = ID
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            MAP_MAX_EXCHANGE,
            [f"{MAP_MAX_ROUTING_KEY_PREFIX}_{self.id}"],
            queue_name=None,
            exclusive=True
        )
        self.join_exchange = middleware.MessageMiddlewareExchangePublisherRabbitMQ(
            MOM_HOST, JOIN_EXCHANGE
        )

        self.bank_max = self.state.setdefault('bank_max', {})
        self._join_exchange_lock = threading.Lock()
        self._sigterm_received = False

        self.eof_controller = EOFController(
            mom_host=MOM_HOST,
            id_worker=self.id,
            prefix_worker=PREFIX_WORKER,
            amount_workers=MAP_AMOUNT,
            eof_control_exchange_name=EOF_CONTROL_EXCHANGE,
            input_eofs_quantities=EXPECTED_INPUT_EOFS,
            on_consensus_ok_callback=self._on_consensus_ok_process_pending_data, 
            on_send_eof_to_next_stage_callback=self._on_send_eof_to_next_stage,
            on_clean_client_in_main_thread_callback=self._clean_client_memory
        )

    def _on_consensus_ok_process_pending_data(self, client_id):
        """
        Callback ejecutado por el EOFController localmente en cada nodo (no solo el líder)
        cuando se alcanza el consenso de que no llegarán más datos.
        Aquí enviamos los resultados agregados locales hacia el Joiner.
        """
        if client_id not in self.bank_max:
            return

        logging.info(f"Mapper {self.id} procesando datos pendientes tras consenso EOF para cliente {client_id}")
        
        for from_bank, (amount, origin) in self.bank_max[client_id].items():
            partition = stable_hash(from_bank) % JOIN_AMOUNT
            routing_key = f"{JOIN_ROUTING_KEY_PREFIX}_{partition}"
            result_bytes = MapperMessageHandler.serialize_result(
                client_id, "FINAL_RESULT", from_bank, amount, origin
            )
            with self._join_exchange_lock:
                self.join_exchange.send(result_bytes, routing_key=routing_key)
            
            self.eof_controller.on_packet_sent_by_client_to(NEXT_STAGE_PREFIX, client_id)

        logging.info(f"Mapper {self.id} envió {len(self.bank_max[client_id])} resultados finales para cliente {client_id}")

    def _on_send_eof_to_next_stage(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        total_sent_to_joiner = totals_by_output.get(NEXT_STAGE_PREFIX, 0)
        
        eof_bytes = EOFMessageHandler.serialize_eof_message(
            client_id, total_sent_to_joiner, origin_worker_prefix, amount_origin_workers
        )
        
        for i in range(JOIN_AMOUNT):
            routing_key = f"{JOIN_ROUTING_KEY_PREFIX}_{i}"
            with self._join_exchange_lock:
                self.join_exchange.send(eof_bytes, routing_key=routing_key)
        logging.info(f"Se envió EOF al map_max_amount_per_bank_joiner para cliente {client_id}")

    def _clean_client_memory(self, client_id):
        self.clean_client_data(client_id, ['bank_max'])

    def process_message(self, raw_msg, ack, nack):
        try:
            msg = message_protocol.internal.deserialize(raw_msg)
            cid = msg.source_client_uuid

            if cid not in self.bank_max:
                self.bank_max[cid] = {}

            match msg.type:
                case InternalMessageType.EOF_MESSAGE | InternalMessageType.EOF_FINAL_MESSAGE:
                    self._handle_eof_message(cid, msg.data)
                case InternalMessageType.DATA_PER_BANK_SHUFFLER_TO_MAP_MAX_AMOUNT_PER_BANK:
                    self._handle_data_message(cid, msg)
                case _:
                    logging.debug(f"Mapper {self.id} ignorando mensaje: {msg.type}")
            self.append_to_batch(op=None, conn=self.input_exchange._connection, ack_func=ack)
        except Exception as e:
            logging.exception(e)
            nack()

    def _handle_eof_message(self, cid, data):
        logging.debug(f"Mapper {self.id} recibió mensaje EOF para el cliente {cid}")
        self.eof_controller.on_input_queue_eof_reception(cid, data)

    def _handle_data_message(self, cid, msg):
        data_id = msg.data_id
        if not self.ensure_idempotent(cid, data_id):
            logging.debug(f"Data ID {data_id} already processed for client {cid}, skipping")
            return
        bank_id = int(msg.data.get("from_bank"))
        amount = float(msg.data.get("amount_received"))
        origin = msg.data.get("account_origin")

        client_dict = self.bank_max.setdefault(cid, {})
        current_max = client_dict.get(bank_id)
        
        if current_max is None or amount > current_max[0]:
            new_val = (amount, origin)
            client_dict[bank_id] = new_val
            self.state_update(['bank_max', cid, str(bank_id)], new_val)
            # Re-envío al joiner (si fuera necesario actualizar el máximo)
            serialized = MapperMessageHandler.serialize_result(cid, data_id, bank_id, amount, origin)
            partition = stable_hash(bank_id) % JOIN_AMOUNT
            routing_key = f"{JOIN_ROUTING_KEY_PREFIX}_{partition}"

            self.join_exchange.send(serialized, routing_key=routing_key)
            self.eof_controller.on_packet_sent_by_client_to("joiner", cid)
        
        self.eof_controller.on_processed_packet_by_client(cid, INPUT_PREFIX)

    def _run_input_consumer(self):
        self.input_exchange.start_consuming(self.process_message)

    def start(self):
        input_thread = threading.Thread(
            target=self._run_input_consumer, 
            name=f"mapper-{self.id}-input-consumer"
        )
        input_thread.start()
        eof_exit_code = self.eof_controller.start()
        input_thread.join()

        self.input_exchange.close()
        if hasattr(self, 'join_exchange'):
            self.join_exchange.close()
        
        return eof_exit_code

    def stop(self):
        self._sigterm_received = True
        try:
            self.input_exchange._connection.add_callback_threadsafe(
                self.input_exchange.stop_consuming
            )
        except Exception as e:
            logging.error(f"Error al detener consumidor: {e}")
            
        self.eof_controller.on_sigterm()
        self.stop_recoverable_worker()

def main():
    logging.basicConfig(level=logging.INFO)
    w = MapMaxAmountPerBank()
    def _sigterm(*_):
        logging.info("SIGTERM received")
        w.stop()
    signal.signal(signal.SIGTERM, _sigterm)
    return w.start()
    

if __name__ == "__main__":
    sys.exit(main())
    
