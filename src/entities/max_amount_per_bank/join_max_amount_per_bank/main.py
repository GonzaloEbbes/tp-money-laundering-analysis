import os
import logging
import signal
import sys
import threading
from common import middleware, message_protocol
from common.snapshots.stateful_worker import StatefulWorker
from common.logging.logging_config import configure_logging_from_env
from common.message_protocol.internal import InternalMessageType
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.eof_controller.message_handler import EOFMessageHandler
from message_handler import MessageHandler as JoinMessageHandler

ID = int(os.environ.get("ID", 0))
JOIN_AMOUNT = int(os.environ.get("JOIN_AMOUNT", 1))
MAP_AMOUNT = int(os.environ.get("MAP_AMOUNT", 1))
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
JOIN_EXCHANGE = os.environ.get("JOIN_EXCHANGE", "query2_join_exchange")
JOIN_ROUTING_KEY_PREFIX = os.environ.get("JOIN_ROUTING_KEY_PREFIX", "join_partition")
EOF_CONTROL_EXCHANGE = os.environ.get("EOF_CONTROL_EXCHANGE", "join_control_exchange")

PREFIX_WORKER = os.environ.get("PREFIX_WORKER", "query2_joiner")
INPUT_PREFIX_ACCOUNTS = os.environ.get("INPUT_PREFIX_1", "bank_filter")
INPUT_PREFIX_MAPPERS = os.environ.get("INPUT_PREFIX_2", "map_max_amount_per_bank")
NEXT_STAGE_PREFIX = os.environ.get("NEXT_STAGE_PREFIX", "gateway")

# El Joiner espera EOFs de ambas fuentes: 1 del bank_filter + 1 de los mappers
EXPECTED_INPUT_EOFS = int(os.environ.get("EXPECTED_INPUT_EOFS", 2))

class JoinMaxAmountPerBank(StatefulWorker):
    def __init__(self):
        super().__init__(
            data_dir=f"/data/snapshots/join_max_amount_per_bank_{ID}",
            set_keys=['bank_cache', 'pending_results']
        )

        self.id = ID
        self.routing_key = f"{JOIN_ROUTING_KEY_PREFIX}_{ID}"
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            JOIN_EXCHANGE,
            routing_keys=[self.routing_key],
            queue_name=None,
            exclusive=True  
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        self._output_queue_lock = threading.Lock()

        self.bank_cache = self.state.setdefault('bank_cache', {})
        self.pending_results = self.state.setdefault('pending_results', {})

        self._sigterm_received = False

        self.eof_controller = EOFController(
            mom_host=MOM_HOST,
            id_worker=self.id,
            prefix_worker=PREFIX_WORKER,
            amount_workers=JOIN_AMOUNT,
            eof_control_exchange_name=EOF_CONTROL_EXCHANGE,
            input_eofs_quantities=EXPECTED_INPUT_EOFS,
            on_consensus_ok_callback=self._on_consensus_ok_process_pending_data,
            on_send_eof_to_next_stage_callback=self._on_send_eof_to_next_stage,
            on_clean_client_in_main_thread_callback=self._clean_client_memory
        )
        

    def process_message(self, raw_msg, ack, nack):
        try:
            msg = message_protocol.internal.deserialize(raw_msg)
            cid = msg.source_client_uuid

            match msg.type:
                case InternalMessageType.EOF_MESSAGE | InternalMessageType.EOF_FINAL_MESSAGE:
                    self._handle_eof_message(cid, msg.data)
                case InternalMessageType.BANK_FILTER_TO_JOINER:
                    if msg.data is None:
                        self._handle_eof_message(cid, msg.data)
                    else:
                        self._handle_bank_filter_data(cid, msg)
                case InternalMessageType.MAX_AMOUNT_PER_BANK_RESULT:
                    self._handle_mapper_result_data(cid, msg)

                case _:
                    logging.debug(f"Joiner {self.id} ignorando mensaje: {msg.type}")

            self.append_to_batch(op=None, conn=self.input_exchange._connection, ack_func=ack)
        except Exception as e:
            logging.exception(e)
            nack()

    def _handle_eof_message(self, cid, data):
        logging.debug(f"Joiner {self.id} recibió mensaje EOF para el cliente {cid}")
        self.eof_controller.on_input_queue_eof_reception(cid, data)

    def _handle_bank_filter_data(self, cid, msg):
        if not self.ensure_idempotent(cid, msg.data_id):
            logging.debug(f"Data ID {msg.data_id} already processed for client {cid}, skipping")
            return
        raw_bank_id = msg.data.get("bank_id")
        bank_name = msg.data.get("bank_name")
        if raw_bank_id is not None and bank_name is not None:
            bank_id = int(raw_bank_id)
            self.bank_cache.setdefault(cid, {})[bank_id] = bank_name
            self.state_update(['bank_cache', cid, str(bank_id)], bank_name)
        self.eof_controller.on_processed_packet_by_client(cid, INPUT_PREFIX_ACCOUNTS)

    def _handle_mapper_result_data(self, cid, msg):
        if not self.ensure_idempotent(cid, msg.data_id):
            logging.debug(f"Data ID {msg.data_id} already processed for client {cid}, skipping")
            return
        raw_from_bank = msg.data.get("from_bank")
        amount = msg.data.get("amount_received")
        origin = msg.data.get("account_origin")
        
        if raw_from_bank is not None and amount is not None:
            from_bank = int(raw_from_bank)
            current = self.pending_results.setdefault(cid, {}).get(from_bank)
            if current is None or amount > current[0]:
                new_value = (amount, origin, msg.data_id)
                self.pending_results[cid][from_bank] = new_value
                self.state_update(['pending_results', cid, str(from_bank)], new_value)
        self.eof_controller.on_processed_packet_by_client(cid, INPUT_PREFIX_MAPPERS)

    def _on_consensus_ok_process_pending_data(self, client_id):
        """Ejecutado localmente cuando se alcanza el consenso para realizar el JOIN."""
        if client_id not in self.pending_results:
            return

        logging.debug(f"Joiner {self.id} procesando JOIN final para cliente {client_id}")

        for from_bank, (amount, origin, data_id) in self.pending_results[client_id].items():
            bank_name = self.bank_cache.get(client_id, {}).get(from_bank, "Unknown")
            
            if bank_name == "Unknown":
                logging.warning(f"Join {self.id} no encontró nombre de banco para ID {from_bank} del cliente {client_id}")
                continue

            result_bytes = JoinMessageHandler.serialize_result(
                client_id, data_id, bank_name, origin, amount
            )
            
            with self._output_queue_lock:
                self.output_queue.send(result_bytes)

            self.eof_controller.on_packet_sent_by_client_to(NEXT_STAGE_PREFIX, client_id)

    def _on_send_eof_to_next_stage(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        total_sent_to_gateway = totals_by_output.get(NEXT_STAGE_PREFIX, 0)
        
        eof_bytes = EOFMessageHandler.serialize_eof_message(
            client_id, total_sent_to_gateway, origin_worker_prefix, amount_origin_workers
        )
        
        with self._output_queue_lock:
            self.output_queue.send(eof_bytes)
        logging.info(f"Joiner envió EOF final al gateway para el cliente {client_id}")

    def _clean_client_memory(self, client_id):
        self.clean_client_data(client_id, ['bank_cache', 'pending_results'])

    def _run_input_consumer(self):
        self.input_exchange.start_consuming(self.process_message)

    def start(self):
        input_thread = threading.Thread(
            target=self._run_input_consumer, 
            name=f"joiner-{self.id}-input-consumer"
        )
        input_thread.start()
        eof_exit_code = self.eof_controller.start()
        input_thread.join()

        self.input_exchange.close()
        if hasattr(self, 'output_queue'):
            self.output_queue.close()

        return eof_exit_code


    def stop(self):
        self._sigterm_received = True
        try:
            self.input_exchange._connection.add_callback_threadsafe(
                self.input_exchange.stop_consuming
            )
        except Exception as e:
            logging.error(f"Error stopping consumer: {e}")
        self.eof_controller.on_sigterm()
        self.stop_recoverable_worker()
            

def main():
    configure_logging_from_env()
    w = JoinMaxAmountPerBank()
    
    def _sigterm(*_):
        logging.info("SIGTERM recibido")
        w.stop()
        
    signal.signal(signal.SIGTERM, _sigterm)
    return w.start()


if __name__ == "__main__":
    sys.exit(main())

