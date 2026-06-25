# src/entities/filters/bank_filter/main.py
import os
import logging
import signal
import threading
import zlib
from common import middleware, message_protocol
from common.logging.logging_config import configure_logging_from_env
from common.message_protocol.internal import InternalMessageType
from message_handler import MessageHandler as BankFilterMessageHandler
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler

ID = int(os.environ["ID"])
BANK_FILTERS_AMOUNT = int(os.environ.get("BANK_FILTERS_AMOUNT", 1))
MOM_HOST = os.environ["MOM_HOST"]
BANK_EXCHANGE = os.environ.get("BANK_EXCHANGE", "bank_exchange")
BANK_ROUTING_KEY_PREFIX = os.environ.get("BANK_ROUTING_KEY_PREFIX", "bank_partition")
JOIN_EXCHANGE = os.environ.get("JOIN_EXCHANGE", "query2_join_exchange")
JOIN_ROUTING_KEY_PREFIX = os.environ.get("JOIN_ROUTING_KEY_PREFIX", "join_partition")
JOIN_AMOUNT = int(os.environ.get("JOIN_AMOUNT", 1))
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]

PREFIX_WORKER = os.environ.get("PREFIX_WORKER", "bank_filter")
INPUT_PREFIX = os.environ.get("INPUT_PREFIX", "gateway")
EXPECTED_INPUT_EOFS = int(os.environ.get("EXPECTED_INPUT_EOFS", 1))
NEXT_STAGE_PREFIX = os.environ.get("NEXT_STAGE_PREFIX", "query2_joiner")

def stable_hash(value):
    try:
        norm_val = int(value)
    except ValueError:
        norm_val = str(value).strip()
    return zlib.crc32(str(norm_val).encode())

class BankFilter:
    def __init__(self):
        self.id = ID
        self.routing_key = f"{BANK_ROUTING_KEY_PREFIX}_{ID}"
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            BANK_EXCHANGE,
            routing_keys=[self.routing_key],
            queue_name=None,
            exclusive=True
        )
        self.join_exchange = middleware.MessageMiddlewareExchangePublisherRabbitMQ(
            MOM_HOST, JOIN_EXCHANGE
        )
        self._sigterm_received = False

        self.seen_banks = {} # {cid: set()}
        self._join_exchange_lock = threading.Lock()

        self.eof_controller = EOFController(
            mom_host=MOM_HOST,
            id_worker=self.id,
            prefix_worker=PREFIX_WORKER,
            amount_workers=BANK_FILTERS_AMOUNT,
            eof_control_exchange_name=EOF_CONTROL_EXCHANGE,
            input_eofs_quantities=EXPECTED_INPUT_EOFS,
            on_consensus_ok_callback=None,
            on_send_eof_to_next_stage_callback=self._on_send_eof_to_joiner,
            on_clean_client_in_main_thread_callback=self._clean_client_memory
        )


    def process_message(self, raw_msg, ack, nack):
        try:
            msg = message_protocol.internal.deserialize(raw_msg)
            cid = msg.source_client_uuid

            match msg.type:
                case InternalMessageType.EOF_MESSAGE | InternalMessageType.EOF_FINAL_MESSAGE:
                    self._handle_eof_message(cid, msg.data)
                case InternalMessageType.GATEWAY_TO_BANK_FILTER:
                    self._handle_data_message(cid, msg)
                case _:
                    logging.debug(f"BankFilter {self.id} ignorando mensaje: {msg.type}")

            ack()
        except Exception as e:
            logging.exception(e)
            nack()

    def _handle_eof_message(self, cid, data):
        logging.debug(f"BankFilter {self.id} recibió mensaje EOF para el cliente {cid}")
        self.eof_controller.on_input_queue_eof_reception(cid, data)

    def _handle_data_message(self, cid, msg):
        raw_bank_id = msg.data.get("bank_id")
        if raw_bank_id is None:
            raw_bank_id = msg.data.get("id")

        bank_name = msg.data.get("bank_name")

        if raw_bank_id is not None:
            bank_id = int(raw_bank_id)

            if cid not in self.seen_banks:
                self.seen_banks[cid] = set()

            if bank_id not in self.seen_banks[cid]:
                self.seen_banks[cid].add(bank_id)
                partition = stable_hash(bank_id) % JOIN_AMOUNT
                routing_key = f"{JOIN_ROUTING_KEY_PREFIX}_{partition}"

                serialized = BankFilterMessageHandler.serialize_join_message(
                    cid, msg.data_id, bank_id, bank_name, message_id=msg.message_id
                )

                with self._join_exchange_lock:
                    self.join_exchange.send(serialized, routing_key=routing_key)

                self.eof_controller.on_packet_sent_by_client_to(NEXT_STAGE_PREFIX, cid)
        self.eof_controller.on_processed_packet_by_client(cid, INPUT_PREFIX)

    def _on_send_eof_to_joiner(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        total_sent_to_joiner = totals_by_output.get(NEXT_STAGE_PREFIX, 0)

        eof_bytes = EOFMessageHandler.serialize_eof_message(
            client_id, total_sent_to_joiner, origin_worker_prefix, amount_origin_workers
        )

        for i in range(JOIN_AMOUNT):
            routing_key = f"{JOIN_ROUTING_KEY_PREFIX}_{i}"
            with self._join_exchange_lock:
                self.join_exchange.send(eof_bytes, routing_key=routing_key)
        logging.info(f"BankFilter envió EOF final al map_max_amount_per_bank_joiner para cliente {client_id}")

    def _clean_client_memory(self, client_id):
        """Callback que el controlador llama una vez que todo el proceso ha finalizado con éxito."""
        if client_id in self.seen_banks:
            del self.seen_banks[client_id]
            logging.debug(f"Memoria de bancos limpiada para el cliente {client_id} en BankFilter {self.id}")

    def _run_input_consumer(self):
        self.input_exchange.start_consuming(self.process_message)


    def start(self):
        input_thread = threading.Thread(
            target=self._run_input_consumer,
            name=f"bankfilter-{self.id}-input-consumer"
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

def main():
    configure_logging_from_env()
    w = BankFilter()

    def _sigterm(*_):
        logging.info("SIGTERM recibido")
        w.stop()

    signal.signal(signal.SIGTERM, _sigterm)
    exit_code = w.start()

    import sys
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
