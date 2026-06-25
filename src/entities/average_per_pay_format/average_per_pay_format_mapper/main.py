import hashlib
import os
import logging
import re
import signal
import sys
import threading
from time import sleep
import uuid

from common import middleware, message_protocol
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.healthcheck.recovery_controller import RecoveryController
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler
from common.dedup import InMemoryDeduplicator, message_dedup_key
from common.logging.logging_config import configure_logging_from_env
from common.message_protocol.internal import TransactionData
from message_handler import MessageHandler as AveragePerPayFormatMapperMessageHandler

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
RECOVERY_PREFIX = os.environ.get("RECOVERY_PREFIX", "recovery")
RECOVERY_AMOUNT = int(os.environ.get("RECOVERY_AMOUNT", "1"))
HEARTBEAT_EXCHANGE = os.environ.get("HEARTBEAT_EXCHANGE", "heartbeat_exchange")
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "2"))
USD_FILTER_Q4_TO_AVERAGE_PER_PAY_FORMAT_MAPPER_QUEUE = os.environ["INPUT_QUEUE"] #Es la propia, que conecta con ambos dos filtros
MAPPER_FILTER_PREFIX = os.environ["MAPPER_FILTER_PREFIX"]
MAPPER_FILTER_AMOUNT = int(os.environ["MAPPER_FILTER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]
EXPECTED_INPUT_EOFS = int(os.environ.get("EXPECTED_INPUT_EOFS", "1"))
INPUT_PREFIX_1 = os.environ["INPUT_PREFIX_1"] #que es el prefix del usd filter q4
AUXILIARY_INPUT = os.environ["AUXILIARY_INPUT"] == "true" #false
OUTPUT_PREFIX_1 = os.environ["OUTPUT_PREFIX_1"] #el joiner

OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"] #average_per_pay_format_mapper_to_average_per_pay_format_aggregator_queue


class AveragePerPayFormatMapper:

    def __init__(self):
        self.usd_filter_q4_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, USD_FILTER_Q4_TO_AVERAGE_PER_PAY_FORMAT_MAPPER_QUEUE
        )
        
        self.id = int(ID)

        self.recovery_producer_controller = RecoveryController(
            mom_host=MOM_HOST,
            heartbeat_exchange=HEARTBEAT_EXCHANGE,
            id=ID,
            prefix=MAPPER_FILTER_PREFIX,
            recovery_prefix=RECOVERY_PREFIX,
            recovery_amount=RECOVERY_AMOUNT,
            heartbeat_interval=HEARTBEAT_INTERVAL,
        )

        self.producer_lock = threading.Lock()

        # definicion de working queue exchanges de la instancia posterior
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE
            )

        self._sigterm_received = False
        self._runtime_error = False

        self.averages_per_client : dict[str, dict[str, dict[str,float]]] = {}
        self.averages_per_client_lock = threading.Lock()
        self.deduplicator = InMemoryDeduplicator()

        self._stop_lock = threading.Lock()
        self._stopping = False

        self.eof_controller = EOFController(MOM_HOST, self.id, MAPPER_FILTER_PREFIX, MAPPER_FILTER_AMOUNT, EOF_CONTROL_EXCHANGE, EXPECTED_INPUT_EOFS,self.on_consensus_ok_callback,self.on_send_eof_to_next_stage_callback, self.on_clean_client_callback, AUXILIARY_INPUT)


    def _run_usd_filter_q4_consumer(self):
        try:
            logging.debug(
                "AveragePerPayFormatMapper consuming usd filter q4 queue=%s",
                USD_FILTER_Q4_TO_AVERAGE_PER_PAY_FORMAT_MAPPER_QUEUE,
            )
            self.usd_filter_q4_queue.start_consuming(self.process_usd_filter_q4_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "usd filter q4 consumer crashed")


    def process_usd_filter_q4_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.USD_FILTER_Q4_TO_AVERAGE_PER_PAY_FORMAT_MAPPER:
                if not self._should_process_message(message):
                    ack()
                    return
                client_id = message.source_client_uuid
                self._process_usd_filter_q4_message(message.data, client_id, message.data_id)
                self.eof_controller.on_processed_packet_by_client(client_id, INPUT_PREFIX_1)
                self.deduplicator.mark_processed(client_id, self._dedup_key(message))
            case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                client_id = message.source_client_uuid
                self.eof_controller.on_input_queue_eof_reception(client_id, message.data)
        ack()

    def _process_usd_filter_q4_message(self, transaction_data, client_id, data_id): 
        payment_format = transaction_data.get("payment_format")
        if not client_id or not payment_format:
            return

        try:
            amount = float(transaction_data.get("amount_received", 0))
        except (TypeError, ValueError):
            return

        with self.averages_per_client_lock:
            if client_id not in self.averages_per_client:
                self.averages_per_client[client_id] = {}
            if payment_format not in self.averages_per_client[client_id]:
                self.averages_per_client[client_id][payment_format] = {"sum_total": 0, "count": 0}
            self.averages_per_client[client_id][payment_format]["sum_total"] += amount
            self.averages_per_client[client_id][payment_format]["count"] += 1


    def on_send_eof_to_next_stage_callback(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        with self.producer_lock:
            self.output_queue.send(EOFMessageHandler.serialize_eof_message(client_id, totals_by_output.get(OUTPUT_PREFIX_1, 0), origin_worker_prefix, amount_origin_workers))
        logging.info(f"Sent final EOF for client {client_id} to average per pay format joiner")


    def on_consensus_ok_callback(self, client_id):
        data_id = str(uuid.uuid4())
        self._send_data_to_joiner(client_id, data_id)

    def _send_data_to_joiner(self, client_id, data_id):
        with self.averages_per_client_lock:
            averages_in_client = dict(self.averages_per_client.get(client_id, {}))

        for payment_format, values in averages_in_client.items():
            message_id = f"{data_id}:{payment_format}"
    
            with self.producer_lock:
                self.output_queue.send(
                    AveragePerPayFormatMapperMessageHandler.serialize_average_per_pay_joiner_message(client_id, data_id, payment_format, values, message_id=message_id)
                )
                self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_1, client_id)
    
    def on_clean_client_callback(self, client_id):
        with self.averages_per_client_lock:
            self.averages_per_client.pop(client_id, None)
        self.deduplicator.remove_client(client_id)

    def _dedup_key(self, message):
        return message_dedup_key(message)

    def _should_process_message(self, message):
        return self.deduplicator.should_process(
            message.source_client_uuid, self._dedup_key(message)
        )

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

        if self.output_queue is not None:
            resources.append(self.output_queue)

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
            target=self._run_usd_filter_q4_consumer,
            name="usd-q4-consumer-thread",
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
    average_per_pay_format_mapper = AveragePerPayFormatMapper()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in average per pay format mapper")
        average_per_pay_format_mapper.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return average_per_pay_format_mapper.start()

if __name__ == "__main__":
    sys.exit(main())
