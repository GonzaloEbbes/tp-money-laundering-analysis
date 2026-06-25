import hashlib
import os
import logging
import re
import signal
import threading
from time import sleep
import uuid

from common import middleware, message_protocol
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.healthcheck.recovery_controller import RecoveryController
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler
from common.dedup import InMemoryDeduplicator
from common.logging.logging_config import configure_logging_from_env
from message_handler import MessageHandler as AveragePerPayFormatJoinerMessageHandler

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
RECOVERY_PREFIX = os.environ.get("RECOVERY_PREFIX", "recovery")
RECOVERY_AMOUNT = int(os.environ.get("RECOVERY_AMOUNT", "1"))
HEARTBEAT_EXCHANGE = os.environ.get("HEARTBEAT_EXCHANGE", "heartbeat_exchange")
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "2"))
INPUT_QUEUE = os.environ["INPUT_QUEUE"] #Es la que conecta con los mappers
JOINER_PREFIX = os.environ["JOINER_PREFIX"]
JOINER_AMOUNT = int(os.environ["JOINER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]
EXPECTED_INPUT_EOFS = int(os.environ.get("EXPECTED_INPUT_EOFS", 1))
OUTPUT_EXCHANGE = os.environ.get("AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE")
INPUT_PREFIX_1 = os.environ["INPUT_PREFIX_1"] #que es el prefix del mapper
AUXILIARY_INPUT = os.environ["AUXILIARY_INPUT"] == "true" #false
OUTPUT_PREFIX_1 = os.environ["OUTPUT_PREFIX_1"] #el amount filter q3
class AveragePerPayFormatJoiner:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        logging.debug(
            "AveragePerPayFormatJoiner wiring: input_queue=%s output_queue=%s joiner_prefix=%s "
            "joiner_amount=%s expected_input_eofs=%s",
            INPUT_QUEUE,
            OUTPUT_EXCHANGE,
            JOINER_PREFIX,
            JOINER_AMOUNT,
            EXPECTED_INPUT_EOFS,
        )
        
        self.id = int(ID)

        self.recovery_producer_controller = RecoveryController(
            mom_host=MOM_HOST,
            heartbeat_exchange=HEARTBEAT_EXCHANGE,
            id=ID,
            prefix=JOINER_PREFIX,
            recovery_prefix=RECOVERY_PREFIX,
            recovery_amount=RECOVERY_AMOUNT,
            heartbeat_interval=HEARTBEAT_INTERVAL,
        )

        self.producer_lock = threading.Lock()

        # definicion de working queue exchanges de la instancia posterior
        self.output_exchange = middleware.MessageMiddlewareExchangePublisherRabbitMQ(
                MOM_HOST, OUTPUT_EXCHANGE
            ) 
        
        self.averages_by_client = {}
        self.averages_lock = threading.Lock()
        self.deduplicator = InMemoryDeduplicator()

        self._sigterm_received = False
        self._runtime_error = False

        self._stop_lock = threading.Lock()
        self._stopping = False

        self.eof_controller = EOFController(MOM_HOST, self.id, JOINER_PREFIX, JOINER_AMOUNT, EOF_CONTROL_EXCHANGE, EXPECTED_INPUT_EOFS,self.on_consensus_ok_callback,self.on_send_eof_to_next_stage_callback, self.on_clean_client_callback, AUXILIARY_INPUT)

    def _run_average_per_pay_format_mapper_consumer(self):
        try:
            logging.debug(
                "AveragePerPayFormatJoiner consuming average per pay format mapper messages=%s",
                INPUT_QUEUE,
            )
            self.input_queue.start_consuming(self.process_average_per_pay_format_mapper_messagges)
        except Exception as e:
            self._handle_runtime_failure(e, "Average per pay format mapper consumer crashed")
    
    def process_average_per_pay_format_mapper_messagges(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_MAPPER_TO_AVERAGE_PER_PAY_FORMAT_JOINER:
                if not self._should_process_message(message):
                    ack()
                    return
                client_id = message.source_client_uuid
                self._process_average_per_pay_format_mapper_message(message.data, client_id, message.data_id)
                self.eof_controller.on_processed_packet_by_client(client_id, INPUT_PREFIX_1)
                self.deduplicator.mark_processed(client_id, self._dedup_key(message))
            case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                client_id = message.source_client_uuid
                self.eof_controller.on_input_queue_eof_reception(client_id, message.data)
        ack()
        

    def _process_average_per_pay_format_mapper_message(self, transaction_data, client_id, data_id): 
        logging.debug("Received averages from mapper for client=%s", client_id)

        payment_format = transaction_data.get("PaymentFormat")
        if not payment_format:
            return None


        sum_total = float(transaction_data.get("sum_total", 0))
        count = int(transaction_data.get("count", 0))

        with self.averages_lock:
            client_averages = self.averages_by_client.setdefault(client_id, {})
            values = client_averages.setdefault(payment_format, {
                "sum_total": 0.0,
                "count": 0,
            })
            values["sum_total"] += sum_total
            values["count"] += count
    
    def _build_average_payload(self, client_id):
        result = {}
        with self.averages_lock:
            client_averages = self.averages_by_client.get(client_id, {})
        for payment_format, values in client_averages.items():
            count = values["count"]
            if count <= 0:
                continue
            sum_total = values["sum_total"]
            result[payment_format] = {
                "sum_total": sum_total,
                "count": count,
                "average": sum_total / count,
            }
        return result
    
    def on_consensus_ok_callback(self, client_id):
        data_id = str(uuid.uuid4()) 
        averages = self._build_average_payload(client_id)
        with self.producer_lock:
            self.output_exchange.send(AveragePerPayFormatJoinerMessageHandler.serialize_amount_filter_q3_exchange_message(averages, client_id, data_id, message_id=data_id), OUTPUT_EXCHANGE)
            self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_1, client_id)
        logging.debug("Sent averages to amount filter q3 for client=%s", client_id)

    def on_send_eof_to_next_stage_callback(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        with self.producer_lock:
            self.output_exchange.send(EOFMessageHandler.serialize_eof_message(client_id,totals_by_output.get(OUTPUT_PREFIX_1, 0), origin_worker_prefix, amount_origin_workers), OUTPUT_EXCHANGE)
        logging.info(f"Sent final EOF for client {client_id} to average amount filter q3")


    def on_clean_client_callback(self, client_id):
        with self.averages_lock:
            self.averages_by_client.pop(client_id, None)
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

        consumers = [self.input_queue]

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.input_queue]

        if self.output_exchange is not None:
            resources.append(self.output_exchange)

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
        target=self._run_average_per_pay_format_mapper_consumer,
        name="average-per-pay-format-mapper-consumer-thread",
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
            self._close_resources()
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
    average_per_pay_format_joiner = AveragePerPayFormatJoiner()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in average per pay format joiner")
        average_per_pay_format_joiner.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return average_per_pay_format_joiner.start()


if __name__ == "__main__":
    raise SystemExit(main())
