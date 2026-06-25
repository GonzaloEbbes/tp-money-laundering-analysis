import os
import logging
import signal
import sys
import threading
from time import sleep

from common import middleware, message_protocol
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.healthcheck.recovery_controller import RecoveryController
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler
from common.dedup import InMemoryDeduplicator, message_dedup_key
from message_handler import MessageHandler as AmountFilterQ1MessageHandler
from common.logging import configure_logging_from_env


ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
RECOVERY_PREFIX = os.environ.get("RECOVERY_PREFIX", "recovery")
RECOVERY_AMOUNT = int(os.environ.get("RECOVERY_AMOUNT", "1"))
HEARTBEAT_EXCHANGE = os.environ.get("HEARTBEAT_EXCHANGE", "heartbeat_exchange")
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "2"))
USD_FILTER_Q1Q2_QUEUE = os.environ["INPUT_QUEUE"] #Es la propia, que conecta con el filtro USD q1q2
AMOUNT_FILTER_PREFIX = os.environ["AMOUNT_FILTER_PREFIX"]
AMOUNT_FILTER_AMOUNT = int(os.environ["AMOUNT_FILTER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]

OUTPUT_QUEUE = os.environ["GATEWAY_FINAL_QUERY_QUEUE"]
EXPECTED_INPUT_EOFS = int(os.environ["EXPECTED_INPUT_EOFS"]) #1
INPUT_PREFIX_1 = os.environ["INPUT_PREFIX_1"] #que es el prefix del usd filter q1q2
AUXILIARY_INPUT = os.environ["AUXILIARY_INPUT"] == "true"
OUTPUT_PREFIX_1 = os.environ["OUTPUT_PREFIX_1"] #GATEWAY

class AmountFilterQ1: 

    def __init__(self):
        self.usd_filter_q1q2_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, USD_FILTER_Q1Q2_QUEUE
        )
        
        self.id = int(ID)

        self.recovery_producer_controller = RecoveryController(
            mom_host=MOM_HOST,
            heartbeat_exchange=HEARTBEAT_EXCHANGE,
            id=ID,
            prefix=AMOUNT_FILTER_PREFIX,
            recovery_prefix=RECOVERY_PREFIX,
            recovery_amount=RECOVERY_AMOUNT,
            heartbeat_interval=HEARTBEAT_INTERVAL,
        )
        self.producer_lock = threading.Lock()
        # definicion de working queue exchanges de la instancia posterior
        self.gateway_final_query_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE
            )

        self._sigterm_received = False
        self._runtime_error = False
        self._stop_lock = threading.Lock()
        self._stopping = False
        self.deduplicator = InMemoryDeduplicator()
        self.eof_controller = EOFController(MOM_HOST, self.id, AMOUNT_FILTER_PREFIX, AMOUNT_FILTER_AMOUNT, EOF_CONTROL_EXCHANGE, EXPECTED_INPUT_EOFS,None,self.on_send_eof_to_next_stage_callback, None, AUXILIARY_INPUT)

    
    def _run_usd_filter_q1q2_consumer(self):
        try:
            self.usd_filter_q1q2_queue.start_consuming(self.process_usd_filter_q1q2_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "USD filter Q1Q2 consumer crashed")

    
    def process_usd_filter_q1q2_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.USD_FILTER_Q1Q2_TO_AMOUNT_FILTER_Q1:
                if not self._should_process_message(message):
                    ack()
                    return
                client_id = message.source_client_uuid
                self._process_transaction(message.data, client_id, message.data_id, message.message_id)
                self.eof_controller.on_processed_packet_by_client(client_id, INPUT_PREFIX_1)
                self.deduplicator.mark_processed(client_id, self._dedup_key(message))
            case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                client_id = message.source_client_uuid
                self.eof_controller.on_input_queue_eof_reception(client_id, message.data)
        ack()
        
        

    def _process_transaction(self, transaction_data, client_id, data_id, message_id=None):
        logging.debug(f"Received USD_FILTER_Q1Q2_TO_AMOUNT_FILTER_Q1 for client {client_id}")
        amount_received = float(transaction_data.get("amount_received"))

        if amount_received > 0 and amount_received < 50:
            with self.producer_lock:
                self.gateway_final_query_queue.send(AmountFilterQ1MessageHandler.serialize_gateway_query_message(client_id, data_id, transaction_data, message_id=message_id))
                self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_1, client_id)
            logging.debug(f"Transaction for client {client_id} sent to final gateway queue")


    def on_send_eof_to_next_stage_callback(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        with self.producer_lock:
            self.gateway_final_query_queue.send(EOFMessageHandler.serialize_eof_message(client_id, totals_by_output.get(OUTPUT_PREFIX_1, 0), origin_worker_prefix, amount_origin_workers))
        logging.info(f"Sent final EOF for client {client_id} to gateway final query queue")

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

        consumers = [self.usd_filter_q1q2_queue]

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.usd_filter_q1q2_queue]
        if self.gateway_final_query_queue is not None:
            resources.append(self.gateway_final_query_queue)

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
        target=self._run_usd_filter_q1q2_consumer,
        name="usd-q1q2-consumer-thread",
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
    amount_filter_q1 = AmountFilterQ1()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in amount filter q1")
        amount_filter_q1.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return amount_filter_q1.start()


if __name__ == "__main__":
    sys.exit(main())
