import os
import logging
import signal
import sys
import threading

from common import middleware, message_protocol
from common.snapshots.recoverable_worker import RecoverableWorker
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler
from common.logging.logging_config import configure_logging_from_env
from message_handler import MessageHandler as USDFilterMessageHandler


ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
USD_FILTER_PREFIX = os.environ["USD_FILTER_PREFIX"]
USD_FILTER_AMOUNT = int(os.environ["USD_FILTER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]

AMOUNT_FILTER_Q1_QUEUE = os.environ["AMOUNT_FILTER_Q1_QUEUE"]
DATA_PER_BANK_SHUFFLER_QUEUE = os.environ["DATA_PER_BANK_SHUFFLER_QUEUE"]

EXPECTED_INPUT_EOFS = int(os.environ["EXPECTED_INPUT_EOFS"]) #1
INPUT_PREFIX_1 = os.environ["INPUT_PREFIX_1"] #que es el prefix del gateway
AUXILIARY_INPUT = os.environ["AUXILIARY_INPUT"] == "true" #va false
OUTPUT_PREFIX_1 = os.environ["OUTPUT_PREFIX_1"] #amount filter q1
OUTPUT_PREFIX_2 = os.environ["OUTPUT_PREFIX_2"] #data per bank redirector
 
class USDFilterQ1Q2(RecoverableWorker):

    def __init__(self):
        super().__init__(data_dir=f"/data/snapshots/usd_filter_q1q2_{ID}")
        self.gateway_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.id = int(ID)
        self.producer_lock = threading.Lock()

        # definicion de working queue exchanges de la instancia posterior
        self.amount_filter_q1_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, AMOUNT_FILTER_Q1_QUEUE
            )
        self.data_per_bank_shuffler_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, DATA_PER_BANK_SHUFFLER_QUEUE
            )

        self._sigterm_received = False
        self._runtime_error = False

        self._stop_lock = threading.Lock()
        self._stopping = False

        self.eof_controller = EOFController(
            MOM_HOST,
            self.id,
            USD_FILTER_PREFIX,
            USD_FILTER_AMOUNT,
            EOF_CONTROL_EXCHANGE,
            EXPECTED_INPUT_EOFS,
            None,
            self.on_send_eof_to_next_stage_callback,
            None,
            AUXILIARY_INPUT,
            self.state.setdefault('eof_state', {}),
            self.append_to_batch
            )

    
    def _run_gateway_consumer(self):
        try:
            self.gateway_queue.start_consuming(self.process_gateway_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Gateway consumer crashed")

    
    def process_gateway_messages(self, message, ack, nack):
        try:
            message = message_protocol.internal.deserialize(message)
            client_id = message.source_client_uuid
            match message.type:
                case message_protocol.internal.InternalMessageType.GATEWAY_TO_USD_FILTER_Q1Q2:
                    self._process_transaction(message.data, client_id, message.data_id)
                case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                    self.eof_controller.on_input_queue_eof_reception(client_id, message.data)
            self.append_to_batch(None, self.gateway_queue._connection, ack)
        except Exception as e:
            logging.exception("Error en filtro USD: %s", e)
            nack()
        

    def _process_transaction(self, transaction_data, client_id, data_id):
        if self.ensure_idempotent(client_id, data_id):
            logging.debug(f"Received GATEWAY_TO_USD_FILTER_Q1Q2 for client {client_id}")
            payment_currency = transaction_data.get("payment_currency")
            receiving_currency = transaction_data.get("receiving_currency")
            
            if payment_currency == "US Dollar" and receiving_currency == "US Dollar":
                with self.producer_lock:
                    self.amount_filter_q1_queue.send(USDFilterMessageHandler.serialize_amount_filter_q1_message(client_id, data_id, transaction_data))
                    self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_1, client_id)
                    self.data_per_bank_shuffler_queue.send(USDFilterMessageHandler.serialize_data_per_bank_redirector_message(client_id, data_id, transaction_data))
                    self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_2, client_id)
                logging.debug(f"Transaction for client {client_id} sent to amount filter and data per bank shuffler")
            self.eof_controller.on_processed_packet_by_client(client_id, INPUT_PREFIX_1)
        else:
            logging.debug(f"Data ID {data_id} already processed for client {client_id}, skipping")


    def on_send_eof_to_next_stage_callback(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        with self.producer_lock:
            self.amount_filter_q1_queue.send(EOFMessageHandler.serialize_eof_message(client_id,totals_by_output.get(OUTPUT_PREFIX_1, 0), origin_worker_prefix, amount_origin_workers))
            self.data_per_bank_shuffler_queue.send(EOFMessageHandler.serialize_eof_message(client_id,totals_by_output.get(OUTPUT_PREFIX_2, 0), origin_worker_prefix, amount_origin_workers))
        logging.info(f"Sent final EOF for client {client_id} to all downstream queues")

    def stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        consumers = [self.gateway_queue]

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")
        self.stop_recoverable_worker()
        

    def _close_resources(self):
        resources = [self.gateway_queue]
        if self.amount_filter_q1_queue is not None:
            resources.append(self.amount_filter_q1_queue)
        if self.data_per_bank_shuffler_queue is not None:
            resources.append(self.data_per_bank_shuffler_queue)

        for resource in resources:
            try:
                resource.close()
            except Exception as e:
                logging.error(f"Error closing resource: {e}")

    def notify_sigterm(self):
        self._sigterm_received = True
        self.stop()
        self.eof_controller.on_sigterm()

    def _handle_runtime_failure(self, error, context):
        logging.error(f"{context}: {error}")
        self._runtime_error = True
        self.stop()
        self.eof_controller.on_stop()
    
    def start(self):

        process_thread = threading.Thread(
            target=self._run_gateway_consumer,
            name="gateway-consumer-thread",
        )

        processing_thread_started = False
        eof_exit_code=0

        try:
            process_thread.start()
            processing_thread_started = True
            eof_exit_code = self.eof_controller.start()

            if processing_thread_started:
                process_thread.join()

        except Exception as e:
            logging.error(e)
            self.stop()
            return max(eof_exit_code, 2)

        finally:
            self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return max(eof_exit_code, 1)

        return max(eof_exit_code, 0)


def main():
    configure_logging_from_env()
    usd_filter_q1q2 = USDFilterQ1Q2()

    def _handle_sigterm(signum, frame):
        logging.debug("SIGTERM received in usd filter q1q2")
        usd_filter_q1q2.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return usd_filter_q1q2.start()


if __name__ == "__main__":
    sys.exit(main())