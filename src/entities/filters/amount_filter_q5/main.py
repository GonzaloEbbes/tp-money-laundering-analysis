import os
import logging
import signal
import sys
import threading
import uuid

from common import middleware, message_protocol
from common.snapshots.recoverable_worker import RecoverableWorker
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.healthcheck.recovery_controller import RecoveryController
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler
from common.dedup import InMemoryDeduplicator, message_dedup_key
from common.logging.logging_config import configure_logging_from_env
from message_handler import MessageHandler as AmountFilterQ5MessageHandler

ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
RECOVERY_PREFIX = os.environ.get("RECOVERY_PREFIX", "recovery")
RECOVERY_AMOUNT = int(os.environ.get("RECOVERY_AMOUNT", "1"))
HEARTBEAT_EXCHANGE = os.environ.get("HEARTBEAT_EXCHANGE", "heartbeat_exchange")
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "2"))
PAY_FORMAT_FILTER_AND_CURRENCY_CONVERTER_QUEUE = os.environ["INPUT_QUEUE"] #Es la propia, que conecta con ambos dos filtros
AMOUNT_FILTER_PREFIX = os.environ["AMOUNT_FILTER_PREFIX"]
AMOUNT_FILTER_AMOUNT = int(os.environ["AMOUNT_FILTER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]

OUTPUT_QUEUE = os.environ["GATEWAY_FINAL_QUERY_QUEUE"]
EXPECTED_INPUT_EOFS = int(os.environ["EXPECTED_INPUT_EOFS"]) #son 2
INPUT_PREFIX_1 = os.environ["INPUT_PREFIX_1"] #que es el prefix del pay format filter
INPUT_PREFIX_2 = os.environ["INPUT_PREFIX_2"] #que es el prefix del currency converter
AUXILIARY_INPUT = os.environ["AUXILIARY_INPUT"] == "true" #va en false
OUTPUT_PREFIX_1 = os.environ["OUTPUT_PREFIX_1"] 

class AmountFilterQ5(RecoverableWorker):
    def __init__(self):
        super().__init__(data_dir=f"/data/snapshots/amount_filter_q5_{ID}")
        self.pay_format_filter_and_currency_converter_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, PAY_FORMAT_FILTER_AND_CURRENCY_CONVERTER_QUEUE
        )
        logging.debug(
            "AmountFilterQ5 wiring: input_queue=%s output_queue=%s amount_filter_prefix=%s "
            "amount_filter_amount=%s expected_input_eofs=%s",
            PAY_FORMAT_FILTER_AND_CURRENCY_CONVERTER_QUEUE,
            OUTPUT_QUEUE,
            AMOUNT_FILTER_PREFIX,
            AMOUNT_FILTER_AMOUNT,
            EXPECTED_INPUT_EOFS,
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

        # definicion de working queue exchanges de la instancia posterior
        self.gateway_final_query_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE
            )
        self.producer_lock = threading.Lock()
            

        self._sigterm_received = False
        self._runtime_error = False

        self._stop_lock = threading.Lock()
        self._stopping = False
        self.deduplicator = InMemoryDeduplicator()

        self.eof_controller = EOFController(
            MOM_HOST, 
            self.id, 
            AMOUNT_FILTER_PREFIX, 
            AMOUNT_FILTER_AMOUNT, 
            EOF_CONTROL_EXCHANGE, 
            EXPECTED_INPUT_EOFS,
            None,
            self.on_send_eof_to_next_stage_callback, 
            None,
            AUXILIARY_INPUT,
            self.state.setdefault('eof_state', {}),
            self.append_to_batch
        )


    
    def _run_pay_format_filter_and_currency_converter_consumer(self):
        try:
            logging.debug(
                "AmountFilterQ5 consuming combined Q5 queue=%s",
                PAY_FORMAT_FILTER_AND_CURRENCY_CONVERTER_QUEUE,
            )
            self.pay_format_filter_and_currency_converter_queue.start_consuming(self.process_pay_format_and_currency_converter_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Pay format filter and currency converter consumer crashed")

    
    def process_pay_format_and_currency_converter_messages(self, message, ack, nack):
<<<<<<< HEAD
        try:
            message = message_protocol.internal.deserialize(message)
            match message.type:
                case message_protocol.internal.InternalMessageType.USD_CURRENCY_CONVERTER_TO_AMOUNT_FILTER_Q5:
                    client_id = message.source_client_uuid
                    self._process_q5_transaction_message(message.data, client_id, message.data_id, INPUT_PREFIX_2)
                case message_protocol.internal.InternalMessageType.PAY_FORMAT_FILTER_TO_AMOUNT_FILTER_Q5:
                    client_id = message.source_client_uuid
                    self._process_q5_transaction_message(message.data, client_id, message.data_id, INPUT_PREFIX_1)
                    
                case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                    client_id = message.source_client_uuid
                    self.eof_controller.on_input_queue_eof_reception(client_id, message.data)
                    
            self.append_to_batch(None, self.pay_format_filter_and_currency_converter_queue._connection, ack)
=======
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.USD_CURRENCY_CONVERTER_TO_AMOUNT_FILTER_Q5:
                if not self._should_process_message(message):
                    ack()
                    return
                client_id = message.source_client_uuid
                self._process_usd_currency_converter_message(message.data, client_id, message.data_id)
                self.eof_controller.on_processed_packet_by_client(client_id, INPUT_PREFIX_2)
                self.deduplicator.mark_processed(client_id, self._dedup_key(message))
                
            case message_protocol.internal.InternalMessageType.PAY_FORMAT_FILTER_TO_AMOUNT_FILTER_Q5:
                if not self._should_process_message(message):
                    ack()
                    return
                client_id = message.source_client_uuid
                self._process_pay_format_message(message.data, client_id, message.data_id)
                self.eof_controller.on_processed_packet_by_client(client_id, INPUT_PREFIX_1)
                self.deduplicator.mark_processed(client_id, self._dedup_key(message))
                
            case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                client_id = message.source_client_uuid
                self.eof_controller.on_input_queue_eof_reception(client_id, message.data)
        ack()
        
>>>>>>> origin/add-recovery-controller

        except Exception as e:
            logging.error("Error in process_messages: %s", e)
            nack()

    def _process_q5_transaction_message(self, transaction_data, client_id, data_id, prefix):
        if not self.ensure_idempotent(client_id, data_id):
            logging.debug(f"Data ID {data_id} already processed for client {client_id}, skipping")
            return
        amount_paid = float(transaction_data.get("amount_paid"))

        if amount_paid > 0 and amount_paid < 1:
            self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_1, client_id) 
            #simulo como que se envió paquete para que el total de paquetes se transforme en el total de transacciones que pasarían a la siguiente capa
        self.eof_controller.on_processed_packet_by_client(client_id, prefix)
        

    def on_send_eof_to_next_stage_callback(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        data_id = str(uuid.uuid4())
        count = totals_by_output.get(OUTPUT_PREFIX_1, 0)

        with self.producer_lock:
            self.gateway_final_query_queue.send(
                AmountFilterQ5MessageHandler.serialize_gateway_query_message(
                    client_id,
                    data_id,
<<<<<<< HEAD
                    {"cantTrx": count},
=======
                    {"cantTrx": totals_by_output.get(OUTPUT_PREFIX_1, 0)},
                    message_id=data_id,
>>>>>>> origin/add-recovery-controller
                )
            )
            self.gateway_final_query_queue.send(EOFMessageHandler.serialize_eof_message(client_id, 1, origin_worker_prefix, amount_origin_workers, None))
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
        
        self.stop_recoverable_worker()

        consumers = [self.pay_format_filter_and_currency_converter_queue]

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.exception("Error stopping AmountFilterQ5 consumer: %s", e)

    def _close_resources(self):
        resources = [self.pay_format_filter_and_currency_converter_queue]
        if self.gateway_final_query_queue is not None:
            resources.append(self.gateway_final_query_queue)

        for resource in resources:
            try:
                resource.close()
            except Exception as e:
                logging.exception("Error closing AmountFilterQ5 resource %s: %s", type(resource).__name__, e)

    def notify_sigterm(self):
        self._sigterm_received = True
        self.stop()
        self.eof_controller.on_sigterm()
<<<<<<< HEAD
        self.stop_recoverable_worker()
=======
        self.recovery_producer_controller.on_sigterm()
>>>>>>> origin/add-recovery-controller

    def _handle_runtime_failure(self, error, context):
        logging.exception("%s: %s", context, error)
        self._runtime_error = True
        self.stop()
        self.eof_controller.on_stop()
        self.stop_recoverable_worker()
    
    def start(self):

        process_thread = threading.Thread(
        target=self._run_pay_format_filter_and_currency_converter_consumer,
        name="pay-format-filter-and-currency-converter-thread",
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
            logging.exception("AmountFilterQ5 start failed")
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
    amount_filter_q5 = AmountFilterQ5()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in amount filter q5")
        amount_filter_q5.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return amount_filter_q5.start()


if __name__ == "__main__":
    sys.exit(main())
