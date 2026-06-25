import hashlib
import os
import logging
import re
import signal
import sys
import threading
from time import sleep

from common import middleware, message_protocol
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler
from common.dedup import InMemoryDeduplicator, message_dedup_key
from common.logging.logging_config import configure_logging_from_env
from message_handler import MessageHandler as DateFilterMessageHandler


ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
DATE_FILTER_PREFIX = os.environ["DATE_FILTER_PREFIX"]
DATE_FILTER_AMOUNT = int(os.environ["DATE_FILTER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]

USD_FILTER_Q3_QUEUE = os.environ["USD_FILTER_Q3_QUEUE"]
USD_FILTER_Q4_QUEUE = os.environ["USD_FILTER_Q4_QUEUE"]
PAY_FORMAT_FILTER_QUEUE = os.environ["PAY_FORMAT_FILTER_QUEUE"]

EXPECTED_INPUT_EOFS = int(os.environ["EXPECTED_INPUT_EOFS"])
INPUT_PREFIX_1 = os.environ["INPUT_PREFIX_1"] #que es el prefix del gateway
AUXILIARY_INPUT = os.environ["AUXILIARY_INPUT"] == "true"
OUTPUT_PREFIX_1 = os.environ["OUTPUT_PREFIX_1"] #que es el prefix del USD FILTER Q3
OUTPUT_PREFIX_2 = os.environ["OUTPUT_PREFIX_2"] #que es el prefix del USD FILTER Q4
OUTPUT_PREFIX_3 = os.environ["OUTPUT_PREFIX_3"] #que es el prefix del PAY FORMAT FILTER



class DateFilter:

    def __init__(self):
        self.gateway_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        
        self.id = int(ID)

        self.producer_lock = threading.Lock()
        # definicion de working queue exchanges de la instancia posterior
        self.usd_filters_q3_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, USD_FILTER_Q3_QUEUE
            )

        self.usd_filters_q4_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, USD_FILTER_Q4_QUEUE
            )
        self.pay_format_filter_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, PAY_FORMAT_FILTER_QUEUE
            )

        self._sigterm_received = False
        self._runtime_error = False

        self._stop_lock = threading.Lock()
        self._stopping = False
        self.deduplicator = InMemoryDeduplicator()

        self.eof_controller = EOFController(MOM_HOST, self.id, DATE_FILTER_PREFIX, DATE_FILTER_AMOUNT, EOF_CONTROL_EXCHANGE, EXPECTED_INPUT_EOFS,None,self.on_send_eof_to_next_stage_callback, None,AUXILIARY_INPUT)
    
    def _run_gateway_consumer(self):
        try:
            self.gateway_queue.start_consuming(self.process_gateway_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Gateway consumer crashed")

    
    def process_gateway_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.GATEWAY_TO_DATE_FILTER:
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
        timestamp = transaction_data.get("timestamp")

        if not re.match(r'^\d{4}/\d{2}/\d{2} \d{2}:\d{2}$', timestamp[:16]):
            logging.warning(f"Timestamp inválido para cliente {client_id}: {timestamp}")
            return
        
        (anio,mes,dia) = (timestamp[0:4], timestamp[5:7], timestamp[8:10])

        if anio == "2022" and mes == "09":
            if dia in ["01", "02", "03", "04", "05"]:

                with self.producer_lock:
                    self.usd_filters_q4_queue.send(
                        DateFilterMessageHandler.serialize_usd_filter_q4_message(client_id, data_id, transaction_data, message_id=message_id)
                    ) 
                    self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_2, client_id)
                
                with self.producer_lock:
                    self.pay_format_filter_queue.send(
                        DateFilterMessageHandler.serialize_pay_format_filter_message(client_id, data_id, transaction_data, message_id=message_id)
                    )
                    self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_3, client_id)

            elif dia in ["06", "07", "08", "09", "10", "11", "12", "13", "14", "15"]:
                with self.producer_lock:
                    self.usd_filters_q3_queue.send(
                        DateFilterMessageHandler.serialize_usd_filter_q3_message(client_id, data_id, transaction_data, message_id=message_id)
                    )
                self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_1, client_id)

    def on_send_eof_to_next_stage_callback(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        with self.producer_lock:
            self.usd_filters_q3_queue.send(EOFMessageHandler.serialize_eof_message(client_id, totals_by_output.get(OUTPUT_PREFIX_1, 0), origin_worker_prefix, amount_origin_workers))
            self.usd_filters_q4_queue.send(EOFMessageHandler.serialize_eof_message(client_id, totals_by_output.get(OUTPUT_PREFIX_2, 0), origin_worker_prefix, amount_origin_workers))
            self.pay_format_filter_queue.send(EOFMessageHandler.serialize_eof_message(client_id, totals_by_output.get(OUTPUT_PREFIX_3, 0), origin_worker_prefix, amount_origin_workers))
        logging.info(f"Sent final EOF for client {client_id} to all downstream queues")

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

        consumers = [self.gateway_queue]

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.gateway_queue]
        if self.usd_filters_q3_queue is not None:
            resources.append(self.usd_filters_q3_queue)
        if self.usd_filters_q4_queue is not None:
            resources.append(self.usd_filters_q4_queue)
        if self.pay_format_filter_queue is not None:
            resources.append(self.pay_format_filter_queue)

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
        name="date-filter-consumer-thread",
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
    date_filter = DateFilter()

    def _handle_sigterm(signum, frame):
        logging.debug("SIGTERM received in date filter")
        date_filter.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return date_filter.start()


if __name__ == "__main__":
    sys.exit(main())
