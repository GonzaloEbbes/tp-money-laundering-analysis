import os
import logging
import signal
import sys
import threading
from time import sleep
from decimal import Decimal, InvalidOperation
from common import middleware, message_protocol
from common.controllers.eof_controller.EOF_controller import EOFController
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler
from common.dedup import InMemoryDeduplicator
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ
from message_handler import MessageHandler as CurrencyConverterMessageHandler
from common.logging import configure_logging_from_env
from common.conversions import (
    ConversionRateProviderError,
    build_conversion_rate_provider,
    conversion_key,
    is_unsupported_by_frankfurter,
    StaticConversionRateProvider,
    to_frankfurter_currency,
)


ID = os.environ["ID"]
MOM_HOST = os.environ["MOM_HOST"]
CONVERSION_INPUT_EXCHANGE = os.environ["CONVERSION_INPUT_EXCHANGE"]
CONVERSION_ROUTING_KEY = os.environ["CONVERSION_ROUTING_KEY"]
CURRENCY_CONVERTER_PREFIX = os.environ["CURRENCY_CONVERTER_PREFIX"]
CURRENCY_CONVERTER_AMOUNT = int(os.environ["CURRENCY_CONVERTER_AMOUNT"])
EOF_CONTROL_EXCHANGE = os.environ["EOF_CONTROL_EXCHANGE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"] #al amount filter q5
EXPECTED_INPUT_EOFS = int(os.environ["EXPECTED_INPUT_EOFS"]) #1
INPUT_PREFIX_1 = os.environ["INPUT_PREFIX_1"] #que es el prefix del pay format filter
AUXILIARY_INPUT = os.environ["AUXILIARY_INPUT"] == "true"
OUTPUT_PREFIX_1 = os.environ["OUTPUT_PREFIX_1"] #amount filter q5
CONVERSION_AMOUNT_FIELD = os.environ.get("CONVERSION_AMOUNT_FIELD", "amount_paid")
CONVERSION_CURRENCY_FIELD = os.environ.get("CONVERSION_CURRENCY_FIELD", "payment_currency")
CONVERSION_DATE_FIELD = os.environ.get("CONVERSION_DATE_FIELD", "timestamp")
CONVERSION_OUTPUT_AMOUNT_FIELD = os.environ.get("CONVERSION_OUTPUT_AMOUNT_FIELD","amount_paid")
STATIC_CONVERSION_RATES_PATH = os.environ.get("STATIC_CONVERSION_RATES_PATH")
CURRENCY_CONVERTER_QUEUE = CURRENCY_CONVERTER_PREFIX+"_queue_"+str(ID)

class CurrencyConverter:

    def __init__(self):

        self.pay_format_filter_exchange = MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                CONVERSION_INPUT_EXCHANGE,
                [CONVERSION_ROUTING_KEY],
                queue_name=CURRENCY_CONVERTER_QUEUE,
                exclusive=False,
            )
        self.id = int(ID)
        self.producer_lock = threading.Lock()
        # definicion de working queue exchanges de la instancia posterior
        self.amount_filter_q5_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE
            )

        self.provider = build_conversion_rate_provider()
        self.static_fallback_provider = self._build_static_fallback_provider()
        self.cache = {}
        self.amount_field = CONVERSION_AMOUNT_FIELD
        self.currency_field = CONVERSION_CURRENCY_FIELD
        self.date_field = CONVERSION_DATE_FIELD
        self.output_amount_field = CONVERSION_OUTPUT_AMOUNT_FIELD

        self._sigterm_received = False
        self._runtime_error = False
        self._stop_lock = threading.Lock()
        self._stopping = False
        self.deduplicator = InMemoryDeduplicator()
        self.eof_controller = EOFController(MOM_HOST, self.id, CURRENCY_CONVERTER_PREFIX, CURRENCY_CONVERTER_AMOUNT, EOF_CONTROL_EXCHANGE, EXPECTED_INPUT_EOFS,None,self.on_send_eof_to_next_stage_callback, None, AUXILIARY_INPUT)


    def _run_pay_format_filter_consumer(self):
        try:
            self.pay_format_filter_exchange.start_consuming(self.process_usd_filter_q1q2_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "USD filter Q1Q2 consumer crashed")


    def process_usd_filter_q1q2_messages(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.PAY_FORMAT_FILTER_TO_USD_CURRENCY_CONVERTER:
                if not self._should_process_message(message):
                    ack()
                    return
                client_id = message.source_client_uuid
                self._process_transaction(message.data, client_id, message.data_id)
                self.eof_controller.on_processed_packet_by_client(client_id, INPUT_PREFIX_1)
                self.deduplicator.mark_processed(client_id, self._dedup_key(message))
            case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                client_id = message.source_client_uuid
                self.eof_controller.on_input_queue_eof_reception(client_id, message.data)
        ack()



    def _process_transaction(self, transaction_data, client_id, data_id):
        logging.debug(f"Received PAY_FORMAT_FILTER_TO_USD_CURRENCY_CONVERTER for client {client_id}")

        try:
            converted_payload = self._convert_payload(transaction_data)
        except (ConversionRateProviderError, InvalidOperation, ValueError, TypeError) as error:
            logging.exception("Currency conversion failed. payload=%s", transaction_data)
            return None

        with self.producer_lock:
            self.amount_filter_q5_queue.send(CurrencyConverterMessageHandler.serialize_amount_filter_q5_message(client_id, data_id, converted_payload,self.output_amount_field))
            self.eof_controller.on_packet_sent_by_client_to(OUTPUT_PREFIX_1, client_id)
            logging.debug(f"Transaction for client {client_id} sent to amount filter q5 queue. Converted payload: {converted_payload}")

    def _convert_payload(self, payload):
        currency = to_frankfurter_currency(
            self._payload_get(payload, self.currency_field, "payment_currency", "PaymentCurrency")
        )
        date = self._payload_get(payload, self.date_field, "timestamp", "Timestamp")
        amount = Decimal(str(self._payload_get(payload, self.amount_field, "amount_paid", "AmountPaid")))
        key = payload.get("conversion_key") or conversion_key(currency, date)

        rate = self.cache.get(key)
        if rate is None:
            rate = self._get_rate(currency, date)
            self.cache[key] = rate
            logging.debug("Conversion cache miss. key=%s rate=%s", key, rate)

        converted_payload = dict(payload)
        converted_payload["conversion_key"] = key
        converted_payload["conversion_rate_to_usd"] = str(rate)
        converted_payload[self.output_amount_field] = str(amount * rate)
        return converted_payload

    def _payload_get(self, payload, *field_names):
        for field_name in field_names:
            if field_name in payload:
                return payload[field_name]
        return None

    def _get_rate(self, currency, date):
        if is_unsupported_by_frankfurter(currency):
            if not self.static_fallback_provider:
                raise ConversionRateProviderError(
                    f"Currency is not supported by Frankfurter and no static fallback is configured: {currency}"
                )
            return self.static_fallback_provider.get_rate_to_usd(currency, date)
        return self.provider.get_rate_to_usd(currency, date)



    def on_send_eof_to_next_stage_callback(self, client_id, totals_by_output, origin_worker_prefix, amount_origin_workers):
        with self.producer_lock:
            self.amount_filter_q5_queue.send(EOFMessageHandler.serialize_eof_message(client_id, totals_by_output.get(OUTPUT_PREFIX_1, 0), origin_worker_prefix, amount_origin_workers))
        logging.info(f"Sent final EOF for client {client_id} to amount filter q5")

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

        consumers = [self.pay_format_filter_exchange]

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [self.pay_format_filter_exchange]
        if self.amount_filter_q5_queue is not None:
            resources.append(self.amount_filter_q5_queue)

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
        target=self._run_pay_format_filter_consumer,
        name="pay-format-filter-consumer-thread",
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

    @staticmethod
    def _build_static_fallback_provider():
        rates_path = STATIC_CONVERSION_RATES_PATH
        if not rates_path:
            return StaticConversionRateProvider()

        try:
            return build_conversion_rate_provider(name="static")
        except ConversionRateProviderError:
            logging.exception(
                "Could not initialize configured static conversion fallback. Using built-in static rates only."
            )
            return StaticConversionRateProvider()


def main():
    configure_logging_from_env()
    currency_converter = CurrencyConverter()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in currency converter")
        currency_converter.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return currency_converter.start()


if __name__ == "__main__":
    sys.exit(main())
