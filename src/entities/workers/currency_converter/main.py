import logging
import os
import signal
from decimal import Decimal, InvalidOperation

from common import message_protocol, middleware
from common.conversions import (
    ConversionRateProviderError,
    StaticConversionRateProvider,
    build_conversion_rate_provider,
    conversion_key,
    is_unsupported_by_frankfurter,
    to_frankfurter_currency,
)
from common.logging.logging_config import configure_logging_from_env


LOGGER = logging.getLogger(__name__)

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
CONVERSION_INPUT_EXCHANGE = os.environ["CONVERSION_INPUT_EXCHANGE"]
CONVERSION_ROUTING_KEY = os.environ["CONVERSION_ROUTING_KEY"]


class CurrencyConverter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            CONVERSION_INPUT_EXCHANGE,
            [CONVERSION_ROUTING_KEY],
            queue_name=INPUT_QUEUE,
            exclusive=False,
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST,
            OUTPUT_QUEUE,
        )
        LOGGER.info(
            "CurrencyConverter wiring: input_queue=%s output_queue=%s input_exchange=%s routing_key=%s",
            INPUT_QUEUE,
            OUTPUT_QUEUE,
            CONVERSION_INPUT_EXCHANGE,
            CONVERSION_ROUTING_KEY,
        )

        self.provider = build_conversion_rate_provider()
        self.static_fallback_provider = _build_static_fallback_provider()
        self.cache = {}
        self.amount_field = os.environ.get("CONVERSION_AMOUNT_FIELD", "amount_paid")
        self.currency_field = os.environ.get("CONVERSION_CURRENCY_FIELD", "payment_currency")
        self.date_field = os.environ.get("CONVERSION_DATE_FIELD", "timestamp")
        self.output_amount_field = os.environ.get(
            "CONVERSION_OUTPUT_AMOUNT_FIELD",
            "amount_paid",
        )
        self._sigterm_received = False
        self._runtime_error = False
        self._stopping = False

    def process_messages(self, raw_message, ack, nack):
        try:
            message = message_protocol.internal.deserialize(raw_message)

            match message.type:
                case message_protocol.internal.InternalMessageType.PAY_FORMAT_FILTER_TO_USD_CURRENCY_CONVERTER:
                    self._process_transaction(message)
                case message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                    self._send_eof(message)
                case _:
                    LOGGER.warning("Ignoring unexpected message type: %s", message.type)

            ack()
        except Exception as error:
            LOGGER.exception("Currency converter failed while processing message")
            self._handle_runtime_failure(error, "Currency converter consumer crashed")
            nack()

    def _process_transaction(self, message):
        payload = message.data or {}
        try:
            converted_payload = self._convert_payload(payload)
        except (ConversionRateProviderError, InvalidOperation, ValueError, TypeError):
            LOGGER.exception("Currency conversion failed. payload=%s", payload)
            return

        output_payload = {
            self.output_amount_field: converted_payload.get(self.output_amount_field),
        }
        self.output_queue.send(
            message_protocol.internal.serialize(
                message_protocol.internal.InternalMessageType.USD_CURRENCY_CONVERTER_TO_AMOUNT_FILTER_Q5,
                message.source_client_uuid,
                message.data_id,
                message_protocol.internal.TransactionData(output_payload),
            )
        )

    def _send_eof(self, message):
        self.output_queue.send(
            message_protocol.internal.serialize(
                message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE,
                message.source_client_uuid,
                message.data_id,
                message.data,
            )
        )

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
            LOGGER.debug("Conversion cache miss. key=%s rate=%s", key, rate)

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

    def stop(self):
        if self._stopping:
            return
        self._stopping = True

        try:
            self.input_queue.stop_consuming()
        except Exception as error:
            LOGGER.error("Error stopping currency converter consumer: %s", error)

    def _close_resources(self):
        for resource in [self.input_queue, self.output_queue]:
            try:
                resource.close()
            except Exception as error:
                LOGGER.error("Error closing currency converter resource: %s", error)

    def notify_sigterm(self):
        self._sigterm_received = True
        self.stop()

    def _handle_runtime_failure(self, error, context):
        LOGGER.error("%s: %s", context, error)
        self._runtime_error = True
        self.stop()

    def start(self):
        try:
            self.input_queue.start_consuming(self.process_messages)
        except Exception as error:
            LOGGER.error("Currency converter stopped with error: %s", error)
            self._runtime_error = True
            self.stop()
            self._close_resources()
            return 2

        self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return 1

        return 0


def _build_static_fallback_provider():
    rates_path = os.environ.get("STATIC_CONVERSION_RATES_PATH")
    if not rates_path:
        return StaticConversionRateProvider()

    try:
        return build_conversion_rate_provider(name="static")
    except ConversionRateProviderError:
        LOGGER.exception(
            "Could not initialize configured static conversion fallback. Using built-in static rates only."
        )
        return StaticConversionRateProvider()


def main():
    configure_logging_from_env()
    currency_converter = CurrencyConverter()

    def _handle_sigterm(signum, frame):
        LOGGER.info("SIGTERM received in currency converter")
        currency_converter.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return currency_converter.start()


if __name__ == "__main__":
    raise SystemExit(main())
