import logging
import os
from decimal import Decimal, InvalidOperation

from common import message_protocol
from common.entity import DeprecatedToEliminateEntity
from common.middleware import MessageMiddlewareExchangeRabbitMQ
from common.conversions import (
    ConversionRateProviderError,
    build_conversion_rate_provider,
    conversion_key,
    is_unsupported_by_frankfurter,
    StaticConversionRateProvider,
    to_frankfurter_currency,
)


LOGGER = logging.getLogger(__name__)


class CurrencyConverter(DeprecatedToEliminateEntity):
    def __init__(self, mom_host, input_queue, output_queue=None):
        super().__init__(mom_host, input_queue, output_queue)
        input_exchange = os.environ.get("CONVERSION_INPUT_EXCHANGE")
        routing_key = os.environ.get("CONVERSION_ROUTING_KEY")
        if input_exchange and routing_key:
            self.input_queue.close()
            self.input_queue = MessageMiddlewareExchangeRabbitMQ(
                mom_host,
                input_exchange,
                [routing_key],
                queue_name=input_queue,
                exclusive=False,
            )
        LOGGER.info(
            "CurrencyConverter wiring: input_queue=%s output_queue=%s input_exchange=%s routing_key=%s",
            input_queue,
            output_queue,
            input_exchange,
            routing_key,
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

    def entity_type(self):
        return "currency_converter"

    def process_message(self, message):
        if message.type == message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
            return message

        if (
            message.type
            != message_protocol.internal.InternalMessageType.PAY_FORMAT_FILTER_TO_USD_CURRENCY_CONVERTER
        ):
            return None

        payload = message.data or {}

        try:
            converted_payload = self._convert_payload(payload)
        except (ConversionRateProviderError, InvalidOperation, ValueError, TypeError) as error:
            LOGGER.exception("Currency conversion failed. payload=%s", payload)
            return None

        output_payload = {
            self.output_amount_field: converted_payload.get(self.output_amount_field),
        }
        return message_protocol.internal.InternalMessage(
            type=message_protocol.internal.InternalMessageType.USD_CURRENCY_CONVERTER_TO_AMOUNT_FILTER_Q5,
            source_client_uuid=message.source_client_uuid,
            data_id=message.data_id,
            data=message_protocol.internal.TransactionData(output_payload),
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
