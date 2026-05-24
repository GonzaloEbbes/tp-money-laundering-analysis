import logging
import os
from decimal import Decimal, InvalidOperation

from common.entity import PipelineEntity
from common.conversions import (
    ConversionRateProviderError,
    build_conversion_rate_provider,
    conversion_key,
    is_unsupported_by_frankfurter,
    StaticConversionRateProvider,
    to_frankfurter_currency,
)


LOGGER = logging.getLogger(__name__)


class CurrencyConverter(PipelineEntity):
    def __init__(self, mom_host, input_queue, output_queue=None):
        super().__init__(mom_host, input_queue, output_queue)
        self.provider = build_conversion_rate_provider()
        self.static_fallback_provider = _build_static_fallback_provider()
        self.cache = {}
        self.amount_field = os.environ.get("CONVERSION_AMOUNT_FIELD", "AmountPaid")
        self.currency_field = os.environ.get("CONVERSION_CURRENCY_FIELD", "PaymentCurrency")
        self.date_field = os.environ.get("CONVERSION_DATE_FIELD", "Timestamp")
        self.output_amount_field = os.environ.get(
            "CONVERSION_OUTPUT_AMOUNT_FIELD",
            "AmountPaidUSD",
        )

    def entity_type(self):
        return "currency_converter"

    def process_message(self, message):
        payload = message.get("payload", {})

        if payload.get("type") == "EOF":
            return message

        try:
            converted_payload = self._convert_payload(payload)
        except (ConversionRateProviderError, InvalidOperation, ValueError, TypeError) as error:
            LOGGER.exception("Currency conversion failed. payload=%s", payload)
            message["payload"] = {
                "status": "CONVERSION_ERROR",
                "query_id": payload.get("query_id"),
                "error": str(error),
            }
            return message

        message["payload"] = converted_payload
        return message

    def _convert_payload(self, payload):
        currency = to_frankfurter_currency(payload.get(self.currency_field))
        date = payload.get(self.date_field)
        amount = Decimal(str(payload.get(self.amount_field)))
        key = payload.get("conversion_key") or conversion_key(currency, date)

        rate = self.cache.get(key)
        if rate is None:
            rate = self._get_rate(currency, date)
            self.cache[key] = rate
            LOGGER.info("Conversion cache miss. key=%s rate=%s", key, rate)

        converted_payload = dict(payload)
        converted_payload["conversion_key"] = key
        converted_payload["conversion_rate_to_usd"] = str(rate)
        converted_payload[self.output_amount_field] = str(amount * rate)
        return converted_payload

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
