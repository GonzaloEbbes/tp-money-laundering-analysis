import logging
import os

from common import message_protocol
from common.conversions import (
    ConversionRateProviderError,
    conversion_key,
    conversion_shard,
    to_frankfurter_currency,
)
from common.entity import PipelineEntity


LOGGER = logging.getLogger(__name__)


class ConversionShardRouter(PipelineEntity):
    def __init__(self, mom_host, input_queue, output_queue=None):
        super().__init__(mom_host, input_queue, output_queue)
        self.total_workers = int(os.environ.get("TOTAL_CONVERSION_WORKERS", "1"))
        self.queue_prefix = os.environ.get(
            "CONVERSION_CONVERTER_QUEUE_PREFIX",
            "currency_converter_queue",
        )
        self.error_queue = os.environ.get("CONVERSION_ERROR_QUEUE", "gateway_results_queue")
        self.currency_field = os.environ.get("CONVERSION_CURRENCY_FIELD", "PaymentCurrency")
        self.date_field = os.environ.get("CONVERSION_DATE_FIELD", "Timestamp")

    def entity_type(self):
        return "conversion_shard_router"

    def process_message(self, message):
        payload = message.get("payload", {})

        if payload.get("type") == "EOF":
            for shard in range(self.total_workers):
                self.output_queue.send(
                    message_protocol.serialize(message.copy()),
                    routing_key=self._queue_name(shard),
                )
            return None

        try:
            currency = to_frankfurter_currency(payload.get(self.currency_field))
            key = conversion_key(currency, payload.get(self.date_field))
        except ConversionRateProviderError as error:
            LOGGER.exception("Conversion routing failed. payload=%s", payload)
            message["payload"] = {
                "status": "CONVERSION_ERROR",
                "query_id": payload.get("query_id"),
                "error": str(error),
            }
            return message, self.error_queue

        shard = conversion_shard(key, self.total_workers)
        payload[self.currency_field] = currency
        payload["conversion_key"] = key
        payload["conversion_shard"] = shard
        message["payload"] = payload

        if currency != "USD":
            LOGGER.info("Routing conversion. key=%s shard=%s", key, shard)
        return message, self._queue_name(shard)

    def _queue_name(self, shard):
        return f"{self.queue_prefix}_{shard}"
