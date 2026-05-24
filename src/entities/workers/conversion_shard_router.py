import logging
import os

from common import message_protocol
from common.conversions import conversion_key, conversion_shard
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

        key = conversion_key(
            payload.get(self.currency_field),
            payload.get(self.date_field),
        )
        shard = conversion_shard(key, self.total_workers)
        payload["conversion_key"] = key
        payload["conversion_shard"] = shard
        message["payload"] = payload

        LOGGER.info("Routing conversion. key=%s shard=%s", key, shard)
        return message, self._queue_name(shard)

    def _queue_name(self, shard):
        return f"{self.queue_prefix}_{shard}"
