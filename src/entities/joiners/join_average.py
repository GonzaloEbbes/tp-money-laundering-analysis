import os
import uuid
from collections import defaultdict

from common import message_protocol
from common.entity import PipelineEntity

TOTAL_AVERAGE_MAPPERS = int(os.environ.get("TOTAL_AVERAGE_MAPPERS", 1))


class JoinAverage(PipelineEntity):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.averages = defaultdict(lambda: defaultdict(lambda: {"sum_total": 0.0, "count": 0}))
        self.eof_counts = defaultdict(int)

    def entity_type(self):
        return "join_average"

    def process_message(self, message):
        if message.type not in (
            message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_MAPPER_TO_AVERAGE_PER_PAY_FORMAT_AGGREGATOR,
            message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE,
        ):
            return None

        client_id = message.source_client_uuid

        if message.type == message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
            self.eof_counts[client_id] += 1
            if self.eof_counts[client_id] < TOTAL_AVERAGE_MAPPERS:
                return None

            averages = self._build_average_payload(client_id)
            result_payload = message_protocol.internal.TransactionData({
                "averages": averages,
            })
            self.output_queue.send(
                message_protocol.internal.serialize(
                    message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_AGGREGATOR_TO_AMOUNT_FILTER_Q3,
                    client_id,
                    str(uuid.uuid4()),
                    result_payload,
                )
            )

            self.averages.pop(client_id, None)
            self.eof_counts.pop(client_id, None)
            self.output_queue.send(
                message_protocol.internal.serialize(
                    message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE,
                    client_id,
                    message.data_id,
                    None,
                )
            )
            return None

        payload = message.data or {}
        payment_format = payload.get("PaymentFormat")
        if not payment_format:
            return None

        try:
            sum_total = float(payload.get("sum_total", 0))
            count = int(payload.get("count", 0))
        except (TypeError, ValueError):
            return None

        values = self.averages[client_id][payment_format]
        values["sum_total"] += sum_total
        values["count"] += count
        return None

    def _build_average_payload(self, client_id):
        result = {}
        for payment_format, values in self.averages[client_id].items():
            count = values["count"]
            if count <= 0:
                continue
            sum_total = values["sum_total"]
            result[payment_format] = {
                "sum_total": sum_total,
                "count": count,
                "average": sum_total / count,
            }
        return result
