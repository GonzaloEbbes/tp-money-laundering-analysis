import uuid
import threading
from collections import defaultdict

from common import message_protocol
from common.entity import PipelineEntity


class MapAverage(PipelineEntity):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.averages = defaultdict(lambda: defaultdict(lambda: {"sum_total": 0.0, "count": 0}))
        self.flushed_clients = set()
        self.lock = threading.Lock()

    def entity_type(self):
        return "map_average"

    def process_message(self, message):
        client_id = message.source_client_uuid

        if message.type == message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
            return self._flush_client(client_id, message)

        if (
            message.type
            != message_protocol.internal.InternalMessageType.USD_FILTER_Q4_TO_AVERAGE_PER_PAY_FORMAT_MAPPER
        ):
            return None

        payload = message.data or {}
        payment_format = payload.get("payment_format")
        if not client_id or not payment_format:
            return None

        try:
            amount = float(payload.get("amount_received", 0))
        except (TypeError, ValueError):
            return None

        with self.lock:
            if client_id in self.flushed_clients:
                return None
            values = self.averages[client_id][payment_format]
            values["sum_total"] += amount
            values["count"] += 1
        return None

    def _flush_client(self, client_id, base_message):
        if not client_id:
            return None

        with self.lock:
            if client_id in self.flushed_clients:
                return None
            client_state = self.averages.pop(client_id, {})
            self.flushed_clients.add(client_id)

        for payment_format, values in client_state.items():
            partial_payload = message_protocol.internal.TransactionData({
                "PaymentFormat": payment_format,
                "sum_total": values["sum_total"],
                "count": values["count"],
            })
            self.output_queue.send(
                message_protocol.internal.serialize(
                    message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_MAPPER_TO_AVERAGE_PER_PAY_FORMAT_AGGREGATOR,
                    client_id,
                    str(uuid.uuid4()),
                    partial_payload,
                )
            )

        self.output_queue.send(
            message_protocol.internal.serialize(
                message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE,
                client_id,
                base_message.data_id,
                None,
            )
        )
        return None
