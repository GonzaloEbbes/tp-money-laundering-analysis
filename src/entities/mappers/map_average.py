import json
import threading
from collections import defaultdict

from common.entity import PipelineEntity

# TODO: Reemplazar por el client_id definitivo del mensaje cuando este mergeado.
DEFAULT_TEST_CLIENT_ID = "555555"


class MapAverage(PipelineEntity):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.averages = defaultdict(lambda: defaultdict(lambda: {"sum_total": 0.0, "count": 0}))
        self.flushed_clients = set()
        self.lock = threading.Lock()

    def entity_type(self):
        return "map_average"

    def process_message(self, message):
        payload = message.get("payload", {})
        client_id = payload.get("client_id", DEFAULT_TEST_CLIENT_ID)

        if payload.get("type") == "EOF":
            return self._flush_client(client_id, message)

        if payload.get("query_id") != "query_3_avg":
            return None

        payment_format = payload.get("PaymentFormat")
        if not client_id or not payment_format:
            return None

        try:
            amount = float(payload.get("AmountReceived", 0))
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
            partial_msg = base_message.copy()
            partial_msg["payload"] = {
                "query_id": "query_3_avg_partial",
                "client_id": client_id,
                "PaymentFormat": payment_format,
                "sum_total": values["sum_total"],
                "count": values["count"],
            }
            self.output_queue.send(json.dumps(partial_msg).encode("utf-8"))

        eof_msg = base_message.copy()
        eof_msg["payload"] = {
            "type": "EOF",
            "query_id": "query_3_avg_partial",
            "client_id": client_id,
        }
        self.output_queue.send(json.dumps(eof_msg).encode("utf-8"))
        return None
