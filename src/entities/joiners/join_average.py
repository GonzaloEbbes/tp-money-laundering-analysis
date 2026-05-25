import json
import os
from collections import defaultdict

from common.entity import PipelineEntity

TOTAL_AVERAGE_MAPPERS = int(os.environ.get("TOTAL_AVERAGE_MAPPERS", 1))

# TODO: Reemplazar por el client_id definitivo del mensaje cuando este mergeado.
DEFAULT_TEST_CLIENT_ID = "555555"


class JoinAverage(PipelineEntity):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.averages = defaultdict(lambda: defaultdict(lambda: {"sum_total": 0.0, "count": 0}))
        self.eof_counts = defaultdict(int)

    def entity_type(self):
        return "join_average"

    def process_message(self, message):
        payload = message.get("payload", {})
        if payload.get("query_id") != "query_3_avg_partial":
            return None

        client_id = payload.get("client_id", DEFAULT_TEST_CLIENT_ID)

        if payload.get("type") == "EOF":
            self.eof_counts[client_id] += 1
            if self.eof_counts[client_id] < TOTAL_AVERAGE_MAPPERS:
                return None

            averages = self._build_average_payload(client_id)
            result_msg = message.copy()
            result_msg["payload"] = {
                "query_id": "query_3_avg_result",
                "client_id": client_id,
                "averages": averages,
            }
            self.output_queue.send(json.dumps(result_msg).encode("utf-8"))

            eof_msg = message.copy()
            eof_msg["payload"] = {
                "type": "EOF",
                "query_id": "query_3_avg_result",
                "client_id": client_id,
            }

            self.averages.pop(client_id, None)
            self.eof_counts.pop(client_id, None)
            self.output_queue.send(json.dumps(eof_msg).encode("utf-8"))
            return None

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
