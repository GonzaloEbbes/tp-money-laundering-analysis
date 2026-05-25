from common.entity import PipelineEntity

# TODO: Reemplazar por el client_id definitivo del mensaje cuando este mergeado.
DEFAULT_TEST_CLIENT_ID = "555555"


class DynamicAmountFilter(PipelineEntity):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.averages_by_client = {}
        self.average_eofs = set()

    def entity_type(self):
        return "dynamic_amount_filter"

    def process_message(self, message):
        payload = message.get("payload", {})
        if payload.get("query_id") != "query_3_avg_result":
            return None

        client_id = payload.get("client_id", DEFAULT_TEST_CLIENT_ID)

        if payload.get("type") == "EOF":
            self.average_eofs.add(client_id)
            return None

        # Se guarda en memoria; no hace falta persistir esta tabla a disco.
        self.averages_by_client[client_id] = payload.get("averages", {})
        return None
