from common.entity import PipelineEntity

CONFIG = {
    "query_1": {
        "filter_amount": 50,
        "target_queue": "gateway_results_queue"
    },
}



class AmountFilter(PipelineEntity):
    def entity_type(self):
        return "amount_filter"

    def process_message(self, message):
        payload = message.get("payload", {})
        query_id = payload.get("query_id")
        config = CONFIG.get(query_id)
        if not config:
            return None
        
        if payload.get("type") == "EOF":
            print(f"Received EOF for {query_id}, sending EOF to {config['target_queue']}", flush=True)
            return message, config.get("target_queue") if config else None

        try:
            amount_received = float(payload.get("AmountReceived", 0))
        except (ValueError, TypeError):
            amount_received = 999999.0
        if amount_received is not None and amount_received > config["filter_amount"]:
            message["payload"] = {"status": "FILTERED", "query_id": query_id}
        else:
            message["payload"] = {k: v for k, v in payload.items() if k != "AmountReceived"}
            message["payload"]["query_id"] = query_id

        return message, config["target_queue"]
