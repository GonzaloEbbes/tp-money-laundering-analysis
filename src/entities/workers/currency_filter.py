from common.entity import PipelineEntity

QUERY_ROUTING_CONFIG = {
    "query_1": {
        "target_currency": "US Dollar",
        "columns_out": [
            "AccountOrigin", 
            "AccountDestiny",
            "AmountReceived"
        ],
        "target_queue": "amount_filter_queue"
    },
    "query_2": {
        "target_currency": "US Dollar",
        "columns_out": ["FromBank", "AccountOrigin", "AmountReceived"],
        "target_queue": "data_per_bank_redirector_queue"
    }
}


class CurrencyFilter(PipelineEntity):
    def entity_type(self):
        return "currency_filter"

    def process_message(self, message):
        payload = message.get("payload", {})

        query_id = payload.get("query_id")
        config = QUERY_ROUTING_CONFIG.get(query_id)
        if not config:
            return None
        
        if payload.get("type") == "EOF":
            print(f"Received EOF for {query_id}, sending EOF to {config['target_queue']}", flush=True)
            return message, config.get("target_queue") if config else None

        target_currency = config.get("target_currency", "USD")
        payment_curr = payload.get("PaymentCurrency")
        receiving_curr = payload.get("ReceivingCurrency")

        if payment_curr == target_currency and receiving_curr == target_currency:
            new_payload = {col: payload.get(col) for col in config["columns_out"] if col in payload}
            new_payload["query_id"] = query_id
            message["payload"] = new_payload
            return message, config["target_queue"]

        message["payload"] = {"status": "FILTERED", "query_id": query_id}
        return message, config["target_queue"]
