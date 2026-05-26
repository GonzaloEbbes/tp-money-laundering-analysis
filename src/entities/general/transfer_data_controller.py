import os
from common import message_protocol
from common.entity import PipelineEntity

TOTAL_DEDUPLICATORS = int(os.environ.get("TOTAL_DEDUPLICATORS", 1))

QUERY_ROUTING_CONFIG = {
    "query_1": {
        "columns": [
            "AccountOrigin", 
            "AccountDestiny", 
            "AmountReceived", 
            "ReceivingCurrency", 
            "PaymentCurrency"
        ],
        "target_queue": "currency_filter_queue"
    },
    "query_2": {
        "columns": [
            "FromBank", 
            "AccountOrigin", 
            "AmountReceived", 
            "ReceivingCurrency", 
            "PaymentCurrency"
        ],
        "target_queue": "currency_filter_queue"
    },
    "query_2_accounts": {
        "columns": [
            "BankID", 
            "BankName"
            ],
        "target_queue": "bank_deduplicator_queue" 
    },
}

class TransferDataController(PipelineEntity):
    def entity_type(self):
        return "transfer_data_controller"

    def process_message(self, message):
        payload = message.get("payload", {})
        query_id = payload.get("query_id")

        if query_id == "query_2_accounts":
            if payload.get("type") == "EOF":
                print(f"Clonando EOF a {TOTAL_DEDUPLICATORS} deduplicadores...", flush=True)
                for i in range(TOTAL_DEDUPLICATORS):
                    msg_copy = message.copy()
                    self.output_queue.send(
                        message_protocol.serialize(msg_copy),
                        routing_key=f"bank_deduplicator_queue_{i}"
                    )
                return None

            bank_id = payload.get("BankID")
            if not bank_id:
                return None

            config = QUERY_ROUTING_CONFIG.get(query_id)
            filtered_data = {col: payload.get(col) for col in config['columns'] if col in payload}
            filtered_data["query_id"] = query_id
            message["payload"] = filtered_data

            partition = hash(bank_id) % TOTAL_DEDUPLICATORS
            target_queue = f"bank_deduplicator_queue_{partition}"

            return message, target_queue

        elif query_id == 'transactions':
            queries_to_spawn = ["query_1", "query_2"]

            if payload.get("type") == "EOF":
                for q_id in queries_to_spawn:
                    config = QUERY_ROUTING_CONFIG.get(q_id)
                    msg_copy = message.copy()

                    msg_copy["payload"] = {"type": "EOF", "query_id": q_id}

                    self.output_queue.send(message_protocol.serialize(msg_copy), 
                                           routing_key=config["target_queue"])
                    print(f"Clonando EOF {q_id} en {config['target_queue']}", flush=True)
                return None

            for q_id in queries_to_spawn:
                config = QUERY_ROUTING_CONFIG.get(q_id)
                msg_copy = message.copy()
                filtered_data = {col: payload.get(col) for col in config['columns'] if col in payload}
                filtered_data["query_id"] = q_id
                msg_copy["payload"] = filtered_data
                self.output_queue.send(message_protocol.serialize(msg_copy), routing_key=config["target_queue"])
            
            return None
        return None
