from common.entity import PipelineEntity

class BankDeduplicator(PipelineEntity):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_banks = set()

    def entity_type(self):
        return "bank_deduplicator"

    def process_message(self, message):
        payload = message.get("payload", {})

        if payload.get("type") == "EOF":
            self.seen_banks.clear()
            print("Received EOF for bank_deduplicator, sending EOF", flush=True)
            return message, "join_max_amount_per_bank_queue"

        bank_id = payload.get("BankID")
        if not bank_id or bank_id in self.seen_banks:
            return None

        self.seen_banks.add(bank_id)
        return message, "join_max_amount_per_bank_queue"
