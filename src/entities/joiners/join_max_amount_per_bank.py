import os
from common import message_protocol
from common.entity import PipelineEntity

TOTAL_DEDUPLICATORS = int(os.environ.get("TOTAL_DEDUPLICATORS", 1))
TOTAL_REDUCERS = int(os.environ.get("TOTAL_REDUCERS", 1))

class JoinMaxAmountPerBank(PipelineEntity):
    def entity_type(self):
        return "join_max_amount_per_bank"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bank_cache = {}
        self.accounts_eof_count = 0
        self.results_eof_count = 0
        self.cache_ready = False
        self.pending_messages = []

    def _normalize_id(self, raw_id):
        if raw_id is None:
            return ""
        val = str(raw_id).strip()
        if val.isdigit():
            return str(int(val))
        return val

    def process_message(self, message):
        payload = message.get("payload", {})
        query_id = payload.get("query_id")

        if query_id == "query_2_accounts":

            # Handle EOF for accounts
            if payload.get("type") == "EOF":
                print("Received EOF for query_2_accounts", flush=True)
                self.accounts_eof_count += 1
                if self.accounts_eof_count >= TOTAL_DEDUPLICATORS:
                    self.cache_ready = True
                    self._process_pending_messages()
                return None

            #Handle bank data
            bank_id = self._normalize_id(payload.get("BankID"))
            bank_name = str(payload.get("BankName", "")).strip()

            if bank_id:
                self.bank_cache[bank_id] = bank_name
            return None

        if query_id == "query_2":

            if not self.cache_ready:
                self.pending_messages.append(message)
                return None

            return self._process_query_2(message)

    def _process_pending_messages(self):
        for msg in self.pending_messages:
            result_msg, target_queue = self._process_query_2(msg)
            if result_msg and target_queue:
                self.output_queue.send(message_protocol.serialize(result_msg),
                                       routing_key=target_queue)
        self.pending_messages.clear()

    def _process_query_2(self, message):
        payload = message.get("payload", {})

        if payload.get("type") == "EOF":
            self.results_eof_count += 1
            if self.results_eof_count >= TOTAL_REDUCERS:
                return message, "gateway_results_queue"
            return None, None
        from_bank = self._normalize_id(payload.get("FromBank"))

        payload["BankName"] = self.bank_cache.get(from_bank, "Unknown")
        message["payload"] = payload
        return message, "gateway_results_queue"
