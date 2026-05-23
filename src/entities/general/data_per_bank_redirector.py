import os
from common import message_protocol
from common.entity import PipelineEntity

TOTAL_REDUCERS = int(os.environ.get("TOTAL_REDUCERS", 1))

class DataPerBankRedirector(PipelineEntity):
    def entity_type(self):
        return "data_per_bank_redirector"

    def process_message(self, message):
        payload = message.get("payload", {})

        if payload.get("type") == "EOF":
            if TOTAL_REDUCERS > 1:
                for i in range(TOTAL_REDUCERS):
                    msg_copy = message.copy()
                    target_queue = f"map_max_amount_per_bank_queue_{i}"
                    self.output_queue.send(message_protocol.serialize(msg_copy), 
                                           routing_key=target_queue)
                return None
            return message, "map_max_amount_per_bank_queue"

        bank = payload.get("FromBank")
        if not bank:
            return None
        if TOTAL_REDUCERS > 1:
            partition_id = hash(bank) % TOTAL_REDUCERS
            target_queue = "map_max_amount_per_bank_queue_" + str(partition_id)
            return message, target_queue
        return message, "map_max_amount_per_bank_queue"
