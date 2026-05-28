from common import message_protocol
from common.entity import PipelineEntity


class MapMaxAmountPerBank(PipelineEntity):

    def entity_type(self):
        return "map_max_amount_per_bank"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bank_max = {}

    def process_message(self, message):
        payload = message.get("payload", {})

        if payload.get("type") == "EOF":
            print(f"Bank amount: {len(self.bank_max)}", flush=True)
            for bank, max_amount in self.bank_max.items():
                result = {"query_id": "query_2", "FromBank": bank, "MaxAmount": max_amount}
                msg = message.copy()
                msg["payload"] = result
                print(f"Sending result for bank {bank}: MaxAmount={max_amount}", flush=True)
                self.output_queue.send(message_protocol.serialize(msg),
                                       routing_key="join_max_amount_per_bank_queue")
            print("Finished sending all max amount results, sending EOF", flush=True)
            return message, "join_max_amount_per_bank_queue"

        bank = payload.get("FromBank")
        amount = float(payload.get("AmountReceived", 0))
        if bank and (bank not in self.bank_max or amount > self.bank_max[bank]):
            self.bank_max[bank] = amount
        return None
