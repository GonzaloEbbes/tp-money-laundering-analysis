from common import message_protocol
from common.message_protocol.internal import InternalMessageType

class MessageHandler:
    @staticmethod
    def serialize_redirect(client_uuid, data_id, from_bank, account_origin, amount_received):
        data = {
            "from_bank": from_bank,
            "account_origin": account_origin,
            "amount_received": amount_received,
        }
        return message_protocol.internal.serialize(
            InternalMessageType.DATA_PER_BANK_SHUFFLER_TO_MAP_MAX_AMOUNT_PER_BANK,
            client_uuid,
            data_id,
            data,
        )