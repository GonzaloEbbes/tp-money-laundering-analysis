from common import message_protocol
from common.message_protocol.internal import InternalMessageType

class MessageHandler:
    @staticmethod
    def serialize_redirect(client_uuid, data_id, from_bank, account_origin, amount_received, message_id=None):
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
            message_id=message_id,
        )

    @staticmethod
    def serialize_eof_message(client_uuid):
        return message_protocol.internal.serialize(
            InternalMessageType.EOF_GENERIC_MESSAGE,
            client_uuid,
            None,
            None,
        )

    @staticmethod
    def serialize_eof_leader_message(client_uuid):
        return message_protocol.internal.serialize(
            InternalMessageType.EOF_LEADER_MESSAGE,
            client_uuid,
            None,
            None,
        )
