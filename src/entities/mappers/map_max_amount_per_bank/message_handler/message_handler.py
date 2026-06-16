# src/entities/filters/map_max_amount_per_bank/message_handler/message_handler.py
from common import message_protocol
from common.message_protocol.internal import InternalMessageType

class MessageHandler:
    @staticmethod
    def serialize_result(client_uuid, data_id, from_bank, amount_received, account_origin):
        data = {
            "from_bank": from_bank,
            "amount_received": amount_received,
            "account_origin": account_origin,
        }
        return message_protocol.internal.serialize(
            InternalMessageType.MAX_AMOUNT_PER_BANK_RESULT,
            client_uuid,
            data_id,
            data,
        )
    
    @staticmethod
    def serialize_eof_join_message(client_uuid):
        return message_protocol.internal.serialize(
            InternalMessageType.MAX_AMOUNT_PER_BANK_RESULT,
            client_uuid,
            None,
            None,
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
