# src/entities/joiners/join_max_amount_per_bank/message_handler/message_handler.py
from common import message_protocol
from common.message_protocol.internal import InternalMessageType

class MessageHandler:
    @staticmethod
    def serialize_result(client_uuid, data_id, bank_name, account_origin, amount_received):
        data = {
            "bank_name": bank_name,
            "account_origin": account_origin,
            "amount_received": amount_received,
        }
        return message_protocol.internal.serialize(
            InternalMessageType.DATE_PER_BANK_REDUCER_TO_GATEWAY,
            client_uuid,
            data_id,
            data,
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
