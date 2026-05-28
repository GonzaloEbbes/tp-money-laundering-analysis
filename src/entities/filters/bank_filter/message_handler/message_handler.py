from common import message_protocol
from common.message_protocol.internal import InternalMessageType

class MessageHandler:
    @staticmethod
    def serialize_join_message(client_uuid, data_id, bank_id, bank_name):
        data = {"bank_id": bank_id, "bank_name": bank_name}
        return message_protocol.internal.serialize(
            InternalMessageType.BANK_FILTER_TO_JOINER,
            client_uuid, data_id, data
        )
    
    @staticmethod
    def serialize_eof_join_message(client_uuid):
        return message_protocol.internal.serialize(
            InternalMessageType.BANK_FILTER_TO_JOINER,
            client_uuid,
            None,
            None
        )

    @staticmethod
    def serialize_eof_message(client_uuid):
        return message_protocol.internal.serialize(
            InternalMessageType.EOF_GENERIC_MESSAGE,
            client_uuid, None, None
        )

    @staticmethod
    def serialize_eof_leader_message(client_uuid):
        return message_protocol.internal.serialize(
            InternalMessageType.EOF_LEADER_MESSAGE,
            client_uuid, None, None
        )
