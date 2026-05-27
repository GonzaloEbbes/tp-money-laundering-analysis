import uuid

from common import message_protocol
from common.message_protocol.internal import InternalMessage, ScatherGatherData, TransactionData


class MessageHandler:

    def _serialize_scather_gather_final_message(client : str, scather_gather_accounts: list[str]):
        message_id = str(uuid.uuid4())
        parsedMessage = ScatherGatherData()
        parsedMessage.type = "FINAL"
        parsedMessage.value = scather_gather_accounts
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.SCATHER_GATHER_JOINER_TO_GATEWAY, client, message_id, parsedMessage)

    def serialize_eof_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE, client, None, None)
    
    def serialize_eof_leader_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_LEADER_MESSAGE, client, None, None)
    
    def deserialize_input_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
