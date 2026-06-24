import uuid

from common import message_protocol
from common.message_protocol.internal import  ScatherGatherData


class MessageHandler:


    def serialize_scather_gather_pair_middle_message(client : str, origen:str, destino:str, middle_account:str):
        message_id = str(uuid.uuid4())
        parsedMessage = ScatherGatherData()
        parsedMessage.type = "PAIR_MIDDLE"
        parsedMessage.value = [origen, destino, middle_account]
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.SCATHER_GATHER_PAIR_JOINER_TO_SCATHER_GATHER_JOINER, client, message_id, parsedMessage, message_id=message_id)

    def serialize_eof_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE, client, None, None)
    
    def serialize_eof_leader_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_LEADER_MESSAGE, client, None, None)
    
    def deserialize_input_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
