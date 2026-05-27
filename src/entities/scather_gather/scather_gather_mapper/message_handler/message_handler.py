import uuid

from common import message_protocol
from common.message_protocol.internal import InternalMessage, ScatherGatherData, TransactionData


class MessageHandler:


    def serialize_scather_gather_mapper_message_fanout(client : str, origen : str, destinos: list[str]):
        MessageHandler._serialize_scather_gather_mapper_message(client, origen, destinos)

    def serialize_scather_gather_mapper_message_fanin(client : str, destino : str, origenes: list[str]):
        MessageHandler._serialize_scather_gather_mapper_message(client, destino, origenes)


    def _serialize_scather_gather_mapper_message(client : str, key : str, values: list[str]):
        message_id = str(uuid.uuid4())
        parsedMessage = ScatherGatherData()
        parsedMessage.key = key
        parsedMessage.value = values
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.SCATHER_GATHER_MAPPER_TO_SCATHER_GATHER_AGGREGATOR, client, message_id, parsedMessage)

    def serialize_eof_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE, client, None, None)
    
    def serialize_eof_leader_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_LEADER_MESSAGE, client, None, None)
    
    def deserialize_input_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
