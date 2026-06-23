import uuid

from common import message_protocol
from common.message_protocol.internal import InternalMessage, ScatherGatherData, TransactionData


class MessageHandler:


    def serialize_scather_gather_middle_message_fanout(client : str, origen : str, destino_middle: str):
        return MessageHandler._serialize_scather_gather_aggregator_message(client, "FANOUT_MIDDLE", origen, destino_middle)

    def serialize_scather_gather_middle_message_fanin(client : str, destino : str, origen_middle: str):
        return MessageHandler._serialize_scather_gather_aggregator_message(client, "FANIN_MIDDLE", destino, origen_middle)


    def _serialize_scather_gather_aggregator_message(client : str, type:str, key : str, values: str):
        message_id = str(uuid.uuid4())
        parsedMessage = ScatherGatherData()
        parsedMessage.type = type
        parsedMessage.key = key
        parsedMessage.value = values
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.SCATHER_GATHER_AGGREGATOR_TO_SCATHER_GATHER_PAIR_JOINER, client, message_id, parsedMessage)
    
    def deserialize_input_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
