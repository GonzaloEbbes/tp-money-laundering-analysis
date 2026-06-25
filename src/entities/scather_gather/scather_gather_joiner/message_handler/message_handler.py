import uuid

from common import message_protocol
from common.message_protocol.internal import InternalMessage, ScatherGatherData, TransactionData


class MessageHandler:

    def _serialize_scather_gather_final_message(client : str, origen: str, destino: str):
        message_id = str(uuid.uuid4())
        parsedMessage = ScatherGatherData()
        parsedMessage.type = "FINAL"
        parsedMessage.value = [origen, destino]
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.SCATHER_GATHER_JOINER_TO_GATEWAY, client, message_id, parsedMessage, message_id=message_id)

    def deserialize_input_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
