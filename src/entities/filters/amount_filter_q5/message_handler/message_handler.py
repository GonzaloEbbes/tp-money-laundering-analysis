import uuid

from common import message_protocol
from common.message_protocol.internal import CantTrxData 


class MessageHandler:


    def serialize_gateway_query_message(client : str, data_id : str, message : any, message_id=None):
        parsedMessage = CantTrxData()
        parsedMessage.cantTrx = message.get("cantTrx")
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.AMOUNT_FILTER_Q5_TO_GATEWAY, client, data_id, parsedMessage, message_id=message_id)

    def deserialize_input_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
