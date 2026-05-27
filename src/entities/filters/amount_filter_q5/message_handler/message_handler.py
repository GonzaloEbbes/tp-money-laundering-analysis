import uuid

from common import message_protocol
from common.message_protocol.internal import CantTrxData 


class MessageHandler:


    def serialize_gateway_query_message(client : str, message_id : str, message : any):
        parsedMessage = CantTrxData()
        parsedMessage.cantTrx = message.get("cantTrx")
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.AMOUNT_FILTER_Q5_TO_GATEWAY, client, message_id, parsedMessage)

    def serialize_eof_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE, client, None, None)
    
    def serialize_eof_leader_message(client,data):
        data_id = str(uuid.uuid4())
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_LEADER_MESSAGE, client, data_id, data)
    
    def deserialize_input_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
