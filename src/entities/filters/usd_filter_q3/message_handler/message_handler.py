import uuid

from common import message_protocol
from common.message_protocol.internal import InternalMessage, TransactionData


class MessageHandler:


    def serialize_amount_filter_q3_message(client : str, message_id : str, message : any):
        parsedMessage = TransactionData()
        parsedMessage.account_origin = message["account_origin"]
        parsedMessage.amount_received = message["amount_received"]
        parsedMessage.payment_format = message["payment_format"]
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.USD_FILTER_Q3_TO_AMOUNT_FILTER_Q3, client, message_id, parsedMessage)

    def deserialize_input_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
