import uuid

from common import message_protocol
from common.message_protocol.internal import InternalMessage, TransactionData


class MessageHandler:


    def serialize_amount_filter_q3_message(client : str, data_id : str, message : any, message_id=None):
        parsedMessage = TransactionData()
        parsedMessage.account_origin = message["account_origin"]
        parsedMessage.amount_received = message["amount_received"]
        parsedMessage.payment_format = message["payment_format"]
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.USD_FILTER_Q3_TO_AMOUNT_FILTER_Q3, client, data_id, parsedMessage, message_id=message_id)

    def deserialize_input_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
