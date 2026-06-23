import uuid

from common import message_protocol
from common.message_protocol.internal import InternalMessage, TransactionData


class MessageHandler:


    def serialize_usd_currency_converter_queue_message(client : str, message_id : str, message : any):
        parsedMessage = TransactionData()
        parsedMessage.timestamp = message["timestamp"]
        parsedMessage.amount_paid = message["amount_paid"]
        parsedMessage.payment_currency = message["payment_currency"]
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.PAY_FORMAT_FILTER_TO_USD_CURRENCY_CONVERTER, client, message_id, parsedMessage)

    def serialize_amount_filter_q5_queue_message(client : str, message_id : str, message : any):
        parsedMessage = TransactionData()
        parsedMessage.amount_paid = message["amount_paid"]
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.PAY_FORMAT_FILTER_TO_AMOUNT_FILTER_Q5, client, message_id, parsedMessage)

    def deserialize_input_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
