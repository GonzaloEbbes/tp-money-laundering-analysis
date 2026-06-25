import uuid

from common import message_protocol
from common.message_protocol.internal import InternalMessage, TransactionData


class MessageHandler:


    def serialize_amount_filter_q5_message(client : str, message_id : str, converted_payload, amount_field : str):
        parsedMessage = TransactionData()
        parsedMessage[amount_field] = converted_payload.get(amount_field)
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.USD_CURRENCY_CONVERTER_TO_AMOUNT_FILTER_Q5, client, message_id, parsedMessage)

    def deserialize_input_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
