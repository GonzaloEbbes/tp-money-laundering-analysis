import uuid

from common import message_protocol
from common.message_protocol.internal import TransactionData
 


class MessageHandler:


    def serialize_amount_filter_q3_exchange_message(averages, client, message_id):
        result_payload = TransactionData({
            "averages": averages,
        })
        return message_protocol.internal.serialize(
            message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_JOINER_TO_AMOUNT_FILTER_Q3,
            client,
            message_id,
            result_payload,
        )

    def deserialize_input_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
