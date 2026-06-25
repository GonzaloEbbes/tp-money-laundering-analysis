import uuid

from common import message_protocol
from common.message_protocol.internal import CantTrxData, TransactionData 


class MessageHandler:


    def serialize_average_per_pay_joiner_message(client : str, data_id : str, payment_format : str, values : dict, message_id=None):
        partial_payload = TransactionData({
                "PaymentFormat": payment_format,
                "sum_total": values.get("sum_total", 0),
                "count": values.get("count", 0),
            })
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_MAPPER_TO_AVERAGE_PER_PAY_FORMAT_JOINER, client, data_id, partial_payload, message_id=message_id)

    def deserialize_input_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
