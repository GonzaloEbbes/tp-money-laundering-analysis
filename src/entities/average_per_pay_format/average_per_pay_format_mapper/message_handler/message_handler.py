import uuid

from common import message_protocol
from common.message_protocol.internal import CantTrxData, TransactionData 


class MessageHandler:


    def serialize_average_per_pay_joiner_message(client : str, message_id : str, payment_format : str, values : dict):
        partial_payload = TransactionData({
                "PaymentFormat": payment_format,
                "sum_total": values.get("sum_total", 0),
                "count": values.get("count", 0),
            })
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_MAPPER_TO_AVERAGE_PER_PAY_FORMAT_JOINER, client, message_id, partial_payload)

    def serialize_eof_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE, client, None, None)
    
    def serialize_eof_leader_message(client, data=None):
        data_id = str(uuid.uuid4())
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_LEADER_MESSAGE, client, data_id, data)
    
    def serialize_eof_final_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_FINAL_MESSAGE, client, None, None)
    
    def deserialize_input_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
