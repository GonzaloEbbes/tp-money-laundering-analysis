import uuid

from common import message_protocol
from common.message_protocol.internal import InternalMessage, TransactionData


class MessageHandler:


    def serialize_average_per_pay_format_mapper_message(client : str, data_id : str, message : any, message_id=None):
        parsedMessage = TransactionData()
        parsedMessage.amount_received = message["amount_received"]
        parsedMessage.payment_format = message["payment_format"]
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.USD_FILTER_Q4_TO_AVERAGE_PER_PAY_FORMAT_MAPPER, client, data_id, parsedMessage, message_id=message_id)

    def serialize_scatter_gather_message(client : str, data_id : str, message : any, message_id=None):
        parsedMessage = TransactionData()
        parsedMessage.account_origin = message["account_origin"]
        parsedMessage.account_destination = message["account_destination"]
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.USD_FILTER_Q4_TO_SCATHER_GATHER_MAPPER, client, data_id, parsedMessage, message_id=message_id)

    def deserialize_input_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
