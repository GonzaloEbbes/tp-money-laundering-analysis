import uuid

from common import message_protocol
from common.message_protocol.internal import InternalMessage, TransactionData


class MessageHandler:


    def serialize_usd_filter_q3_message(client : str, message_id : str, message : any):
        parsedMessage = TransactionData()
        parsedMessage.account_origin = message["account_origin"]
        parsedMessage.amount_received = message["amount_received"]
        parsedMessage.payment_currency = message["payment_currency"]
        parsedMessage.receiving_currency = message["receiving_currency"]
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.DATE_FILTER_TO_USD_FILTER_Q3, client, message_id, parsedMessage)

    def serialize_usd_filter_q4_message(client : str, message_id : str, message : any):
        parsedMessage = TransactionData()
        parsedMessage.account_origin = message["account_origin"]
        parsedMessage.account_destination = message["account_destination"]
        parsedMessage.amount_received = message["amount_received"]
        parsedMessage.receiving_currency = message["receiving_currency"]
        parsedMessage.payment_currency = message["payment_currency"]
        parsedMessage.payment_format = message["payment_format"]
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.DATE_FILTER_TO_USD_FILTER_Q4, client, message_id, parsedMessage)
    
    def serialize_pay_format_filter_message(client : str, message_id : str, message : any):
        parsedMessage = TransactionData()
        parsedMessage.timestamp = message["timestamp"]
        parsedMessage.amount_paid = message["amount_paid"]
        parsedMessage.payment_currency = message["payment_currency"]
        parsedMessage.payment_format = message["payment_format"]
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.DATE_FILTER_TO_PAY_FORMAT_FILTER, client, message_id, parsedMessage)

    def serialize_eof_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE, client, None, None)
    
    def serialize_eof_leader_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_LEADER_MESSAGE, client, None, None)
    
    def deserialize_gateway_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
