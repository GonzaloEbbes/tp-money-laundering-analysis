import uuid

from common import message_protocol
from common.message_protocol.internal import InternalMessage, TransactionData


class MessageHandler:


    def serialize_amount_filter_q1_message(client : str, message_id : str, message : any):
        parsedMessage = TransactionData()
        parsedMessage.account_origin = message["account_origin"]
        parsedMessage.account_destination = message["account_destination"]
        parsedMessage.amount_received = message["amount_received"]
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.USD_FILTER_Q1Q2_TO_AMOUNT_FILTER_Q1, client, message_id, parsedMessage)

    def serialize_data_per_bank_redirector_message(client : str, message_id : str, message : any):
        parsedMessage = TransactionData()
        parsedMessage.account_origin = message["account_origin"]
        parsedMessage.from_bank = message["from_bank"]
        parsedMessage.amount_received = message["amount_received"]
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.USD_FILTER_Q1Q2_TO_DATA_PER_BANK_SHUFFLER, client, message_id, parsedMessage)

    def deserialize_gateway_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
