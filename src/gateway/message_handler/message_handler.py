import uuid
import logging

from common import message_protocol
from common.controllers.eof_controller.message_handler.message_handler import EOFMessageHandler

logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s.%(msecs)03d - %(message)s',
            datefmt='%H:%M:%S'
        )

class MessageHandler:

    def __init__(self):
        self.client_uuid = str(uuid.uuid4())
        self.line_counter_by_client = {}
        self.message_counter_by_client = {}
        self.eof_count = 0

    def _get_next_data_id(self):
        self.line_counter_by_client[self.client_uuid] = self.line_counter_by_client.get(self.client_uuid, 0) + 1
        return self.line_counter_by_client[self.client_uuid]

    def _get_next_message_id(self):
        self.message_counter_by_client[self.client_uuid] = self.message_counter_by_client.get(self.client_uuid, 0) + 1
        return self.message_counter_by_client[self.client_uuid]

    def serialize_account_data(self, msg_data):
        bank_name, bank_id, _, _, _ = msg_data
        account_obj = message_protocol.internal.AccountData({
            "bank_name": bank_name,
            "bank_id": bank_id
        })
        data_id = self._get_next_data_id()
        message_id = self._get_next_message_id()
        return message_protocol.internal.serialize(
            message_protocol.internal.InternalMessageType.GATEWAY_TO_BANK_FILTER,
            self.client_uuid,
            data_id,
            account_obj,
            message_id=message_id,
        )
    
    def transaction_is_reinvestment(self, msg_data):
        (_, _, _, _, _, _, _, _,
         _, payment_format) = msg_data
        return payment_format.lower() == "reinvestment"
    
    def extract_bank_id(self, msg_data):
        _bank_name, bank_id, _, _, _ = msg_data
        return bank_id
    
    def serialize_transaction_currency(self, msg_data):
        (_timestamp, from_bank, account_origin, _, account_destination,
         amount_received, receiving_currency, _amount_paid,
         payment_currency, _payment_format) = msg_data
        currency_data = {
            "from_bank": from_bank,
            "account_origin": account_origin,
            "account_destination": account_destination,
            "amount_received": amount_received,
            "receiving_currency": receiving_currency,
            "payment_currency": payment_currency,
        }
        currency_obj = message_protocol.internal.TransactionData(currency_data)
        data_id = self._get_next_data_id()
        message_id = self._get_next_message_id()
        return message_protocol.internal.serialize(
            message_protocol.internal.InternalMessageType.GATEWAY_TO_USD_FILTER_Q1Q2,
            self.client_uuid,
            data_id,
            currency_obj,
            message_id=message_id,
        )
    
    def serialize_transaction_date(self, msg_data):
        (timestamp, _from_bank, account_origin, _, account_destination,
         amount_received, receiving_currency, amount_paid,
         payment_currency, payment_format) = msg_data
        date_data = {
            "timestamp": timestamp,
            "account_origin": account_origin,
            "account_destination": account_destination,
            "amount_received": amount_received,
            "receiving_currency": receiving_currency,
            "amount_paid": amount_paid,
            "payment_currency": payment_currency,
            "payment_format": payment_format,
        }
        date_obj = message_protocol.internal.TransactionData(date_data)
        data_id = self._get_next_data_id()
        message_id = self._get_next_message_id()
        return message_protocol.internal.serialize(
            message_protocol.internal.InternalMessageType.GATEWAY_TO_DATE_FILTER,
            self.client_uuid,
            data_id,
            date_obj,
            message_id=message_id,
        )

    def serialize_eof(self,total_packets_sent):
        data_id = self._get_next_data_id()
        amount_of_gateway_workers = 1
        my_prefix = "gateway"
        return EOFMessageHandler.serialize_eof_message(self.client_uuid, total_packets_sent, my_prefix, amount_of_gateway_workers, data_id)
    
    def deserialize_result_message(self, message):
        internal_message = message_protocol.internal.deserialize(message)
        if (
            internal_message.type
            not in [
                message_protocol.internal.InternalMessageType.AMOUNT_FILTER_Q1_TO_GATEWAY,
                message_protocol.internal.InternalMessageType.DATE_PER_BANK_REDUCER_TO_GATEWAY,
                message_protocol.internal.InternalMessageType.AMOUNT_FILTER_Q3_TO_GATEWAY,
                message_protocol.internal.InternalMessageType.SCATHER_GATHER_JOINER_TO_GATEWAY,
                message_protocol.internal.InternalMessageType.AMOUNT_FILTER_Q5_TO_GATEWAY,
                message_protocol.internal.InternalMessageType.EOF_MESSAGE
            ]
        ):
            return None

        # Only consume the result that belongs to this gateway-side client handler.
        if internal_message.source_client_uuid != self.client_uuid:
            return None

        logging.debug(
            "Client %s received message with data %s",
            internal_message.source_client_uuid,
            internal_message.data,
        )
        return internal_message
