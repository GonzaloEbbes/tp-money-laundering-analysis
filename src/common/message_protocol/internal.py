import json
import uuid

import json

class InternalMessageType:
    GATEWAY_TO_DATE_FILTER = 0
    GATEWAY_TO_USD_FILTER_Q1Q2 = 1
    USD_FILTER_Q1Q2_TO_AMOUNT_FILTER_Q1 = 2
    USD_FILTER_Q1Q2_TO_DATA_PER_BANK_SHUFFLER = 3
    DATA_PER_BANK_SHUFFLER_TO_DATA_PER_BANK_REDUCER = 4
    DATE_FILTER_TO_USD_FILTER_Q3 = 5
    DATE_FILTER_TO_USD_FILTER_Q4 = 6
    DATE_FILTER_TO_PAY_FORMAT_FILTER = 7
    USD_FILTER_Q4_TO_AVERAGE_PER_PAY_FORMAT_MAPPER = 8
    AVERAGE_PER_PAY_FORMAT_MAPPER_TO_AVERAGE_PER_PAY_FORMAT_AGGREGATOR = 9
    AVERAGE_PER_PAY_FORMAT_AGGREGATOR_TO_AMOUNT_FILTER_Q3 = 10
    USD_FILTER_Q4_TO_SCATHER_GATHER_MAPPER = 11
    SCATHER_GATHER_MAPPER_TO_SCATHER_GATHER_AGGREGATOR = 12
    SCATHER_GATHER_AGGREGATOR_TO_SCATHER_GATHER_JOINER = 13
    PAY_FORMAT_FILTER_TO_USD_CURRENCY_CONVERTER = 14
    USD_CURRENCY_CONVERTER_TO_AMOUNT_FILTER_Q5 = 15
    PAY_FORMAT_FILTER_TO_AMOUNT_FILTER_Q5 = 16
    AMOUNT_FILTER_Q1_TO_GATEWAY = 17
    DATE_PER_BANK_REDUCER_TO_GATEWAY = 18
    AMOUNT_FILTER_Q3_TO_GATEWAY = 19
    SCATHER_GATHER_JOINER_TO_GATEWAY = 20
    AMOUNT_FILTER_Q5_TO_GATEWAY = 21
    EOF_GENERIC_MESSAGE = 22

class TransactionData(dict):
    timestamp : str
    from_bank : str
    account_origin : str
    to_bank : str
    account_destination : str
    amount_received : float
    receiving_currency : str
    amount_paid : float
    payment_currency : str
    payment_format : str
    is_laundering : str

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        try:
            del self[name]
        except KeyError:
            raise AttributeError(name)

class AccountData(dict):
    bank_name : str
    bank_id : str
    account_number : str
    entity_id : str
    entity_name : str

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        try:
            del self[name]
        except KeyError:
            raise AttributeError(name)
class InternalMessage:

    type : InternalMessageType
    source_client_uuid : str | None
    data : TransactionData | AccountData | None
    
    def __init__(self, type=None, source_client_uuid=None, data_id=None, data=None):
        self.type = type
        self.source_client_uuid = source_client_uuid
        self.data_id = data_id
        self.data = data

    def _serialize(self):
        msg_dict = {"type": self.type}

        if self.source_client_uuid is not None:
            msg_dict["source_client_uuid"] = self.source_client_uuid

        if self.data_id is not None:
            msg_dict["data_id"] = self.data_id

        if self.data is not None:
            msg_dict["data"] = self.data

        return json.dumps(msg_dict).encode("utf-8")
    
    def _deserialize(self, data):
        msg = json.loads(data.decode("utf-8"))
        self.type = msg["type"] if "type" in msg else None
        self.source_client_uuid = msg["source_client_uuid"] if "source_client_uuid" in msg else None
        self.data_id = msg["data_id"] if "data_id" in msg else None
        self.data = msg["data"] if "data" in msg else None


def serialize(type,client_id,data_id,data) -> bytes:
    msg = InternalMessage(type=type, source_client_uuid=client_id, data_id=data_id, data=data)
    return msg._serialize()



def deserialize(data) -> InternalMessage:
    msg = InternalMessage()
    msg._deserialize(data)
    return msg
