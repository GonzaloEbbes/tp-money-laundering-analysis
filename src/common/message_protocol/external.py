from asyncio import IncompleteReadError

from . import external_serializer


class MsgType:
    ACCOUNT_RECORD = 1
    TRANSACTION_RECORD = 2
    QUERY_1_RESULT = 3
    QUERY_2_RESULT = 4
    QUERY_3_RESULT = 5
    QUERY_4_RESULT = 6
    QUERY_5_RESULT = 7
    ACK = 8
    EOF = 9
    CLOSE = 10


def _recv_sized(socket, size):
    """
    Receives exactly 'num_bytes' bytes through the provided socket.
    If no bytes are read from the socket IncompleteReadError is raised
    """
    buf = bytearray(size)
    pos = 0
    while pos < size:
        n = socket.recv_into(memoryview(buf)[pos:])
        if n == 0:
            raise IncompleteReadError(bytes(buf[:pos]), size)
        pos += n
    return bytes(buf)


def _recv_account_record(socket):
    bank_name_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    bank_name = external_serializer.deserialize_string(
        _recv_sized(socket, bank_name_size)
    )
    bank_id = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    account_number_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    account_number = external_serializer.deserialize_string(
        _recv_sized(socket, account_number_size)
    )
    entity_id_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    entity_id = external_serializer.deserialize_uint32(
        _recv_sized(socket, entity_id_size)
    )
    entity_name_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    entity_name = external_serializer.deserialize_string(
        _recv_sized(socket, entity_name_size)
    )
    return (bank_name, bank_id, account_number, entity_id, entity_name)

def _recv_transaction_record(socket):
    timestamp_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    timestamp = external_serializer.deserialize_string(
        _recv_sized(socket, timestamp_size)
    )
    from_bank_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    from_bank = external_serializer.deserialize_string(
        _recv_sized(socket, from_bank_size)
    )
    account_origin_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    account_origin = external_serializer.deserialize_string(
        _recv_sized(socket, account_origin_size)
    )
    to_bank_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    to_bank = external_serializer.deserialize_string(
        _recv_sized(socket, to_bank_size)
    )
    account_destination_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    account_destination = external_serializer.deserialize_string(
        _recv_sized(socket, account_destination_size)
    )
    amount_received = external_serializer.deserialize_double(
        _recv_sized(socket, external_serializer.DOUBLE_SIZE)
    )
    receiving_currency_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    receiving_currency = external_serializer.deserialize_string(
        _recv_sized(socket, receiving_currency_size)
    )
    amount_paid = external_serializer.deserialize_double(
        _recv_sized(socket, external_serializer.DOUBLE_SIZE)
    )
    payment_currency_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    payment_currency = external_serializer.deserialize_string(
        _recv_sized(socket, payment_currency_size)
    )
    payment_format_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    payment_format = external_serializer.deserialize_string(
        _recv_sized(socket, payment_format_size)
    )

    return (timestamp, from_bank, account_origin, to_bank, account_destination,
            amount_received, receiving_currency, amount_paid,
            payment_currency, payment_format)

def _recv_query_1_result(socket):
    account_origin_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    account_origin = external_serializer.deserialize_string(
        _recv_sized(socket, account_origin_size)
    )
    account_destination_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    account_destination = external_serializer.deserialize_string(
        _recv_sized(socket, account_destination_size)
    )
    total_amount = external_serializer.deserialize_double(
        _recv_sized(socket, external_serializer.DOUBLE_SIZE)
    )

    return (account_origin, account_destination, total_amount)

def _recv_query_2_result(socket):
    bank_name_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    bank_name = external_serializer.deserialize_string(
        _recv_sized(socket, bank_name_size)
    )
    account_origin_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    account_origin = external_serializer.deserialize_string(
        _recv_sized(socket, account_origin_size)
    )
    max_amount = external_serializer.deserialize_double(
        _recv_sized(socket, external_serializer.DOUBLE_SIZE)
    )
    return (bank_name, account_origin, max_amount)

def _recv_query_3_result(socket):
    account_origin_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    account_origin = external_serializer.deserialize_string(
        _recv_sized(socket, account_origin_size)
    )
    amount = external_serializer.deserialize_double(
        _recv_sized(socket, external_serializer.DOUBLE_SIZE)
    )
    return (account_origin, amount)

def _recv_query_4_result(socket):
    accounts_size = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    accounts = []
    for _ in range(accounts_size):
        account_size = external_serializer.deserialize_uint32(
            _recv_sized(socket, external_serializer.UINT32_SIZE)
        )
        account = external_serializer.deserialize_string(
            _recv_sized(socket, account_size)
        )
        accounts.append(account)
    return accounts

def _recv_query_5_result(socket):
    transaction_amount = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    return transaction_amount


def _recv_empty(socket):
    return None


RECV_MSG_HANDLERS = {
    MsgType.ACCOUNT_RECORD: _recv_account_record,
    MsgType.TRANSACTION_RECORD: _recv_transaction_record,
    MsgType.QUERY_1_RESULT: _recv_query_1_result,
    MsgType.QUERY_2_RESULT: _recv_query_2_result,
    MsgType.QUERY_3_RESULT: _recv_query_3_result,
    MsgType.QUERY_4_RESULT: _recv_query_4_result,
    MsgType.QUERY_5_RESULT: _recv_query_5_result,
    MsgType.ACK: _recv_empty,
    MsgType.EOF: _recv_empty,
    MsgType.CLOSE: _recv_empty,
}


def recv_msg(socket):
    msg_type = external_serializer.deserialize_uint32(
        _recv_sized(socket, external_serializer.UINT32_SIZE)
    )
    msg_handler = RECV_MSG_HANDLERS[msg_type]
    return (msg_type, msg_handler(socket))


def _serialize_account_record(bank_name, bank_id, account_number, entity_id, entity_name):
    return b"".join(
        [
            external_serializer.serialize_uint32(MsgType.ACCOUNT_RECORD),
            external_serializer.serialize_uint32(len(bank_name)),
            external_serializer.serialize_string(bank_name),
            external_serializer.serialize_uint32(bank_id),
            external_serializer.serialize_uint32(len(account_number)),
            external_serializer.serialize_string(account_number),
            external_serializer.serialize_uint32(len(entity_id)),
            external_serializer.serialize_string(entity_id),
            external_serializer.serialize_uint32(len(entity_name)),
            external_serializer.serialize_string(entity_name),
        ]
    )

def _serialize_transaction_record(timestamp, from_bank, account_origin,
                                  to_bank, account_destination, amount_received,
                                  receiving_currency, amount_paid,
                                  payment_currency, payment_format):
    return b"".join(
        [
            external_serializer.serialize_uint32(MsgType.TRANSACTION_RECORD),
            external_serializer.serialize_uint32(len(timestamp)),
            external_serializer.serialize_string(timestamp),
            external_serializer.serialize_uint32(len(from_bank)),
            external_serializer.serialize_string(from_bank),
            external_serializer.serialize_uint32(len(account_origin)),
            external_serializer.serialize_string(account_origin),
            external_serializer.serialize_uint32(len(to_bank)),
            external_serializer.serialize_string(to_bank),
            external_serializer.serialize_uint32(len(account_destination)),
            external_serializer.serialize_string(account_destination),
            external_serializer.serialize_double(amount_received),
            external_serializer.serialize_uint32(len(receiving_currency)),
            external_serializer.serialize_string(receiving_currency),
            external_serializer.serialize_double(amount_paid),
            external_serializer.serialize_uint32(len(payment_currency)),
            external_serializer.serialize_string(payment_currency),
            external_serializer.serialize_uint32(len(payment_format)),
            external_serializer.serialize_string(payment_format),
        ]
    )

def _serialize_query_1_result(account_origin, account_destination, amount_received : float
):
    return b"".join(
        [
            external_serializer.serialize_uint32(MsgType.QUERY_1_RESULT),
            external_serializer.serialize_uint32(len(account_origin)),
            external_serializer.serialize_string(account_origin),
            external_serializer.serialize_uint32(len(account_destination)),
            external_serializer.serialize_string(account_destination),
            external_serializer.serialize_double(amount_received),
        ]
    )

def _serialize_query_2_result(bank_name, account_origin, amount_received):
    return b"".join(
        [
            external_serializer.serialize_uint32(MsgType.QUERY_2_RESULT),
            external_serializer.serialize_uint32(len(bank_name)),
            external_serializer.serialize_string(bank_name),
            external_serializer.serialize_uint32(len(account_origin)),
            external_serializer.serialize_string(account_origin),
            external_serializer.serialize_double(amount_received),
        ]
    )

def _serialize_query_3_result(account_origin, amount_received):
    return b"".join(
        [
            external_serializer.serialize_uint32(MsgType.QUERY_3_RESULT),
            external_serializer.serialize_uint32(len(account_origin)),
            external_serializer.serialize_string(account_origin),
            external_serializer.serialize_double(amount_received),
        ]
    )

def _serialize_query_4_result(accounts):
    msg = external_serializer.serialize_uint32(MsgType.QUERY_4_RESULT)
    msg += external_serializer.serialize_uint32(len(accounts))
    for account in accounts:
        msg += external_serializer.serialize_uint32(len(account))
        msg += external_serializer.serialize_string(account)
    return msg

def _serialize_query_5_result(transaction_amount):
    msg = external_serializer.serialize_uint32(MsgType.QUERY_5_RESULT)
    msg += external_serializer.serialize_uint32(transaction_amount)
    return msg

def _send_query_1_result(socket, account_origin, account_destination, total_amount):
    msg = _serialize_query_1_result(account_origin, account_destination, total_amount)
    socket.sendall(msg)

def _send_query_2_result(socket, bank_name, account_origin, max_amount):
    msg = _serialize_query_2_result(bank_name, account_origin, max_amount)
    socket.sendall(msg)

def _send_query_3_result(socket, account_origin, amount):
    msg = _serialize_query_3_result(account_origin, amount)
    socket.sendall(msg)

def _send_query_4_result(socket, accounts):
    msg = _serialize_query_4_result(accounts)
    socket.sendall(msg)

def _send_query_5_result(socket, transaction_amount):
    msg = _serialize_query_5_result(transaction_amount)
    socket.sendall(msg)

def _send_account_record(socket, bank_name, bank_id, account_number, entity_id, entity_name):
    msg = _serialize_account_record(bank_name, bank_id, account_number, entity_id, entity_name)
    socket.sendall(msg)

def _send_transaction_record(socket, timestamp, from_bank, account_origin,
                                to_bank, account_destination, amount_received,
                                receiving_currency, amount_paid,
                                payment_currency, payment_format):
    msg = _serialize_transaction_record(timestamp, from_bank, account_origin,
                                        to_bank, account_destination, amount_received,
                                        receiving_currency, amount_paid,
                                        payment_currency, payment_format)
    socket.sendall(msg)


def _send_ack(socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.ACK))


def _send_eof(socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.EOF))

def _send_close(socket):
    socket.sendall(external_serializer.serialize_uint32(MsgType.CLOSE))


SEND_MSG_HANDLERS = {
    MsgType.ACCOUNT_RECORD: _send_account_record,
    MsgType.TRANSACTION_RECORD: _send_transaction_record,
    MsgType.QUERY_1_RESULT: _send_query_1_result,
    MsgType.QUERY_2_RESULT: _send_query_2_result,
    MsgType.QUERY_3_RESULT: _send_query_3_result,
    MsgType.QUERY_4_RESULT: _send_query_4_result,
    MsgType.QUERY_5_RESULT: _send_query_5_result,
    MsgType.ACK: _send_ack,
    MsgType.EOF: _send_eof,
    MsgType.CLOSE: _send_close,
}


def send_msg(socket, msg_type, *args):
    msg_handler = SEND_MSG_HANDLERS[msg_type]
    msg_handler(socket, *args)
