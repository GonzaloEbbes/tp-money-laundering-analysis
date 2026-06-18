import uuid

from common import message_protocol
from common.message_protocol.internal import CantTrxData, EOFData, TransactionData 


class MessageHandler:

    def serialize_eof_message(client, total_packets, origin_worker_prefix):
        msg = EOFData()
        msg.packets = total_packets
        msg.origin_worker_prefix = origin_worker_prefix
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_MESSAGE, client, None, msg)
    
    def serialize_eof_consensus_request_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_CONSENSUS_REQUEST, client, None, None)
    
    
    def deserialize_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
