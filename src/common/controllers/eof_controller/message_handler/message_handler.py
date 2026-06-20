import uuid

from common import message_protocol
from common.message_protocol.internal import CantTrxData, EOFData, TransactionData 


class MessageHandler:

    def serialize_eof_message(client, total_packets, origin_worker_prefix, amount_origin_workers):
        msg = EOFData()
        msg.total_packets = total_packets
        msg.origin_worker_prefix = origin_worker_prefix
        msg.amount_origin_workers = amount_origin_workers
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_MESSAGE, client, None, msg)
    
    def serialize_eof_consensus_request_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_CONSENSUS_REQUEST, client, None, None)
    
    def serialize_eof_consensus_response_message_default(client, packets_in_flux_1, packets_in_flux_2, origin_worker_prefix_flux_1, origin_worker_prefix_flux_2, worker_id_flux_1, worker_id_flux_2):
        msg = EOFData()
        msg.partial_packets_count_flux_1 = packets_in_flux_1
        msg.partial_packets_count_flux_2 = packets_in_flux_2
        msg.origin_worker_prefix_flux_1 = origin_worker_prefix_flux_1
        msg.origin_worker_prefix_flux_2 = origin_worker_prefix_flux_2
        msg.worker_id_flux_1 = worker_id_flux_1
        msg.worker_id_flux_2 = worker_id_flux_2
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_CONSENSUS_RESPONSE, client, None, None)
    
    def serialize_eof_consensus_response_message_auxiliary_flux(client, packets_in_flux_1, packets_in_aux_flux_2, origin_worker_prefix_flux_1, origin_worker_prefix_aux_flux_2, worker_id_flux_1, worker_id_aux_flux_2):
        msg = EOFData()
        msg.partial_packets_count_flux_1 = packets_in_flux_1
        msg.partial_packets_count_flux_2 = packets_in_aux_flux_2
        msg.origin_worker_prefix_flux_1 = origin_worker_prefix_flux_1
        msg.origin_worker_prefix_flux_2 = origin_worker_prefix_aux_flux_2
        msg.worker_id_flux_1 = worker_id_flux_1
        msg.worker_id_flux_2 = worker_id_aux_flux_2
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_CONSENSUS_RESPONSE, client, None, None)
    
    def serialize_eof_consensus_ok_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_CONSENSUS_OK, client, None, None)

    def serialize_eof_consensus_failed_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_CONSENSUS_FAIL, client, None, None)

    def serialize_eof_post_consensus_ok_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_POST_CONSENSUS_OK, client, None, None)
    
    
    def deserialize_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
