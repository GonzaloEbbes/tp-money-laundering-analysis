from common import message_protocol
from common.message_protocol.internal import CantTrxData, EOFData, TransactionData 
from common.controllers.eof_controller.types import partial_count_by_worker_prefix, partial_count_by_worker_prefix_and_id, total_count_by_prefix

class MessageHandler:

    def serialize_eof_message(client, total_packets, origin_worker_prefix, amount_origin_workers):
        msg = EOFData()
        msg.total_packets = total_packets
        msg.origin_worker_prefix = origin_worker_prefix
        msg.amount_origin_workers = amount_origin_workers
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_MESSAGE, client, None, msg)
    
    def serialize_eof_consensus_request_message(client, eofs_received_by_client, eofs_received_by_client_lock, is_auxiliary_input):
        with eofs_received_by_client_lock:
            eofs_received_for_client = set(eofs_received_by_client.get(client, set()))
        msg = EOFData()
        if is_auxiliary_input and "average_per_pay_format_joiner" in eofs_received_for_client:
            msg.awaited_origin_worker_prefix_flux_2 = "average_per_pay_format_joiner"
            eofs_received_for_client.discard("average_per_pay_format_joiner")
            msg.awaited_origin_worker_prefix_flux_1 = eofs_received_for_client.pop() if len(eofs_received_for_client) > 0 else None
        else:
            msg.awaited_origin_worker_prefix_flux_1 = eofs_received_for_client.pop() if len(eofs_received_for_client) > 0 else None
            msg.awaited_origin_worker_prefix_flux_2 = eofs_received_for_client.pop() if len(eofs_received_for_client) > 0 else None
        
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_CONSENSUS_REQUEST, client, None, msg)
    
    def serialize_eof_consensus_response_message(client, worker_id, is_auxiliary_input, partial_packets : dict[str, partial_count_by_worker_prefix], partial_packets_lock):
        with partial_packets_lock:
            partial_packets_by_client : partial_count_by_worker_prefix = dict(partial_packets.get(client, {}))

        packets_in_flux_1 = None
        origin_worker_prefix_flux_1 = None
        packets_in_flux_2 = None
        origin_worker_prefix_flux_2 = None

        if is_auxiliary_input:
            # Reviso los elementos de partial_packets_by_client (que son del tipo dict[flujo,parcial] y busco si hay alguno que sea average_per_pay_format_joiner. 
            # Si lo hay, ese lo envio como parcial DEL FLUJO 2, y el resto de los parciales los envio como parciales del flujo 1. Si no hay ninguno que sea average_per_pay_format_joiner, es indistinto
            for origin_worker_prefix_flux, partial_count in partial_packets_by_client.items():
                if origin_worker_prefix_flux == "average_per_pay_format_joiner":
                    packets_in_flux_2 = partial_count
                    origin_worker_prefix_flux_2 = origin_worker_prefix_flux
                else:
                    packets_in_flux_1 = partial_count
                    origin_worker_prefix_flux_1 = origin_worker_prefix_flux

            return MessageHandler._serialize_eof_consensus_response_message_default(client, packets_in_flux_1, packets_in_flux_2, origin_worker_prefix_flux_1, origin_worker_prefix_flux_2, worker_id)
        else:
            # Aquí es indistinto. El primero que llega es el flujo 1, el segundo el flujo 2. Si solo llega uno, ese es el flujo 1 y el flujo 2 queda con valor 0
            for origin_worker_prefix_flux, partial_count in partial_packets_by_client.items():
                if packets_in_flux_1 == None:
                    packets_in_flux_1 = partial_count
                    origin_worker_prefix_flux_1 = origin_worker_prefix_flux
                else:
                    packets_in_flux_2 = partial_count
                    origin_worker_prefix_flux_2 = origin_worker_prefix_flux

            return MessageHandler._serialize_eof_consensus_response_message_default(client, packets_in_flux_1, packets_in_flux_2, origin_worker_prefix_flux_1, origin_worker_prefix_flux_2, worker_id)

    def _serialize_eof_consensus_response_message_default(client, packets_in_flux_1, packets_in_flux_2, origin_worker_prefix_flux_1, origin_worker_prefix_flux_2, worker_id):
        msg = EOFData()
        msg.partial_packets_count_flux_1 = packets_in_flux_1 if packets_in_flux_1 != None else 0
        msg.partial_packets_count_flux_2 = packets_in_flux_2 if packets_in_flux_2 != None else 0
        msg.origin_worker_prefix_flux_1 = origin_worker_prefix_flux_1
        msg.origin_worker_prefix_flux_2 = origin_worker_prefix_flux_2
        msg.worker_id_sending_partials = worker_id
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_CONSENSUS_RESPONSE, client, None, msg)
    
    def serialize_eof_consensus_ok_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_CONSENSUS_OK, client, None, None)

    def serialize_eof_consensus_failed_message(client):
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_CONSENSUS_FAIL, client, None, None)

    def serialize_eof_post_consensus_ok_message(client, id):
        msg = EOFData()
        msg.postconsensus_worker_id = id
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_POST_CONSENSUS_OK, client, None, msg)


    def deserialize_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
