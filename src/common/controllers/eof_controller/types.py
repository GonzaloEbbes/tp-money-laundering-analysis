from enum import IntEnum
import logging
import threading

type partial_count_by_worker_prefix = dict[str, int]
type total_count_by_prefix = dict[str, (int,int)] #total de workers de la capa y el total de paquetes informados por esos workers para ese prefix
type partial_count_by_worker_prefix_and_id = dict[str, dict[str, int]] # worker_prefix -> worker_id -> count


class EOFStates(IntEnum):
    ACTIVE = 0
    PENDING_EOF = 1
    PENDING_EOF_IN_CONSENSUS_REQUEST_SENT = 2
    PENDING_EOF_IN_CONSENSUS_RESPONSE_SENT = 3
    EOF_CONSENSUS_ACHIEVED = 4
    EOF_FINISH_ENABLED = 5


'''Clas de manejo de estados del cliente con respecto a EOF. Totalmente idempotente, se puede llamar varias veces a los metodos de cambio de estado y no se rompe el flujo.'''
class ClientEOFState:
    
    def __init__(self, state: EOFStates = EOFStates.ACTIVE):
        self.state = state 
    
    def _update_state(self, new_state: EOFStates):
        if (self._consensus_failed(new_state) or self.state < new_state):
            self.state = new_state

    def _consensus_failed(self, new_state: EOFStates):
        return self.state == EOFStates.PENDING_EOF_IN_CONSENSUS_RESPONSE_SENT and new_state == EOFStates.PENDING_EOF 

    def change_state_to_pending_eof(self):
        self._update_state(EOFStates.PENDING_EOF)
    
    def change_state_to_pending_eof_in_consensus_request_sent(self):
        self._update_state(EOFStates.PENDING_EOF_IN_CONSENSUS_REQUEST_SENT)

    def change_state_to_pending_eof_in_consensus_response_sent(self):
        self._update_state(EOFStates.PENDING_EOF_IN_CONSENSUS_RESPONSE_SENT)

    def change_state_to_eof_consensus_failed(self):
        self._update_state(EOFStates.PENDING_EOF)
    
    def change_state_to_eof_consensus_achieved(self):
        self._update_state(EOFStates.EOF_CONSENSUS_ACHIEVED)
    
    def change_state_to_eof_finish_enabled(self):
        self._update_state(EOFStates.EOF_FINISH_ENABLED)

    def mark_client_as_active(client_id, client_list : dict[str, 'ClientEOFState'], lock: threading.Lock):
        with lock:
            client_list.setdefault(client_id, ClientEOFState())

    def mark_client_as_pending_eof(client_id, client_list : dict[str, 'ClientEOFState'], lock: threading.Lock):
        with lock:
            client_list.setdefault(client_id, ClientEOFState()).change_state_to_pending_eof()
    
    def mark_client_as_pending_eof_in_consensus_request_sent(client_id, client_list : dict[str, 'ClientEOFState'], lock: threading.Lock):
        with lock:
            client_list.setdefault(client_id, ClientEOFState()).change_state_to_pending_eof_in_consensus_request_sent()
    
    def mark_client_as_pending_eof_in_consensus_response_sent(client_id, client_list : dict[str, 'ClientEOFState'], lock: threading.Lock):
        with lock:
            client_list.setdefault(client_id, ClientEOFState()).change_state_to_pending_eof_in_consensus_response_sent()

    def mark_client_as_eof_consensus_failed(client_id, client_list : dict[str, 'ClientEOFState'], lock: threading.Lock):
        with lock:
            client_list.setdefault(client_id, ClientEOFState()).change_state_to_eof_consensus_failed()
    
    def mark_client_as_eof_consensus_achieved(client_id, client_list : dict[str, 'ClientEOFState'], lock: threading.Lock):
        with lock:
            client_list.setdefault(client_id, ClientEOFState()).change_state_to_eof_consensus_achieved()