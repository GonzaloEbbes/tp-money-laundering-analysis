


import threading
import logging

from common import message_protocol, middleware
from common.controllers.message_handler.message_handler import MessageHandler


class EOFController:

    #todo: VAS A TENER QUE HACER FUNCIONES CALLBACK PARA EL ENVIO PROPIO DEL EOF FINAL Y LA RECEPCION DEL EOF INICIAL?

    def __init__(self, mom_host, id_worker, prefix_worker, amount_workers,eof_control_exchange_name,input_eofs_quantities):

        
        self.id = int(id_worker)
        self.amount_workers = int(amount_workers)
        self.mom_host = mom_host
        self.prefix_worker : str = prefix_worker
        self.eof_control_exchange_name : str = eof_control_exchange_name
        self.input_eofs_quantities = input_eofs_quantities

        self.eofs_received_by_client_lock = threading.Lock()
        self.eofs_received_by_client : dict[str, set] = {} # Por cliente, set de prefixes de los cuales se recibieron EOFs

        self.total_packets_received_by_client_lock = threading.Lock()
        self.total_packets_received_by_client : dict[str, int] = {} # Paquetes informados de la capa anterior por cliente


        self.packets_processed_by_client_lock = threading.Lock()
        self.packets_processed_by_client : dict[str, int] = {} # Paquetes procesados por cliente en esta instancia del worker
        #TODO: enlazar con funcion al procesamiento de datos

        #TODO: esto sera una funcion de callback desde el worker que definira el proceso a realizar al recibir todos los eof de la capa anterior 
        #TODO: (si hace falta). Considerar que es luego del consenso de eof
        self.on_total_ok_eof_reception_for_client_callback = None

        if self.is_single_instance():
            other_worker_instances = []
            for i in range(self.amount_workers):
                if i != self.id:
                    other_worker_instances.append(f"{self.prefix_worker}_{i}")

            self.eof_exchange_consumer_lock = threading.Lock()
            self.eof_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
                    self.mom_host,
                    self.eof_control_exchange_name,
                    [f"{self.prefix_worker}_{self.id}"],
                )
            
            self.eof_exchange_producer_fanout_lock = threading.Lock()
            self.eof_exchange_producer_fanout = middleware.MessageMiddlewareExchangeRabbitMQ(
                    self.mom_host,
                    self.eof_control_exchange_name,
                    other_worker_instances,
                )
            
            self.eof_exchange_producer_to_leader_lock = threading.Lock()
            self.eof_exchange_producer_to_leader = middleware.MessageMiddlewareExchangePublisherRabbitMQ(
                    self.mom_host,
                    self.eof_control_exchange_name
                )
            
            self.eof_consensus_pending_by_client_lock = threading.Lock()
            self.eof_consensus_pending_by_client : set[str] = set()
        


        self._stop_lock = threading.Lock()
        self._stopping = False


    
    
    def _im_leader(self):
        return self.id == 0
    
    def is_single_instance(self):
        return self.amount_workers == 1
    
    #Funcion a llamarse para cuando desde alguna de las colas/exchanges de entrada se reciba un EOF
    def on_input_queue_eof_reception(self, client_id,data):
        #Sumar el EOF al set de EOFs recibidos para ese cliente. como clave debe ir el prefix que viaja en el tipo de mensaje EOF_MESSAGE
        with self.eofs_received_by_client_lock:
            self.eofs_received_by_client.setdefault(client_id, set())
            self.eofs_received_by_client[client_id].add(data.origin_worker_prefix)

        self._broadcast_eof_message_to_other_worker_instances(client_id, data.packets, data.origin_worker_prefix)

        #Si soy lider, chequear si se alcanzaron todos los EOFs necesarios para ese cliente y en ese caso iniciar proceso de consenso EOF
        if self._im_leader():
            self._check_and_start_eof_consensus_if_applicable(client_id)
        
    
    #Funcion a llamarse para enviar EOF_MESSAGE a la capa siguiente una vez que se recibieron todos los EOFs de las colas/exchanges de entrada
    def send_eof_message_to_next_stage(self, client_id):
        pass

    def process_eof_control_message(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        match message.type:
            case message_protocol.internal.InternalMessageType.EOF_MESSAGE:
                logging.debug(f"Received EOF_MESSAGE for client {message.source_client_uuid}")
                self.on_input_queue_eof_reception(message.source_client_uuid,message.data)

        ack()




    #FUNCIONES RELACIONADAS A CONSENSO DE EOF ENTRE INSTANCIAS DE UN MISMO WORKER
    def _check_and_start_eof_consensus_if_applicable(self, client_id):
        with self.eofs_received_by_client_lock:
            eof_prefixes_received = self.eofs_received_by_client.get(client_id, set())
        if len(eof_prefixes_received) == self.input_eofs_quantities:
            logging.debug(f"Leader {self.prefix_worker} ha recibido todos los EOFs de las colas de entrada para el cliente {client_id}. Iniciando consenso de EOF.")
            if not self.is_single_instance():
                self._mark_eof_consensus_pending_for_client(client_id)
                self._send_eof_consensus_request(client_id)
            else:
                # Si es una sola instancia, no necesito hacer consenso, directamente proceso como si ya se hubiera hecho
                self.execute_on_total_ok_eof_reception_for_client(client_id)

    def _mark_eof_consensus_pending_for_client(self, client_id):
        with self.eof_consensus_pending_by_client_lock:
            self.eof_consensus_pending_by_client.add(client_id)


    # Ejecutar callback para que el worker realice las acciones necesarias al recibir todos los EOF de la capa anterior 
    # (si es que hace falta hacer algo antes de recibir todos los EOFs)
    def execute_on_total_ok_eof_reception_for_client(self, client_id):
        if self.on_total_eof_reception_for_client_callback is not None:
            self.on_total_eof_reception_for_client_callback(client_id)
            
        
    #FUNCIONES PRIVADAS DE MENSAJERIA
    def _broadcast_eof_message_to_other_worker_instances(self, client_id, total_packets, origin_worker_prefix):
        if not self.is_single_instance():
            return
        with self.eof_exchange_producer_fanout_lock:
            self.eof_exchange_producer_fanout.send(MessageHandler.serialize_eof_message(client_id, total_packets, origin_worker_prefix))

    def _send_eof_consensus_request(self, client_id):
        with self.eof_exchange_producer_fanout_lock:
            # se sobreentiende que si yo soy el lider el resto son mismo workers objetivo
            self.eof_exchange_producer_fanout.send(MessageHandler.serialize_eof_consensus_request_message(client_id))
        logging.debug(f"Leader {self.prefix_worker} sent EOF_CONSENSUS_REQUEST for client {client_id}")

    def _stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        consumers = [
            self.usd_filter_q3_queue,
            self.average_per_pay_format_to_filter_exchange_consumer,
        ]
        if self.eof_exchange_consumer is not None:
            consumers.append(self.eof_exchange_consumer)

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [
            self.usd_filter_q3_queue,
            self.average_per_pay_format_to_filter_exchange_consumer,
        ]
        if self.eof_exchange_consumer is not None:
            resources.append(self.eof_exchange_consumer)
        if self.gateway_final_query_queue is not None:
            resources.append(self.gateway_final_query_queue)
        if self.eof_exchange_producer_fanout is not None:
            resources.append(self.eof_exchange_producer_fanout)

        for resource in resources:
            try:
                resource.close()
            except Exception as e:
                logging.error(f"Error closing resource: {e}")

    def _run_control_consumer(self):
        try:
            self.eof_exchange_consumer.start_consuming(self.process_eof_control_message)
        except Exception as e:
            logging.error(f"{self.prefix_worker.replace('_', ' ').title()} Control Consumer Crashed: {e}")
            self._runtime_error = True
            self._stop()
    
    def start(self):

        if self.amount_workers > 1:
            control_thread = threading.Thread(
                target=self._run_control_consumer,
                name=f"{self.worker_prefix.replace('_', '-')}-control-consumer-thread",
            )

        control_started = False

        try:
            if self.amount_workers > 1:
                control_thread.start()
                control_started = True

        except Exception as e:
            logging.error(e)
            self._stop()
            self._close_resources()
            return 2

        if control_started:
            control_thread.join()

        self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return 1

        return 0

    '''
    def _process_eof_average_per_pay_format(self, client_id):
        with self.all_averages_received_for_client_lock: #actualizo que ya tengo todas las medias para el cliente, 
            self.all_averages_received_for_client[client_id] = True
        
        # Procesar los pendientes que tenga guardados en el CSV para ese cliente
        pending_transactions = self.csv_file_manager.read_all_transactions(client_id)
        for pending_transaction, data_id in pending_transactions:
            self._filter_data_with_averages(client_id, data_id, pending_transaction)
        
        with self._eof_counter_lock: #actualizo eofs
            self._eof_counter_by_client[client_id] = self._eof_counter_by_client.get(client_id, 0) + 1

        with self._eof_counter_lock:
            obtenidosDosEofs = self._eof_counter_by_client[client_id] == 2
        if obtenidosDosEofs:
            with self._inflight_message_lock:
                if self._inflight_messages.get(client_id, 0) > 0:
                    logging.debug(f"EOF received for client {client_id} from averages but there are still inflight messages. Marking client as finalized but waiting for inflight messages to finish.")
                    with self._is_pending_to_finalize_client_lock:
                        self._is_pending_to_finalize_client.add(client_id)
                else:
                    logging.debug(f"EOF received for client {client_id} from averages and no inflight messages. Finalizing client.")
                    self._finalize_client(client_id)
                        
    def send_final_eof(self, client_id):
        self.gateway_final_query_queue.send(AmountFilterQ3MessageHandler.serialize_eof_message(client_id))
        logging.info(f"Sent final EOF for client {client_id} to gateway final query queue")
    
    def _process_usd_filter_q3_eof(self, client_id):
        logging.debug(f"Received EOF for client {client_id}")

        with self._eof_counter_lock:
            self._eof_counter_by_client[client_id] = self._eof_counter_by_client.get(client_id, 0) + 1

        if self.amount_workers > 1:
            with self._eof_producer_lock:
                self.amount_filter_eof_exchange_producer.send(AmountFilterQ3MessageHandler.serialize_eof_message(client_id))
            logging.debug(f"Sent EOF for client {client_id} to other amount filters")

        with self.all_averages_received_for_client_lock:
            averages_received = client_id in self.all_averages_received_for_client

        # Check if there are pending transactions in the CSV file
        pending_transactions = self.csv_file_manager.read_all_transactions(client_id)
        is_pending_data_to_send = len(pending_transactions) > 0

        if averages_received and not is_pending_data_to_send:
            self._finalize_client(client_id)
    
    def _check_and_finalize_client_if_pending(self, client_id):
        should_finalize = False

        with self._is_pending_to_finalize_client_lock:
            is_pending = client_id in self._is_pending_to_finalize_client

        if is_pending:
            with self._inflight_message_lock:
                should_finalize = self._inflight_messages.get(client_id, 0) == 0

        if should_finalize:
            logging.debug(f"Finalizando cliente {client_id} que estaba pendiente")
            self._finalize_client(client_id)
                        
    def _finalize_client(self, client_id):

        with self._finalized_clients_lock:
            if client_id in self._finalized_clients:
                return
            logging.debug(f"Finalizando cliente {client_id}")
            self._finalized_clients.add(client_id)

        # Clean up CSV file after client is finalized
        self.csv_file_manager.delete_csv_file(client_id)

        if self._is_leader():
            self._leader_count_eof_for_client(client_id)
        else:
            self.send_eof_leader_message(client_id)

        with self._is_pending_to_finalize_client_lock:
            if client_id in self._is_pending_to_finalize_client:
                self._is_pending_to_finalize_client.remove(client_id)
        

    def send_eof_leader_message(self, client_id):
        with self._eof_producer_lock:
            self.amount_filter_eof_exchange_producer.send(AmountFilterQ3MessageHandler.serialize_eof_leader_message(client_id))
        logging.debug(f"Sent EOF_LEADER_MESSAGE for client {client_id} to leader")
        
    def _leader_count_eof_for_client(self, client_id):
        should_send_final_eof = False
        with self._leader_eof_lock:
            self.total_eof_received_by_client[client_id] = self.total_eof_received_by_client.get(client_id, 0) + 1
            
            if self.total_eof_received_by_client[client_id] == self.amount_workers:
                logging.debug(f"Leader ha recibido EOF de todos los filtros para el cliente {client_id}. Enviando EOF a la capa siguiente.")
                should_send_final_eof = True
                del self.total_eof_received_by_client[client_id]
        
        if should_send_final_eof:
            self.send_final_eof(client_id)
            
            
    def _process_eof_from_control_exchange(self, client_id):
        with self._eof_counter_lock:
            self._eof_counter_by_client[client_id] = self._eof_counter_by_client.get(client_id, 0) + 1
            if self._eof_counter_by_client[client_id] < 2:
                return

        with self._inflight_message_lock:
            if self._inflight_messages.get(client_id, 0) > 0:
                logging.debug(f"EOF received for client {client_id} but there are still inflight messages. Marking client as finalized but waiting for inflight messages to finish.")
                with self._is_pending_to_finalize_client_lock:
                    self._is_pending_to_finalize_client.add(client_id)
            else:
                logging.debug(f"EOF received for client {client_id} and no inflight messages. Finalizing client.")
                self._finalize_client(client_id)

'''



