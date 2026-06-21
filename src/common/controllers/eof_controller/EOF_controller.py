


import threading
import logging
from time import sleep

from common import message_protocol, middleware
from common.controllers.eof_controller.message_handler.message_handler import MessageHandler
from common.controllers.eof_controller.types import ClientEOFState
from common.message_protocol.internal import EOFData, InternalMessageType, deserialize
from common.controllers.eof_controller.types import partial_count_by_worker_prefix, partial_count_by_worker_prefix_and_id, total_count_by_prefix

class EOFController:

    
    BACKOFF_TIME_SECONDS_BEFORE_RESENDING_CONSENSUS_REQUEST = 2

    #todo: VAS A TENER QUE HACER FUNCIONES CALLBACK PARA EL ENVIO PROPIO DEL EOF FINAL Y LA RECEPCION DEL EOF INICIAL?

    def __init__(self, mom_host, id_worker, prefix_worker, amount_workers,eof_control_exchange_name,input_eofs_quantities,on_consensus_ok_callback,on_send_eof_to_next_stage_callback,auxiliary_input_data=False):

        
        self.id = int(id_worker)
        self.amount_workers = int(amount_workers)
        self.mom_host = mom_host
        self.prefix_worker : str = prefix_worker
        self.eof_control_exchange_name : str = eof_control_exchange_name
        self.input_eofs_quantities = input_eofs_quantities
        self.auxiliary_input_data = auxiliary_input_data

        # Por cliente, set de prefixes de los cuales se recibieron EOFs
        self.eofs_received_by_client_lock = threading.Lock()
        self.eofs_received_by_client : dict[str, set] = {} 

        # Por cliente, set de prefixes de los cuales se recibieron EOFs
        self.postprocess_received_by_client_lock = threading.Lock()
        self.postprocess_received_by_client : dict[str, set] = {} #clave el cliente y valor un set con los IDs de esta instancia


        # Totalizadores de EOF de la capa anterior informados por cliente
        self.total_packets_received_by_client_lock = threading.Lock()
        self.total_packets_received_by_client : dict[str, total_count_by_prefix] = {} 

        # Parciales de procesamiento de paquetes de esta instancia
        self.packets_processed_by_client_lock = threading.Lock()
        self.packets_processed_by_client : dict[str, partial_count_by_worker_prefix] = {}

        # Parcial de consenso - usados solo por el lider
        self.consensus_partial_count_by_client_lock = threading.Lock()
        self.consensus_partial_count_by_client : dict[str, partial_count_by_worker_prefix_and_id] = {}


        # FUNCIONES DE CALLBACK A CONECTAR SI O SI. PUEDEN VENIR COMO None SI NO HACE FALTA
        self.on_consensus_ok_reception_for_client_callback = on_consensus_ok_callback
        self.on_send_eof_to_next_stage_callback = on_send_eof_to_next_stage_callback #se necesita para desde el hilo principal enviar el mensaje por el producer correcto

        if not self.is_single_instance():
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
            self.eof_exchange_producer_to_leader = middleware.MessageMiddlewareExchangeRabbitMQ(
                    self.mom_host,
                    self.eof_control_exchange_name,
                    [f"{self.prefix_worker}_0"], #el lider es el worker con id 0, entonces le envio directo a su cola de consumo
                )
            #TODO: si tuviese que hacer la elección de lider, debería comenzar al final de la inicializacion.
            #el eof_exchange_producer_to_leader quedaría en None hasta poder establecer la routing key del nodo lider
            ##el algoritmo a utilizar debería ser
            
        self.eof_client_state_lock = threading.Lock()
        self.eof_client_state : dict[str, ClientEOFState] = {} 
        


        self._stop_lock = threading.Lock()
        self._stopping = False

    
    def _im_leader(self):
        return self.id == 0
    
    def is_single_instance(self):
        return self.amount_workers == 1


    #FUNCIONES PARA CONECTAR CON AFUERA

    # Funcion que suma la cantidad de paquetes procesados por esta instancia. Debe llamarse desde el consumer principal, al procesar cada mensaje de datos
    # para ir llevando la cuenta de cuantos paquetes se procesaron por cliente y por flujo (en caso de haber mas de un flujo de entrada)
    def on_processed_packet_by_client(self, client_id):
        with self.packets_processed_by_client_lock:
            self.packets_processed_by_client.setdefault(client_id, {}).setdefault(self.prefix_worker, 0)
            self.packets_processed_by_client[client_id][self.prefix_worker] += 1 #TODO: no tiene validacion de ventana para sumar paquetes

    #Funcion a llamarse para cuando desde alguna de las colas/exchanges de entrada se reciba un EOF. Usar tambien desde fuera del controller para la cola de carga
    def on_input_queue_eof_reception(self, client_id,data : EOFData):
        ClientEOFState.mark_client_as_active(client_id, self.eof_client_state, self.eof_client_state_lock)

        #Sumar el EOF al set de EOFs recibidos para ese cliente. como clave debe ir el prefix que viaja en el tipo de mensaje EOF_MESSAGE
        with self.eofs_received_by_client_lock:
            self.eofs_received_by_client.setdefault(client_id, set())
            self.eofs_received_by_client[client_id].add(data.origin_worker_prefix)
        
        #Agregar el total de paquetes informados por ese EOF al total de paquetes recibidos para ese cliente
        with self.total_packets_received_by_client_lock:
            self.total_packets_received_by_client.setdefault(client_id, {}).setdefault(data.origin_worker_prefix, (data.amount_origin_workers, data.total_packets))
            #Nota: se usa setdefault sin problema porque nunca va a pasar de recibir 2 eofs con totalizaciones diferentes provenientes del mismo flujo

        

        self._broadcast_eof_message_to_other_worker_instances(client_id, data.packets, data.origin_worker_prefix, data.amount_origin_workers)

        #Si soy lider, chequear si se alcanzaron todos los EOFs necesarios para ese cliente y en ese caso iniciar proceso de consenso EOF
        self._check_and_start_eof_consensus_if_applicable(client_id)






    # Consumer de mensaje recibidos ya sea por el lider o no lider, para procesar los mensajes de control de EOF.
    def _process_eof_control_message(self, message, ack, nack):
        try:
            message = deserialize(message)
            match message.type:
                case InternalMessageType.EOF_MESSAGE:
                    logging.debug(f"Received EOF_MESSAGE for client {message.source_client_uuid}")
                    self.on_input_queue_eof_reception(message.source_client_uuid,message.data)
                case InternalMessageType.EOF_CONSENSUS_REQUEST:
                    logging.debug(f"Received EOF_CONSENSUS_REQUEST for client {message.source_client_uuid}")
                    self._process_eof_consensus_request(message.source_client_uuid)
                case InternalMessageType.EOF_CONSENSUS_RESPONSE:
                    logging.debug(f"Received EOF_CONSENSUS_RESPONSE for client {message.source_client_uuid}")
                    self._process_eof_consensus_response(message.source_client_uuid, message.data)
                case InternalMessageType.EOF_CONSENSUS_OK:
                    logging.debug(f"Received EOF_CONSENSUS_OK for client {message.source_client_uuid}")
                    self._process_eof_consensus_ok(message.source_client_uuid)
                case InternalMessageType.EOF_CONSENSUS_FAIL:
                    logging.debug(f"Received EOF_CONSENSUS_FAIL for client {message.source_client_uuid}")
                    self._process_eof_consensus_fail(message.source_client_uuid)
                case InternalMessageType.EOF_POST_CONSENSUS_OK:
                    logging.debug(f"Received EOF_POST_CONSENSUS_OK for client {message.source_client_uuid}")
                    self._process_eof_post_consensus_ok(message.source_client_uuid,message.data)
            ack()
        except Exception as e:
            logging.error(f"Error processing EOF control message: {e}")
            nack()




    #FUNCIONES RELACIONADAS A CONSENSO DE EOF ENTRE INSTANCIAS DE UN MISMO WORKER
    def _check_and_start_eof_consensus_if_applicable(self, client_id):
        with self.eofs_received_by_client_lock:
            eof_prefixes_received = self.eofs_received_by_client.get(client_id, set())
        if len(eof_prefixes_received) == self.input_eofs_quantities:
            if self._im_leader():
                if not self.is_single_instance():
                    logging.debug(f"Leader de {self.prefix_worker} ha recibido todos los EOFs de las colas de entrada para el cliente {client_id}. Iniciando consenso de EOF.")
                    ClientEOFState.mark_client_as_pending_eof(client_id, self.eof_client_state, self.eof_client_state_lock)
                    self._send_eof_consensus_request(client_id)
                else:
                    logging.debug(f"Worker {self.prefix_worker}-{self.id} ha recibido todos los EOFs de las colas de entrada para el cliente {client_id}. No hay consenso necesario, procesando como si ya se hubiera hecho.")
                    # Si es una sola instancia, no necesito hacer consenso, directamente proceso como si ya se hubiera hecho
                    self.execute_on_total_ok_eof_reception_for_client(client_id)
                    ClientEOFState.mark_client_as_eof_consensus_achieved(client_id, self.eof_client_state, self.eof_client_state_lock)
            else:
                # Si no soy lider, marco que estoy pendiente de consenso y espero a que el lider me mande la respuesta
                ClientEOFState.mark_client_as_pending_eof(client_id, self.eof_client_state, self.eof_client_state_lock)
                logging.debug(f"Worker {self.prefix_worker}-{self.id} ha recibido todos los EOFs de las colas de entrada para el cliente {client_id}. Marcando como pendiente de consenso de EOF y esperando respuesta del lider.")


    # Ejecutar callback para que el worker realice las acciones necesarias al recibir todos los EOF de la capa anterior 
    # (si es que hace falta hacer algo antes de enviar los EOFs a la capa siguiente, como eviar datos acumulados)
    def execute_on_total_ok_eof_reception_for_client(self, client_id):
        if self.on_consensus_ok_reception_for_client_callback is not None:
            self.on_consensus_ok_reception_for_client_callback(client_id)

    def _accumulate_consensus_partials(self, client_id, data):
        with self.consensus_partial_count_by_client_lock:
            self.consensus_partial_count_by_client.setdefault(client_id, {}).setdefault(data.origin_worker_prefix_flux_1, {}).setdefault(data.worker_id_sending_partials, 0)
            self.consensus_partial_count_by_client[client_id][data.origin_worker_prefix_flux_1][data.worker_id_sending_partials] = data.partial_packets_count_flux_1

            if data.partial_packets_count_flux_2 != None:
                self.consensus_partial_count_by_client.setdefault(client_id, {}).setdefault(data.origin_worker_prefix_flux_2, {}).setdefault(data.worker_id_sending_partials, 0)
                self.consensus_partial_count_by_client[client_id][data.origin_worker_prefix_flux_2][data.worker_id_sending_partials] = data.partial_packets_count_flux_2

    def _have_received_all_consensus_partials_for_client(self, client_id):
        with self.consensus_partial_count_by_client_lock:
            partials_by_client = self.consensus_partial_count_by_client.get(client_id, {})
        
        flujo_tiene_todos_los_parciales = []
        for origin_worker_prefix, partials_by_id in partials_by_client.items():
            if len(partials_by_id) == self.amount_workers:
                flujo_tiene_todos_los_parciales.append(True)
            else:
                flujo_tiene_todos_los_parciales.append(False)
        return len(flujo_tiene_todos_los_parciales) > 0 and all(flujo_tiene_todos_los_parciales)

    def _totalizer_has_achieved_consensus_for_client(self, client_id):
        flux_achieved_consensus = []

        if (not self.auxiliary_input_data):
            # sumo todos los valores de los parciales de ambos prefixes, de todos los ids. 
            # Si es igual al total informado por los EOFs de los (1 o 2) flujos de entrada, entonces se alcanzo consenso. Si no, no se alcanzo consenso.
            total_packets_processed_in_all_fluxes = 0
            total_packets_informed_by_eofs_for_this_flux = 0

            with self.consensus_partial_count_by_client_lock:
                partials_by_client = self.consensus_partial_count_by_client.get(client_id, {})

            for prefix_flux, partials_by_id in partials_by_client.items():
                total_packets_processed_in_this_flux = sum(partials_by_id.values())
                total_packets_processed_in_all_fluxes += total_packets_processed_in_this_flux

                with self.total_packets_received_by_client_lock:
                    total_packets_informed_by_eofs_for_this_flux += self.total_packets_received_by_client.get(client_id, {}).get(prefix_flux, (0,0))[1]

            if total_packets_informed_by_eofs_for_this_flux == total_packets_processed_in_all_fluxes:
                flux_achieved_consensus.append(True)
            else:
                flux_achieved_consensus.append(False)
            
        else:

            total_packets_processed_in_flux_1 = 0
            total_packets_informed_by_eofs_for_flux_1 = 0
            total_packets_processed_in_flux_2 = 0
            total_packets_informed_by_eofs_for_flux_2 = 0

            with self.consensus_partial_count_by_client_lock:
                partials_by_client = self.consensus_partial_count_by_client.get(client_id, {})

            for prefix_flux, partials_by_id in partials_by_client.items():
                if prefix_flux == "average_per_pay_format_joiner":
                    # Para el prefix 2 (el auxiliar), el que tiene nombre de prefix "AVERAGE_PER_APY_JOINER" o algo así, me fijo que todos los parciales 
                    # tengan el mismo valor, y ese valor sea igual a la cantidad de paquetes procesados que yo tengo registrados para ese flujo

                    with self.total_packets_received_by_client_lock:
                        total_packets_informed_by_eofs_for_flux_2 += self.total_packets_received_by_client.get(client_id, {}).get(prefix_flux, (0,0))[1]
                    
                    for _, count_total_received_from_exchange in partials_by_id.items():
                        total_packets_processed_in_flux_2 = count_total_received_from_exchange
                        if total_packets_processed_in_flux_2 != total_packets_informed_by_eofs_for_flux_2:
                            flux_achieved_consensus.append(False)
                        else:
                            flux_achieved_consensus.append(True)

                else:
                    # sumo todos los valores de los parciales del prefix 1.
                    # Me fijo si es igual al total informado por los EOFs de los (1 o 2) flujos de entrada
                    total_packets_processed_in_flux_1 += sum(partials_by_id.values())

                    with self.total_packets_received_by_client_lock:
                        total_packets_informed_by_eofs_for_flux_1 += self.total_packets_received_by_client.get(client_id, {}).get(prefix_flux, (0,0))[1]

                    if total_packets_informed_by_eofs_for_flux_1 == total_packets_processed_in_flux_1:
                        flux_achieved_consensus.append(True)
                    else:
                        flux_achieved_consensus.append(False)

        return len(flux_achieved_consensus) > 0 and all(flux_achieved_consensus)


    def _update_leader_own_partials(self,client_id):
        # El lider tambien debe actualizar sus propios parciales en la estructura de consenso, para poder alcanzar consenso con sus propios datos
        with self.packets_processed_by_client_lock:
            leader_partials = self.packets_processed_by_client.get(client_id, {})
        
        for prefix_flux, partial_count in leader_partials.items():
            #el partial_count es de datatype number de paquetes procesados por el lider para ese cliente y ese prefix de flujo.
            with self.consensus_partial_count_by_client_lock:
                self.consensus_partial_count_by_client.setdefault(client_id, {}).setdefault(prefix_flux, {}).setdefault(self.id, 0)
                self.consensus_partial_count_by_client[client_id][prefix_flux][self.id] = partial_count


    #FUNCIONES DE PROCESAMIENTO DE MENSAJES RECIBIDOS POR EL CONSUMER DE CONTROL DE EOF

    #le llega a los no lider
    def _process_eof_consensus_request(self, client_id):
        #Al comenzar, actualizo mi estado a PENDING_EOF_IN_CONSENSUS_REQUEST_SENT
        ClientEOFState.mark_client_as_pending_eof_in_consensus_request_sent(client_id, self.eof_client_state, self.eof_client_state_lock)
        #Al recibir esto, intento enviar mis parciales propios. 
        self._send_eof_consensus_response(client_id)

        #Cuando termino de enviar actualizo mi estado a PENDING_EOF_IN_CONSENSUS_RESPONSE_SENT
        ClientEOFState.mark_client_as_pending_eof_in_consensus_response_sent(client_id, self.eof_client_state, self.eof_client_state_lock)
    
    #le llega al lider, con los parciales de las otras instancias
    def _process_eof_consensus_response(self, client_id, data):
        # Va acumulando los parciales que le llegan del cliente, con ese prefix del flujo origen y ID del worker que envio la data
        self._accumulate_consensus_partials(client_id, data)
        # Cuando los totalizadores alcanzan consenso manda ok. Si no se alcanza el total y encima se obtuvieron totales de todas las instancias, se manda fail
        if (self._totalizer_has_achieved_consensus_for_client(client_id)):
            self._send_eof_consensus_ok_message(client_id)
            # Ejecución en el lider del post-consenso
            self.execute_on_total_ok_eof_reception_for_client(client_id)
            with self.postprocess_received_by_client_lock:
                self.postprocess_received_by_client.setdefault(client_id, set())
                self.postprocess_received_by_client[client_id].add(self.id)
        elif (self._have_received_all_consensus_partials_for_client(client_id)):
            self._send_eof_consensus_fail_message(client_id)
            self._clear_consensus_partials_for_client(client_id) #limpiar los parciales que tengo acumulados para consenso de ese cliente, para esperar sus parciales correspondientes nuevos
            self._ask_to_resend_eof_consensus_request(client_id) #realizar un proceso de espera de n segundos y volver a enviar EOF_CONSENSUS_REQUEST con _send_eof_consensus_request
        
    #le llega a los no lider, con la respuesta del lider de si se alcanzo consenso o no
    def _process_eof_consensus_ok(self, client_id):
        # Si recibo esto, significa que se alcanzo consenso. Actualizo mi estado a EOF_CONSENSUS_ACHIEVED
        ClientEOFState.mark_client_as_eof_consensus_achieved(client_id, self.eof_client_state, self.eof_client_state_lock)
        # Luego debere ejecutar un callback de posproceso, si es que esta definido
        self.execute_on_total_ok_eof_reception_for_client(client_id)
        
        self._send_eof_post_consensus_ok_message(client_id)

    
    def _process_eof_consensus_fail(self, client_id):
        # Si recibo esto, significa que no se alcanzo consenso. Actualizo mi estado a PENDING_EOF y me quedo esperando al lider
        ClientEOFState.mark_client_as_pending_eof(client_id, self.eof_client_state, self.eof_client_state_lock)

    # le llega al lider, luego de que finalicen tareas post consenso
    def _process_eof_post_consensus_ok(self, client_id, postconsensus_worker_id):
        # Actualizo el estado de la variable postprocess_received_by_client para ese cliente, agregando el ID del postconsenso que se realizó
        with self.postprocess_received_by_client_lock:
            self.postprocess_received_by_client.setdefault(client_id, set())
            self.postprocess_received_by_client[client_id].add(postconsensus_worker_id)
        # Cuando tengo tantos elementos en ese set como cantidad de instancias el worker - 1 (lo tenes en self.amount_workers)
        # entonces se que ya se realizaron todos los procesos post consenso de todas las instancias,
        # Entonces envío el mensaje EOF_MESSAGE a la capa siguiente, usando la función send_eof_message_to_next_stage
        if len(self.postprocess_received_by_client[client_id]) == self.amount_workers:
            self.on_send_eof_to_next_stage_callback(client_id)
            # Actualizo el estado del cliente a EOF_FINISH_ENABLED, que es el estado que habilita el envio de los EOFs a la capa siguiente
            ClientEOFState.mark_client_as_eof_finish_enabled(client_id, self.eof_client_state, self.eof_client_state_lock)
            self.send_eof_message_to_next_stage(client_id)
        


        
    #FUNCIONES PRIVADAS DE ENVIO DE MENSAJERIA
    def _broadcast_eof_message_to_other_worker_instances(self, client_id, total_packets, origin_worker_prefix, amount_origin_workers):
        if self.is_single_instance():
            return
        with self.eof_exchange_producer_fanout_lock:
            self.eof_exchange_producer_fanout.send(MessageHandler.serialize_eof_message(client_id, total_packets, origin_worker_prefix, amount_origin_workers))

    #llamarse solo si ya se pregunto im_leader()
    def _send_eof_consensus_request(self, client_id):
        with self.eof_exchange_producer_fanout_lock:
            # se sobreentiende que si yo soy el lider el resto son de mi mismo workers objetivo y por eso uso el fanout
            self.eof_exchange_producer_fanout.send(MessageHandler.serialize_eof_consensus_request_message(client_id))
        logging.debug(f"Leader {self.prefix_worker} sent EOF_CONSENSUS_REQUEST for client {client_id}")
        self._update_leader_own_partials(client_id) #actualizo mis propios parciales para ese cliente, antes de que me lleguen los parciales de las otras instancias y poder alcanzar consenso
        ClientEOFState.mark_client_as_pending_eof_in_consensus_request_sent(client_id, self.eof_client_state, self.eof_client_state_lock)

    #Funcion a llamarse para enviar EOF_MESSAGE a la capa siguiente una vez que se recibieron todos los EOFs de las colas/exchanges de entrada
    def send_eof_message_to_next_stage(self, client_id):
        
        if self.on_send_eof_to_next_stage_callback is not None:
            self.on_send_eof_to_next_stage_callback(client_id)
        #al final, limpiar toda la informacion que haya de ese cliente 
        #TODO: cuando limpia los no lideres? Cuando pasan a estado 5
        self._clear_all_client_data(client_id)

    # Funcion a llamarse por NO LIDER para enviar la respuesta del consenso de EOF al lider, con el resultado del conteo de los parciales
    def _send_eof_consensus_response(self, client_id):
        with self.eof_exchange_producer_to_leader_lock:
            # Si no tengo parciales para enviar de esos flujos, envio un parcial con valor 0, para que el lider sepa que no tengo nada procesado de ese flujo                
            self.eof_exchange_producer_to_leader.send(MessageHandler.serialize_eof_consensus_response_message(client_id, self.id, self.auxiliary_input_data, self.packets_processed_by_client, self.packets_processed_by_client_lock))

    def _send_eof_consensus_ok_message(self, client_id):
        # Si se alcanzo consenso, cambio estado a EOF_CONSENSUS_ACHIEVED
        with self.eof_exchange_producer_fanout_lock:
            # se sobreentiende que si yo soy el lider el resto son de mi mismo workers objetivo y por eso uso el fanout
            self.eof_exchange_producer_fanout.send(MessageHandler.serialize_eof_consensus_ok_message(client_id))
        
        ClientEOFState.mark_client_as_eof_consensus_achieved(client_id, self.eof_client_state, self.eof_client_state_lock)

    def _send_eof_consensus_fail_message(self, client_id):
        # cambio estado a PENDING_EOF (para reiniciar el proceso y esperar a que me vuelvan a llegar los EOFs de entrada, porque algo fallo en el medio)
        with self.eof_exchange_producer_fanout_lock:
            # se sobreentiende que si yo soy el lider el resto son de mi mismo workers objetivo y por eso uso el fanout
            self.eof_exchange_producer_fanout.send(MessageHandler.serialize_eof_consensus_failed_message(client_id))
        ClientEOFState.mark_client_as_pending_eof(client_id, self.eof_client_state, self.eof_client_state_lock)

    def _ask_to_resend_eof_consensus_request(self, client_id):
        # Espero n segundos y vuelvo a enviar el mensaje de EOF_CONSENSUS_REQUEST para ese cliente
        sleep(self.BACKOFF_TIME_SECONDS_BEFORE_RESENDING_CONSENSUS_REQUEST)
        self._send_eof_consensus_request(client_id)
    
    def _send_eof_post_consensus_ok_message(self, client_id):
        with self.eof_exchange_producer_to_leader_lock:
            self.eof_exchange_producer_to_leader.send(MessageHandler.serialize_eof_post_consensus_ok_message(client_id, self.id))
        ClientEOFState.mark_client_as_eof_finish_enabled(client_id, self.eof_client_state, self.eof_client_state_lock)
        self._clear_all_client_data(client_id) #limpiar toda la informacion que haya de ese cliente, para liberar memoria lo antes posible
    
    def _clear_consensus_partials_for_client(self, client_id):
        with self.consensus_partial_count_by_client_lock:
            if client_id in self.consensus_partial_count_by_client:
                del self.consensus_partial_count_by_client[client_id]

    def _clear_all_client_data(self, client_id):
        with self.eofs_received_by_client_lock:
            if client_id in self.eofs_received_by_client:
                del self.eofs_received_by_client[client_id]
        with self.total_packets_received_by_client_lock:
            if client_id in self.total_packets_received_by_client:
                del self.total_packets_received_by_client[client_id]
        with self.packets_processed_by_client_lock:
            if client_id in self.packets_processed_by_client:
                del self.packets_processed_by_client[client_id]
        with self.consensus_partial_count_by_client_lock:
            if client_id in self.consensus_partial_count_by_client:
                del self.consensus_partial_count_by_client[client_id]
        with self.postprocess_received_by_client_lock:
            if client_id in self.postprocess_received_by_client:
                del self.postprocess_received_by_client[client_id]


    def _stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        consumers = [
            self.eof_exchange_consumer,
        ]

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")

    def _close_resources(self):
        resources = [
            self.eof_exchange_consumer,
            self.eof_exchange_producer_fanout,
            self.eof_exchange_producer_to_leader
        ]
        for resource in resources:
            try:
                resource.close()
            except Exception as e:
                logging.error(f"Error closing resource: {e}")

    def _run_control_consumer(self):
        try:
            self.eof_exchange_consumer.start_consuming(self._process_eof_control_message)
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
