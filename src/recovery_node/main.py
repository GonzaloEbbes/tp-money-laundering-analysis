import hashlib
import os
import logging
import signal
import sys
import threading
import subprocess
from time import sleep, monotonic

from common import middleware, message_protocol
from common.controllers.healthcheck.health_checking_message_handler.message_handler import HealthCheckingMessageHandler
from common.controllers.healthcheck.recovery_controller import RecoveryController
from common.controllers.healthcheck.utils import recovery_node_id_responsible_of_recovery
from common.logging import configure_logging_from_env
from common.message_protocol.internal import HealthCheckData

ID = os.environ.get("ID")
MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
RECOVERY_PREFIX = os.environ.get("RECOVERY_PREFIX", "recovery")
RECOVERY_AMOUNT = int(os.environ.get("RECOVERY_AMOUNT", "1"))
HEARTBEAT_EXCHANGE = os.environ.get("HEARTBEAT_EXCHANGE", "heartbeat_exchange")

HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "2")) # debe ser 2 o mas
MAX_HEARTBEAT_MISSES = int(os.environ.get("MAX_HEARTBEAT_MISSES", "4"))

if MAX_HEARTBEAT_MISSES < 2:
    raise ValueError("MAX_HEARTBEAT_MISSES must be >= 2")

HEARTBEAT_TIMEOUT_SECS = MAX_HEARTBEAT_MISSES * HEARTBEAT_INTERVAL
RECOVERY_GRACE_SECS = HEARTBEAT_TIMEOUT_SECS
TTL_MESSAGE = HEARTBEAT_INTERVAL * (MAX_HEARTBEAT_MISSES - 1) * 1000 # en milisegundos
DOCKER_RESTART_TIMEOUT_SECS = int(os.environ.get("DOCKER_RESTART_TIMEOUT_SECS", "15"))
ALL_MONITORED_CONTAINERS = [
    name.strip()
    for name in os.environ.get("MONITORED_CONTAINERS", "").split(",")
    if name.strip()
] #incluye los recovery_nodes. Es una tira strin separada por comas

class RecoveryNode: 

    def __init__(self):

        self.id = int(ID)
        
        self.heartbeat_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST,HEARTBEAT_EXCHANGE,
            [
                HEARTBEAT_EXCHANGE,          # broadcast de heartbeats
                f"{RECOVERY_PREFIX}_{ID}",   # mensajes dedicados a este recovery
            ],
            queue_name=f"{RECOVERY_PREFIX}_{ID}",
            exclusive=False,
            queue_arguments={
                "x-message-ttl": TTL_MESSAGE,
                "x-max-length": 1000,
                "x-overflow": "drop-head",
            },)

        self.recovery_producer_controller = RecoveryController(
            mom_host=MOM_HOST,
            heartbeat_exchange=HEARTBEAT_EXCHANGE,
            id=ID,
            prefix=RECOVERY_PREFIX,
            recovery_prefix=RECOVERY_PREFIX,
            recovery_amount=RECOVERY_AMOUNT,
            heartbeat_interval=HEARTBEAT_INTERVAL,
        ) 

        self.producer_lock = threading.Lock()

        self.state_lock = threading.Lock()
        self.last_heartbeat_received : dict[int, float] = {}

        self.workers_currently_reseting : dict[str, float] = {}

        self._sigterm_received = False
        self._runtime_error = False
        self._stop_lock = threading.Lock()
        self._stopping = False

        self._build_data()

    def _build_data(self):
        #del listado de contenedores totales a monitorear, se realiza un shardeo determinista
        #luego, se agrega ademas el recovery node a monitorear
        monitored_containers = [container for container in ALL_MONITORED_CONTAINERS if recovery_node_id_responsible_of_recovery(self.id, container, RECOVERY_PREFIX, RECOVERY_AMOUNT) == self.id]
        time_mark_now = self.request_time_mark()
        with self.state_lock:
            for container in monitored_containers:
                #En last_heartbeat_received se guardan todos los contenedores que son de este recovery node
                self.last_heartbeat_received[container] = time_mark_now + RECOVERY_GRACE_SECS # inicializo con un tiempo futuro para que no se resetee al iniciar
        self._init_log(monitored_containers)
    
    def _init_log(self,monitored_containers):
        logging.info(f"Recovery node {self.id} monitoring containers:")
        for container in monitored_containers:
            logging.info(f" - {container}")
    
    def _clean_queue(self):
        try:
            self.heartbeat_exchange_consumer.discard_pending_messages_in_exchange_queue()
        except Exception as e:
            logging.error(f"Error discarding pending messages in heartbeat queue: {e}")
    
    def next_ring_node_id(self):
        return (self.id + 1) % RECOVERY_AMOUNT

    def previous_ring_node_id(self):
        return (self.id - 1) % RECOVERY_AMOUNT
    
    def request_time_mark(self):
        return monotonic()

    def process_heartbeat_messages(self, message, ack, nack):
        try:
            message = HealthCheckingMessageHandler.deserialize_healthcheck_message(message)
            match message.type:
                case message_protocol.internal.InternalMessageType.HEARTBEAT_MESSAGE:
                    data = HealthCheckData()
                    data.container_name = message.data["container_name"]
                    self._process_heartbeat(data)
            ack()
        except Exception as e:
            logging.error(f"Error processing heartbeat message: {e}")
            nack()

    def _process_heartbeat(self, data: HealthCheckData):
        container_name = data.container_name
        now = self.request_time_mark()

        with self.state_lock:
            if container_name not in self.last_heartbeat_received:
                logging.debug(f"Heartbeat received from {container_name} at {now}. Updated last seen time. Not mine. DISCARDED")
                return

            recovering_until = self.workers_currently_reseting.get(container_name)

            if recovering_until is not None:
                if now < recovering_until:
                    return

                logging.info(f"Recovery process for {container_name} completed.")
                self.workers_currently_reseting.pop(container_name, None)

            self.last_heartbeat_received[container_name] = now
            logging.debug(f"Heartbeat received from {container_name} at {now}. Updated last seen time.")


    def _run_heartbeat_consumer(self):
        try:
            self.heartbeat_exchange_consumer.start_consuming(self.process_heartbeat_messages)
        except Exception as e:
            self._handle_runtime_failure(e, "Heartbeat consumer crashed")

    def _run_heartbeat_checker(self):
        try:
            while not self._sigterm_received and not self._runtime_error:
                sleep(HEARTBEAT_INTERVAL)
                self._check_heartbeats()
        except Exception as e:
            self._handle_runtime_failure(e, "Heartbeat checker crashed")
            return 2
        if self._runtime_error and not self._sigterm_received:
            return 1
        return 0

    def _check_heartbeats(self):
        now = self.request_time_mark()
        workers_to_reset = []

        with self.state_lock:
            for container_name, last_seen in self.last_heartbeat_received.items():

                recovering_until = self.workers_currently_reseting.get(container_name)

                if recovering_until is not None:
                    if now < recovering_until:
                        continue

                    logging.info(
                        f"Recovery grace expired for {container_name}. Will check heartbeat again."
                    )
                    self.workers_currently_reseting.pop(container_name, None)

                if now - last_seen > HEARTBEAT_TIMEOUT_SECS:
                    workers_to_reset.append(container_name)

        for container_name in workers_to_reset:
            self._reset_worker(container_name)


    def _reset_worker(self, container_name):
        now = self.request_time_mark()

        with self.state_lock:
            last_seen = self.last_heartbeat_received.get(container_name)

            if last_seen is None:
                return

            if container_name in self.workers_currently_reseting:
                return

            if now - last_seen <= HEARTBEAT_TIMEOUT_SECS:
                return

            # Marco "restart en progreso" para evitar doble restart concurrente.
            self.workers_currently_reseting[container_name] = float("inf")

        restarted = self.docker_restart_container(container_name)
        now_after_restart = self.request_time_mark()

        with self.state_lock:
            if restarted:
                self.workers_currently_reseting[container_name] = now_after_restart + RECOVERY_GRACE_SECS
                self.last_heartbeat_received[container_name] = now_after_restart
            else:
                self.workers_currently_reseting.pop(container_name, None)

    def docker_restart_container(self, container_name):
        try:
            logging.info(f"Restarting container {container_name}")

            result = subprocess.run(
                ["docker", "restart", "--time", "15", container_name],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=DOCKER_RESTART_TIMEOUT_SECS,
            )

            if result.returncode != 0:
                logging.error(
                    f"Error restarting {container_name}. "
                    f"returncode={result.returncode}, stderr={result.stderr}"
                )
                return False

            logging.info(f"Container {container_name} restarted successfully.")
            return True

        except subprocess.TimeoutExpired:
            logging.error(f"Timeout restarting container {container_name}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error restarting container {container_name}: {e}")
            return False

    def _handle_runtime_failure(self, error, context):
        logging.error(f"{context}: {error}")
        self._runtime_error = True
        self._stop()

    def _close_resources(self):
        resources = [self.heartbeat_exchange_consumer]
        for resource in resources:
            try:
                resource.close()
            except Exception as e:
                logging.error(f"Error closing resource: {e}")

    def notify_sigterm(self):
        self._sigterm_received = True
        self._stop()
        self.recovery_producer_controller.on_sigterm()

    def _stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        consumers = [self.heartbeat_exchange_consumer]

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error(f"Error stopping consumer: {e}")


    def start(self):

        consumer_thread = threading.Thread(
            target=self._run_heartbeat_consumer,
            name="heartbeat-consumer-thread",
        )

        timer_thread = threading.Thread(
            target=self._run_heartbeat_checker,
            name="heartbeat-timer-thread",
        )

        stop_recovery_controller_callback = None
        processing_thread_started = False
        timer_thread_started = False
        recovery_controller_exit_code = 0

        try:
            stop_recovery_controller_callback = (
                self.recovery_producer_controller.start_recovery_producer_controller()
            )

            consumer_thread.start()
            processing_thread_started = True

            timer_thread.start()
            timer_thread_started = True

            if processing_thread_started:
                consumer_thread.join()

            if timer_thread_started:
                timer_thread.join()

        except Exception as e:
            logging.error(e)
            self._stop()
            self.recovery_producer_controller.on_sigterm()
            return 2

        finally:
            if stop_recovery_controller_callback is not None:
                recovery_controller_exit_code = stop_recovery_controller_callback()

            self._close_resources()

        if recovery_controller_exit_code != 0:
            return recovery_controller_exit_code

        if self._runtime_error and not self._sigterm_received:
            return 1

        return 0
    
def main():
    configure_logging_from_env()
    worker = RecoveryNode()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received in amount filter q1")
        worker.notify_sigterm()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return worker.start()


if __name__ == "__main__":
    sys.exit(main())


