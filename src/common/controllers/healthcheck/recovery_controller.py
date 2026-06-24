import hashlib
import os
import logging
import signal
import sys
import threading
from time import sleep, monotonic

from common import middleware, message_protocol
from common.controllers.healthcheck.health_checking_message_handler.message_handler import HealthCheckingMessageHandler
from common.controllers.healthcheck.utils import recovery_node_id_responsible_of_recovery
from common.logging import configure_logging_from_env
from common.message_protocol.internal import HealthCheckData

class RecoveryController: 

    def __init__(self, mom_host, heartbeat_exchange, id: str, prefix: str, recovery_prefix: str, recovery_amount: int):

        self.id = int(id)
        self.prefix_worker = prefix

        self.heartbeat_producer = middleware.MessageMiddlewareExchangePublisherRabbitMQ(
            mom_host,
            heartbeat_exchange
        )
        self.heartbeat_exchange_routing_key = f"{recovery_prefix}_{recovery_node_id_responsible_of_recovery(self.id, prefix, recovery_prefix, recovery_amount)}"   

        self.producer_lock = threading.Lock()

        self._sigterm_received = False
        self._runtime_error = False


    def _handle_runtime_failure(self, error, context):
        logging.error(f"{context}: {error}")
        self._runtime_error = True

    def _close_resources(self):
        resources = [self.heartbeat_producer]
        for resource in resources:
            try:
                resource.close()
            except Exception as e:
                logging.error(f"Error closing resource: {e}")

    def notify_sigterm(self):
        self._sigterm_received = True

    def _run_heartbeat_sender(self):
        try:
            self.heartbeat_producer.start_consuming(self._process_heartbeat_message)
        except Exception as e:
            self._handle_runtime_failure(e, "Error in heartbeat sender")

    def start_recovery_controller(self):

        
        heartbeat_sender = threading.Thread(
            target=self._run_heartbeat_sender,
            name=f"{self.prefix_worker.replace('_', '-')}-heartbeat-sender-thread",
        )

        sender_started = False

        try:
            heartbeat_sender.start()
            sender_started = True

        except Exception as e:
            logging.error(e)
            self._close_resources()
            return 2

        if sender_started:
            heartbeat_sender.join()

        self._close_resources()

        if self._runtime_error and not self._sigterm_received:
            return 1

        return 0


