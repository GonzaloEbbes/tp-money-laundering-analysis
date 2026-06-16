import logging
import os
import uuid
import threading
from collections import defaultdict

from common import message_protocol, middleware
from common.entity import PipelineEntity

MAP_AVERAGE_ID = int(os.environ.get("ID", "0"))
MAP_AVERAGE_PREFIX = os.environ.get("MAP_AVERAGE_PREFIX", "map_average")
MAP_AVERAGE_AMOUNT = int(os.environ.get("MAP_AVERAGE_AMOUNT", "1"))
EOF_CONTROL_EXCHANGE = os.environ.get(
    "EOF_CONTROL_EXCHANGE",
    f"{MAP_AVERAGE_PREFIX}_eof_control_exchange",
)


class MapAverage(PipelineEntity):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        mom_host = kwargs.get("mom_host") if "mom_host" in kwargs else args[0]
        self.id = MAP_AVERAGE_ID
        self.averages = defaultdict(lambda: defaultdict(lambda: {"sum_total": 0.0, "count": 0}))
        self.flushed_clients = set()
        self.lock = threading.Lock()
        self._eof_propagated_clients = set()
        self._eof_propagated_clients_lock = threading.Lock()
        self._inflight_messages = defaultdict(int)
        self._inflight_message_lock = threading.Lock()
        self._is_pending_to_flush_client = set()
        self._is_pending_to_flush_client_lock = threading.Lock()
        self._stop_lock = threading.Lock()
        self._stopping = False

        self.eof_control_exchange_consumer = None
        self.eof_control_exchange_producer = None
        if MAP_AVERAGE_AMOUNT > 1:
            other_mappers = [
                f"{MAP_AVERAGE_PREFIX}_{i}"
                for i in range(MAP_AVERAGE_AMOUNT)
                if i != self.id
            ]
            self.eof_control_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
                mom_host,
                EOF_CONTROL_EXCHANGE,
                [f"{MAP_AVERAGE_PREFIX}_{self.id}"],
            )
            self._eof_producer_lock = threading.Lock()
            self.eof_control_exchange_producer = middleware.MessageMiddlewareExchangeRabbitMQ(
                mom_host,
                EOF_CONTROL_EXCHANGE,
                other_mappers,
            )

    def entity_type(self):
        return "map_average"

    def process_message(self, message):
        client_id = message.source_client_uuid

        if message.type == message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
            self._process_input_eof(client_id, message)
            return None

        if (
            message.type
            != message_protocol.internal.InternalMessageType.USD_FILTER_Q4_TO_AVERAGE_PER_PAY_FORMAT_MAPPER
        ):
            return None

        self._add_inflight_message(client_id)
        try:
            self._process_transaction(client_id, message)
        finally:
            self._decrease_inflight_message(client_id)
            self._check_and_flush_client_if_pending(client_id)
        return None

    def _process_transaction(self, client_id, message):
        payload = message.data or {}
        payment_format = payload.get("payment_format")
        if not client_id or not payment_format:
            return

        try:
            amount = float(payload.get("amount_received", 0))
        except (TypeError, ValueError):
            return

        with self.lock:
            if client_id in self.flushed_clients:
                return
            values = self.averages[client_id][payment_format]
            values["sum_total"] += amount
            values["count"] += 1

    def _process_input_eof(self, client_id, base_message):
        if not client_id:
            return

        self._propagate_eof_to_other_mappers(client_id, base_message)
        self._try_flush_client(client_id, base_message)

    def _propagate_eof_to_other_mappers(self, client_id, base_message):
        if MAP_AVERAGE_AMOUNT <= 1:
            return

        with self._eof_propagated_clients_lock:
            if client_id in self._eof_propagated_clients:
                return
            self._eof_propagated_clients.add(client_id)

        with self._eof_producer_lock:
            self.eof_control_exchange_producer.send(
                message_protocol.internal.serialize(
                    message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE,
                    client_id,
                    base_message.data_id,
                    None,
                )
            )
        logging.debug("Sent map_average EOF for client %s to other mappers", client_id)

    def _process_control_eof(self, client_id, base_message):
        if not client_id:
            return
        self._try_flush_client(client_id, base_message)

    def _try_flush_client(self, client_id, base_message):
        with self._inflight_message_lock:
            has_inflight = self._inflight_messages.get(client_id, 0) > 0

        if has_inflight:
            with self._is_pending_to_flush_client_lock:
                self._is_pending_to_flush_client.add(client_id)
            logging.debug(
                "EOF received for client %s but map_average still has inflight messages",
                client_id,
            )
            return

        self._flush_client(client_id, base_message)

    def _check_and_flush_client_if_pending(self, client_id):
        with self._is_pending_to_flush_client_lock:
            is_pending = client_id in self._is_pending_to_flush_client

        if is_pending:
            self._try_flush_client(client_id, None)

    def _flush_client(self, client_id, base_message):
        if not client_id:
            return None

        with self.lock:
            if client_id in self.flushed_clients:
                return None
            client_state = self.averages.pop(client_id, {})
            self.flushed_clients.add(client_id)

        for payment_format, values in client_state.items():
            partial_payload = message_protocol.internal.TransactionData({
                "PaymentFormat": payment_format,
                "sum_total": values["sum_total"],
                "count": values["count"],
            })
            self.output_queue.send(
                message_protocol.internal.serialize(
                    message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_MAPPER_TO_AVERAGE_PER_PAY_FORMAT_JOINER,
                    client_id,
                    str(uuid.uuid4()),
                    partial_payload,
                )
            )

        data_id = base_message.data_id if base_message is not None else None
        self.output_queue.send(
            message_protocol.internal.serialize(
                message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE,
                client_id,
                data_id,
                None,
            )
        )
        with self._is_pending_to_flush_client_lock:
            self._is_pending_to_flush_client.discard(client_id)
        return None

    def _add_inflight_message(self, client_id):
        if not client_id:
            return
        with self._inflight_message_lock:
            self._inflight_messages[client_id] += 1

    def _decrease_inflight_message(self, client_id):
        if not client_id:
            return
        with self._inflight_message_lock:
            if client_id not in self._inflight_messages:
                return
            self._inflight_messages[client_id] -= 1
            if self._inflight_messages[client_id] <= 0:
                del self._inflight_messages[client_id]

    def _run_input_consumer(self):
        try:
            self.input_queue.start_consuming(self._handle_raw_message)
        except Exception:
            logging.exception("MapAverage input consumer crashed")
            self.stop()

    def _run_control_consumer(self):
        try:
            self.eof_control_exchange_consumer.start_consuming(self.process_eof_control_message)
        except Exception:
            logging.exception("MapAverage EOF control consumer crashed")
            self.stop()

    def process_eof_control_message(self, raw_message, ack, nack):
        try:
            message = message_protocol.deserialize(raw_message)
            if message.type == message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                self._process_control_eof(message.source_client_uuid, message)
            ack()
        except Exception:
            logging.exception("map_average failed while processing EOF control message")
            nack()

    def stop(self):
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True

        consumers = [self.input_queue]
        if self.eof_control_exchange_consumer is not None:
            consumers.append(self.eof_control_exchange_consumer)

        for consumer in consumers:
            try:
                consumer.stop_consuming()
            except Exception as e:
                logging.error("Error stopping map_average consumer: %s", e)

    def close(self):
        resources = [self.input_queue]
        if self.output_queue is not None:
            resources.append(self.output_queue)
        if self.eof_control_exchange_consumer is not None:
            resources.append(self.eof_control_exchange_consumer)
        if self.eof_control_exchange_producer is not None:
            resources.append(self.eof_control_exchange_producer)

        for resource in resources:
            try:
                resource.close()
            except Exception as e:
                logging.error("Error closing map_average resource: %s", e)

    def start(self):
        logging.debug(
            "Starting %s. id=%s amount=%s input_queue=%s output_queue=%s",
            self.entity_type(),
            self.id,
            MAP_AVERAGE_AMOUNT,
            self.input_queue_name,
            self.output_queue_name,
        )

        input_thread = threading.Thread(
            target=self._run_input_consumer,
            name="map-average-input-consumer-thread",
        )

        control_thread = None
        if MAP_AVERAGE_AMOUNT > 1:
            control_thread = threading.Thread(
                target=self._run_control_consumer,
                name="map-average-control-consumer-thread",
            )

        try:
            input_thread.start()
            if control_thread is not None:
                control_thread.start()

            input_thread.join()
            if control_thread is not None:
                control_thread.join()
        finally:
            self.close()
