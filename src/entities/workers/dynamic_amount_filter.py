import logging
import os
import threading
from collections import defaultdict

from common import message_protocol, middleware
from common.entity import PipelineEntity

EXPECTED_INPUT_EOFS = int(os.environ.get("EXPECTED_INPUT_EOFS", "2"))
AMOUNT_FILTER_Q3_ID = int(os.environ.get("ID", "0"))
AMOUNT_FILTER_Q3_PREFIX = os.environ.get("AMOUNT_FILTER_Q3_PREFIX", "amount_filter_q3")
AMOUNT_FILTER_Q3_AMOUNT = int(os.environ.get("AMOUNT_FILTER_Q3_AMOUNT", "1"))
EOF_CONTROL_EXCHANGE = os.environ.get(
    "EOF_CONTROL_EXCHANGE",
    f"{AMOUNT_FILTER_Q3_PREFIX}_eof_control_exchange",
)


class DynamicAmountFilter(PipelineEntity):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        mom_host = kwargs.get("mom_host") if "mom_host" in kwargs else args[0]
        self.id = AMOUNT_FILTER_Q3_ID
        self.averages_by_client = {}
        self.pending_transactions_by_client = defaultdict(list)
        self.eof_counts_by_client = defaultdict(int)
        self.clients_with_averages = set()
        self.finalized_clients = set()
        self.leader_eof_counts_by_client = defaultdict(int)
        self._stop_lock = threading.Lock()
        self._stopping = False

        self.eof_control_exchange_consumer = None
        self.eof_control_exchange_producer = None
        if AMOUNT_FILTER_Q3_AMOUNT > 1:
            other_filters = [
                f"{AMOUNT_FILTER_Q3_PREFIX}_{i}"
                for i in range(AMOUNT_FILTER_Q3_AMOUNT)
                if i != self.id
            ]
            self.eof_control_exchange_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
                mom_host,
                EOF_CONTROL_EXCHANGE,
                [f"{AMOUNT_FILTER_Q3_PREFIX}_{self.id}"],
            )
            self._eof_producer_lock = threading.Lock()
            self.eof_control_exchange_producer = middleware.MessageMiddlewareExchangeRabbitMQ(
                mom_host,
                EOF_CONTROL_EXCHANGE,
                other_filters,
            )

    def entity_type(self):
        return "dynamic_amount_filter"

    def process_message(self, message):
        client_id = message.source_client_uuid

        if message.type == message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_AGGREGATOR_TO_AMOUNT_FILTER_Q3:
            self._process_average_message(client_id, message, propagate=True)
            return None

        if message.type == message_protocol.internal.InternalMessageType.USD_FILTER_Q3_TO_AMOUNT_FILTER_Q3:
            self._process_transaction(client_id, message.data_id, message.data or {})
            return None

        if message.type == message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
            self._process_eof(client_id)
            return None

        return None

    def _process_average_message(self, client_id, message, propagate):
        if self._is_finalized(client_id):
            logging.error("Received averages after EOF for client=%s", client_id)
            return

        averages = (message.data or {}).get("averages", {})
        self.averages_by_client[client_id] = averages
        self.clients_with_averages.add(client_id)
        self._propagate_control_message(message, propagate)
        self._flush_pending_transactions(client_id)
        self._try_finalize_client(client_id)

    def _process_transaction(self, client_id, data_id, transaction):
        if self._is_finalized(client_id):
            logging.error("Received Q3 transaction after EOF for client=%s data_id=%s", client_id, data_id)
            return

        if client_id not in self.averages_by_client:
            self.pending_transactions_by_client[client_id].append((data_id, transaction))
            return

        self._emit_if_above_average(client_id, data_id, transaction)

    def _flush_pending_transactions(self, client_id):
        if client_id not in self.averages_by_client:
            return

        pending = self.pending_transactions_by_client.pop(client_id, [])
        for data_id, transaction in pending:
            self._emit_if_above_average(client_id, data_id, transaction)

    def _process_eof(self, client_id):
        self._process_input_eof(client_id, propagate=True)

    def _process_input_eof(self, client_id, propagate):
        if self._is_finalized(client_id):
            logging.debug("Ignoring duplicate EOF for finalized client=%s", client_id)
            return

        self.eof_counts_by_client[client_id] += 1
        self._propagate_control_eof(client_id, propagate)
        logging.debug(
            "Received EOF for client=%s (%s/%s)",
            client_id,
            self.eof_counts_by_client[client_id],
            EXPECTED_INPUT_EOFS,
        )
        self._try_finalize_client(client_id)

    def _try_finalize_client(self, client_id):
        if self._is_finalized(client_id):
            return

        if self.eof_counts_by_client[client_id] < EXPECTED_INPUT_EOFS:
            return

        if client_id not in self.clients_with_averages:
            logging.debug("EOFs received for client=%s but averages are still pending", client_id)
            return

        self._finalize_client(client_id)

    def _finalize_client(self, client_id):
        if self._is_finalized(client_id):
            return

        self.finalized_clients.add(client_id)
        self._flush_pending_transactions(client_id)
        if AMOUNT_FILTER_Q3_AMOUNT <= 1:
            self._send_eof(client_id)
        elif self._is_leader():
            self._leader_count_eof_for_client(client_id)
        else:
            self._send_eof_leader_message(client_id)
        self._clear_client(client_id)

    def _is_finalized(self, client_id):
        return client_id in self.finalized_clients

    def _is_leader(self):
        return self.id == 0

    def _propagate_control_eof(self, client_id, propagate):
        if not propagate or AMOUNT_FILTER_Q3_AMOUNT <= 1:
            return

        self._send_control_message(
            message_protocol.internal.serialize(
                message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE,
                client_id,
                None,
                None,
            )
        )

    def _propagate_control_message(self, message, propagate):
        if not propagate or AMOUNT_FILTER_Q3_AMOUNT <= 1:
            return

        self._send_control_message(
            message_protocol.internal.serialize(
                message.type,
                message.source_client_uuid,
                message.data_id,
                message.data,
            )
        )

    def _send_eof_leader_message(self, client_id):
        self._send_control_message(
            message_protocol.internal.serialize(
                message_protocol.internal.InternalMessageType.EOF_LEADER_MESSAGE,
                client_id,
                None,
                None,
            )
        )

    def _send_control_message(self, serialized_message):
        with self._eof_producer_lock:
            self.eof_control_exchange_producer.send(serialized_message)

    def _leader_count_eof_for_client(self, client_id):
        self.leader_eof_counts_by_client[client_id] += 1
        if self.leader_eof_counts_by_client[client_id] == AMOUNT_FILTER_Q3_AMOUNT:
            logging.debug(
                "Leader received EOF from all amount filters for client=%s. Sending downstream EOF.",
                client_id,
            )
            self.leader_eof_counts_by_client.pop(client_id, None)
            self._send_eof(client_id)

    def process_eof_control_message(self, raw_message, ack, nack):
        try:
            message = message_protocol.deserialize(raw_message)
            client_id = message.source_client_uuid
            if message.type == message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
                self._process_input_eof(client_id, propagate=False)
            elif message.type == message_protocol.internal.InternalMessageType.EOF_LEADER_MESSAGE:
                if self._is_leader():
                    self._leader_count_eof_for_client(client_id)
            elif message.type == message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_AGGREGATOR_TO_AMOUNT_FILTER_Q3:
                self._process_average_message(client_id, message, propagate=False)
            ack()
        except Exception:
            logging.exception("dynamic_amount_filter failed while processing EOF control message")
            nack()

    def _emit_if_above_average(self, client_id, data_id, transaction):
        payment_format = transaction.get("payment_format")
        averages = self.averages_by_client.get(client_id, {})
        average_data = averages.get(payment_format)
        if not average_data:
            return

        try:
            amount_received = float(transaction.get("amount_received"))
            average = float(average_data.get("average"))
        except (TypeError, ValueError):
            logging.exception("Invalid Q3 amount data. transaction=%s average=%s", transaction, average_data)
            return

        if amount_received <= average:
            return

        output_payload = message_protocol.internal.TransactionData({
            "account_origin": transaction.get("account_origin"),
            "amount_received": amount_received,
        })
        self.output_queue.send(
            message_protocol.internal.serialize(
                message_protocol.internal.InternalMessageType.AMOUNT_FILTER_Q3_TO_GATEWAY,
                client_id,
                data_id,
                output_payload,
            )
        )

    def _send_eof(self, client_id):
        self.output_queue.send(
            message_protocol.internal.serialize(
                message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE,
                client_id,
                None,
                None,
            )
        )

    def _clear_client(self, client_id):
        self.averages_by_client.pop(client_id, None)
        self.pending_transactions_by_client.pop(client_id, None)
        self.eof_counts_by_client.pop(client_id, None)
        self.clients_with_averages.discard(client_id)
        return None

    def _run_input_consumer(self):
        self.input_queue.start_consuming(self._handle_raw_message)

    def _run_control_consumer(self):
        self.eof_control_exchange_consumer.start_consuming(self.process_eof_control_message)

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
                logging.error("Error stopping dynamic_amount_filter consumer: %s", e)

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
                logging.error("Error closing dynamic_amount_filter resource: %s", e)

    def start(self):
        logging.info(
            "Starting %s. id=%s amount=%s input_queue=%s output_queue=%s",
            self.entity_type(),
            self.id,
            AMOUNT_FILTER_Q3_AMOUNT,
            self.input_queue_name,
            self.output_queue_name,
        )

        input_thread = threading.Thread(
            target=self._run_input_consumer,
            name="dynamic-amount-filter-input-consumer-thread",
        )

        control_thread = None
        if AMOUNT_FILTER_Q3_AMOUNT > 1:
            control_thread = threading.Thread(
                target=self._run_control_consumer,
                name="dynamic-amount-filter-control-consumer-thread",
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
