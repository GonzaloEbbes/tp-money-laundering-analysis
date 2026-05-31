import logging
import os
from collections import defaultdict

from common import message_protocol
from common.entity import PipelineEntity

EXPECTED_INPUT_EOFS = int(os.environ.get("EXPECTED_INPUT_EOFS", "2"))


class DynamicAmountFilter(PipelineEntity):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.averages_by_client = {}
        self.pending_transactions_by_client = defaultdict(list)
        self.eof_counts_by_client = defaultdict(int)
        self.clients_with_averages = set()
        self.finalized_clients = set()

    def entity_type(self):
        return "dynamic_amount_filter"

    def process_message(self, message):
        client_id = message.source_client_uuid

        if message.type == message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_AGGREGATOR_TO_AMOUNT_FILTER_Q3:
            if self._is_finalized(client_id):
                logging.error("Received averages after EOF for client=%s", client_id)
                return None
            averages = (message.data or {}).get("averages", {})
            self.averages_by_client[client_id] = averages
            self.clients_with_averages.add(client_id)
            self._flush_pending_transactions(client_id)
            self._try_finalize_client(client_id)
            return None

        if message.type == message_protocol.internal.InternalMessageType.USD_FILTER_Q3_TO_AMOUNT_FILTER_Q3:
            self._process_transaction(client_id, message.data_id, message.data or {})
            return None

        if message.type == message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
            self._process_eof(client_id)
            return None

        return None

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
        if self._is_finalized(client_id):
            logging.debug("Ignoring duplicate EOF for finalized client=%s", client_id)
            return

        self.eof_counts_by_client[client_id] += 1
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
        self._send_eof(client_id)
        self._clear_client(client_id)

    def _is_finalized(self, client_id):
        return client_id in self.finalized_clients

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
