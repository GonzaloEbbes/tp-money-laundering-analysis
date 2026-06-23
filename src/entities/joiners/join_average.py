import logging
import os
import uuid
import threading
from collections import defaultdict

from common import message_protocol, middleware
from common.snapshots.snapshot import SnapshotManager
from common.entity import PipelineEntity

TOTAL_AVERAGE_MAPPERS = int(os.environ.get("TOTAL_AVERAGE_MAPPERS", 1))
AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE = os.environ.get("AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE")


class JoinAverage(PipelineEntity):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        mom_host = kwargs.get("mom_host") if "mom_host" in kwargs else args[0]
        self.averages = defaultdict(lambda: defaultdict(lambda: {"sum_total": 0.0, "count": 0}))
        self.eof_counts = defaultdict(int)

        amount_filter_bindings = []
        self.amount_filter_q3_exchange = middleware.MessageMiddlewareExchangePublisherRabbitMQ(
            mom_host,
            AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE
        )

        data_dir = f"/data/snapshots/join_average_{os.environ.get('ID', '0')}"
        self.snapshot_manager = SnapshotManager(data_dir)
        self.state = self.snapshot_manager.recover()

        self.amount_filter_q3_exchange = middleware.MessageMiddlewareExchangePublisherRabbitMQ(
            mom_host,
            AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE
        )

        # --- Micro-batching ---
        self.BATCH_MAX_SIZE = 100
        self.FLUSH_INTERVAL_SECONDS = 2.0
        self.batch_ops = []
        self.batch_lock = threading.Lock()

        # --- Hilo temporal de flush ---
        self._stop_flush_event = threading.Event()
        self._flush_thread = threading.Thread(
            target=self._periodic_flush_loop,
            daemon=True,
            name="flush-join-avg"
        )
        self._flush_thread.start()

    def _periodic_flush_loop(self):
        while not self._stop_flush_event.wait(timeout=self.FLUSH_INTERVAL_SECONDS):
            self._flush_batch_thread_safe()

    def _flush_batch_thread_safe(self):
        with self.batch_lock:
            self._flush_batch_locked()

    def _flush_batch_locked(self):
        if not self.batch_ops:
            return
            
        if hasattr(self.snapshot_manager, 'apply_batch'):
            self.snapshot_manager.apply_batch(self.batch_ops)
        else:
            for op in self.batch_ops:
                self.snapshot_manager.apply_operation(op)
                
        self.batch_ops.clear()

    def entity_type(self):
        return "join_average"

    def process_message(self, message, ack, nack):
        if message.type not in (
            message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_JOINER_TO_AMOUNT_FILTER_Q3,
            message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE,
        ):
            return None

        client_id = message.source_client_uuid

        if message.type == message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE:
            logging.info("Received EOF from mapper for client=%s", client_id)
            eof_key = f"{client_id}_eofs"
            current_eofs = self.state.get(eof_key, 0) + 1
            self.state[eof_key] = current_eofs

            with self.batch_lock:
                self.batch_ops.append({
                    'type':'set',
                    'key':eof_key,
                    'value': current_eofs
                })
                self._flush_batch_locked()

            logging.debug("Received EOF for client=%s. Count=%s", client_id, current_eofs)
            if current_eofs < TOTAL_AVERAGE_MAPPERS:
                return None

            averages = self._build_average_payload(client_id)
            result_payload = message_protocol.internal.TransactionData({
                "averages": averages,
            })
            average_message = message_protocol.internal.serialize(
                message_protocol.internal.InternalMessageType.AVERAGE_PER_PAY_FORMAT_AGGREGATOR_TO_AMOUNT_FILTER_Q3,
                client_id,
                str(uuid.uuid4()),
                result_payload,
            )
            
            logging.info("Sending averages for client=%s to amount_filter_q3", client_id)
            logging.info("Message payload: %s", averages)
            self.amount_filter_q3_exchange.send(
                average_message,
                AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE,
            )

            eof = message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE, client_id, None, None)
            self.amount_filter_q3_exchange.send(
                eof,
                AVERAGE_PER_PAY_FORMAT_TO_FILTER_EXCHANGE,
            )

            with self.batch_lock:
                self.batch_ops.extend([
                    {'type': 'delete', 'key': f"{client_id}_data"},
                    {'type': 'delete', 'key': eof_key}
                ])
                self._flush_batch_locked()

            return None
        
        logging.debug("Received averages from mapper for client=%s", client_id)
        payload = message.data or {}
        payment_format = payload.get("PaymentFormat")
        if not payment_format:
            return None

        try:
            sum_total = float(payload.get("sum_total", 0))
            count = int(payload.get("count", 0))
        except (TypeError, ValueError):
            return None
        
        data_key = f"{client_id}_data"
        client_data = self.state.setdefault(data_key, {})
        format_data = client_data.setdefault(payment_format, {"sum_total": 0.0, "count": 0})
        
        format_data["sum_total"] += sum_total
        format_data["count"] += count

        op = {
            'type': 'update',
            'path': [data_key, payment_format],
            'value': format_data
        }

        with self.batch_lock:
            self.batch_ops.append(op)
            if len(self.batch_ops) >= self.BATCH_MAX_SIZE:
                self._flush_batch_locked()

        return None

    def _build_average_payload(self, client_id):
        result = {}
        client_data = self.state.get(f"{client_id}_data", {})

        for payment_format, values in client_data.items():
            count = values["count"]
            if count <= 0:
                continue
            sum_total = values["sum_total"]
            result[payment_format] = {
                "sum_total": sum_total,
                "count": count,
                "average": sum_total / count,
            }
        return result

    def close(self):
        super().close()
        self.amount_filter_q3_exchange.close()
