import threading
from common.snapshots.snapshot import SnapshotManager

class RecoverableWorker:
    def __init__(self, data_dir, batch_max_size=10000, flush_interval=5.0, set_keys=None):
        default_set_keys = ['processed_ids', 'eof_state']
        if set_keys:
            all_set_keys = list(set(default_set_keys) | set(set_keys))
        else:
            all_set_keys = default_set_keys

        
        self.snapshot_manager = SnapshotManager(data_dir)
        self.state = self.snapshot_manager.recover(set_keys=all_set_keys)
        
        self.BATCH_MAX_SIZE = batch_max_size
        self.FLUSH_INTERVAL_SECONDS = flush_interval
        self.batch_ops = []
        self.batch_acks = []
        self.batch_lock = threading.Lock()
        
        self._stop_flush_event = threading.Event()
        self._flush_thread = threading.Thread(
            target=self._periodic_flush_loop, daemon=True, name="flush-thread"
        )
        self._flush_thread.start()

        self.processed_ids = self.state.setdefault('processed_ids', {})

    def ensure_idempotent(self, client_id, data_id):
        processed_set = self.processed_ids.setdefault(client_id, set())
        if data_id in processed_set:
            return False
        processed_set.add(data_id)
        op = {
            'type': 'add_to_set',
            'path': ['processed_ids', client_id],
            'value': data_id
        }
        self.append_to_batch(op)
        return True
    
    def clean_processed_ids(self, client_id):
        if client_id in self.processed_ids:
            del self.processed_ids[client_id]
            self.append_to_batch({'type': 'delete', 'path': ['processed_ids', client_id]})

    def append_to_batch(self, op=None, conn=None, ack_func=None):
        """Agrega una operación de estado y/o delega la confirmación (ACK) de RabbitMQ."""
        with self.batch_lock:
            if op:
                self.batch_ops.append(op)
            if conn and ack_func:
                self.batch_acks.append((conn, ack_func))
                
            if len(self.batch_ops) >= self.BATCH_MAX_SIZE:
                self._flush_batch_locked()

    def _periodic_flush_loop(self):
        while not self._stop_flush_event.wait(timeout=self.FLUSH_INTERVAL_SECONDS):
            with self.batch_lock:
                self._flush_batch_locked()

    def _flush_batch_locked(self):
        if self.batch_ops:
            if hasattr(self.snapshot_manager, 'apply_batch'):
                self.snapshot_manager.apply_batch(self.batch_ops)
            else:
                for op in self.batch_ops:
                    self.snapshot_manager.apply_operation(op)
            self.batch_ops.clear()

        for conn, ack_func in self.batch_acks:
            if conn is not None and callable(ack_func):
                conn.add_callback_threadsafe(ack_func)
        self.batch_acks.clear()

    def stop_recoverable_worker(self):
        self._stop_flush_event.set()
        if self._flush_thread.is_alive():
            self._flush_thread.join()
        with self.batch_lock:
            self._flush_batch_locked()
