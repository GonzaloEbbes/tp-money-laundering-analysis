import threading
from common.snapshots.recoverable_worker import RecoverableWorker

class StatefulWorker(RecoverableWorker):
    """
    Extiende RecoverableWorker añadiendo helpers para:
    - Idempotencia (processed_ids)
    - Conversión automática de listas a sets en claves específicas
    - Operaciones comunes de estado (add_to_set, update, delete)
    """
    
    def __init__(self, data_dir, set_keys=None, **kwargs):
        """
        set_keys: lista de claves en self.state que deben ser sets (ej. ['processed_ids', 'seen_banks'])
        """
        super().__init__(data_dir, set_keys=set_keys, **kwargs)
        self.state_lock = threading.Lock()

    def state_add_to_set(self, path, value):
        self.append_to_batch({'type': 'add_to_set', 'path': path, 'value': value})

    def state_update(self, path, value):
        self.append_to_batch({'type': 'update', 'path': path, 'value': value})

    def state_delete(self, path):
        self.append_to_batch({'type': 'delete', 'path': path})

    def clean_client_data(self, client_id, keys_to_clean=None):
        keys_to_clean = keys_to_clean or ['processed_ids']
        for key in keys_to_clean:
            if client_id in self.state.get(key, {}):
                del self.state[key][client_id]
                self.state_delete([key, client_id])