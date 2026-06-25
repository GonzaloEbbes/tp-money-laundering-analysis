import os
import json
import shutil
import threading
from typing import Dict, Any, List, Optional, Set

class SnapshotManager:
    def __init__(self, data_dir: str, snapshot_interval_ops: int = 1000):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

        self.snapshot_path = os.path.join(data_dir, "snapshot.json")
        self.wal_path = os.path.join(data_dir, "wal.log")

        self.state: Dict[str, Any] = {}
        self._lock = threading.RLock()

        self.snapshot_interval_ops = snapshot_interval_ops
        self.op_count_since_snapshot = 0
        self.last_snapshot_index = 0

    def recover(self, set_keys: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Carga el último snapshot y aplica todas las operaciones del WAL
        posteriores a ese snapshot. Retorna el estado reconstruido.
        - set_keys: lista de claves en el estado que deben ser sets (en lugar de listas).
        """
        with self._lock:
            if os.path.exists(self.snapshot_path):
                with open(self.snapshot_path, 'r', encoding='utf-8') as f:
                    snap = json.load(f)
                raw_state = snap.get('data', {})
                # Convertir listas a sets
                if set_keys:
                    self.state = self._convert_lists_to_sets(raw_state, set_keys)
                else:
                    self.state = raw_state
                self.last_snapshot_index = snap.get('last_index', 0)
            else:
                self.state = {}
                self.last_snapshot_index = 0

            # Replay del WAL
            if os.path.exists(self.wal_path):
                with open(self.wal_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        op = json.loads(line.strip())
                        if op.get('index', 0) > self.last_snapshot_index:
                            self._apply_op_no_wal(op)

            self.op_count_since_snapshot = 0
            return self.state


    def apply_operation(self, op: Dict) -> None:
        """
        Aplica una operación al estado, la escribe en el WAL,
        y toma snapshot si se alcanza el intervalo.
        """
        with self._lock:
            op_index = self.last_snapshot_index + self.op_count_since_snapshot + 1
            op['index'] = op_index

            with open(self.wal_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(op) + '\n')

            self._apply_op_no_wal(op)
            self.op_count_since_snapshot += 1

            if self.op_count_since_snapshot >= self.snapshot_interval_ops:
                self._take_snapshot_locked()

    def _apply_op_no_wal(self, op: Dict) -> None:
        """Aplica la operación al estado (sin escribir en WAL)."""
        op_type = op.get('type')

        if op_type == 'set':
            if 'path' in op:
                path = op['path']
                target = self.state
                for key in path[:-1]:
                    target = target.setdefault(key, {})
                target[path[-1]] = op['value']
            else:
                self.state[op['key']] = op['value']

        elif op_type == 'delete':
            if 'path' in op:
                path = op['path']
                target = self.state
                for key in path[:-1]:
                    target = target.get(key, {})
                if path:
                    target.pop(path[-1], None)
            else:
                self.state.pop(op.get('key', None), None)

        elif op_type == 'update':
            path = op.get('path', [])
            if not path:
                return
            target = self.state
            for key in path[:-1]:
                target = target.setdefault(key, {})
            target[path[-1]] = op['value']

        elif op_type == 'append':
            key = op.get('key')
            if key is None:
                return
            self.state.setdefault(key, []).append(op['value'])

        elif op_type == 'add_to_set':
            if 'path' in op:
                path = op['path']
                target = self.state
                for key in path[:-1]:
                    target = target.setdefault(key, {})
                current = target.get(path[-1])
                if current is None:
                    # Si no existe, crear un set con el valor
                    target[path[-1]] = {op['value']}
                elif isinstance(current, list):
                    # Convertir lista a set y añadir
                    current_set = set(current)
                    current_set.add(op['value'])
                    target[path[-1]] = current_set
                elif isinstance(current, set):
                    current.add(op['value'])
                else:
                    # Si es otro tipo (ej. dict), crear un set con el valor existente y el nuevo
                    target[path[-1]] = {current, op['value']}
            else:
                key = op.get('key')
                if key is None:
                    return
                current = self.state.get(key)
                if current is None:
                    self.state[key] = {op['value']}
                elif isinstance(current, list):
                    current_set = set(current)
                    current_set.add(op['value'])
                    self.state[key] = current_set
                elif isinstance(current, set):
                    current.add(op['value'])
                else:
                    self.state[key] = {current, op['value']}

        elif op_type == 'remove_from_set':
            if 'path' in op:
                path = op['path']
                target = self.state
                for key in path[:-1]:
                    target = target.get(key, {})
                if not path:
                    return
                current = target.get(path[-1])
                if isinstance(current, set):
                    current.discard(op['value'])
                elif isinstance(current, list):
                    # Convertir lista a set y eliminar
                    current_set = set(current)
                    current_set.discard(op['value'])
                    target[path[-1]] = current_set
                # Si no es ni set ni lista, ignorar
            else:
                key = op.get('key')
                if key is None:
                    return
                current = self.state.get(key)
                if isinstance(current, set):
                    current.discard(op['value'])
                elif isinstance(current, list):
                    current_set = set(current)
                    current_set.discard(op['value'])
                    self.state[key] = current_set


    def take_snapshot(self) -> None:
        with self._lock:
            self._take_snapshot_locked()

    def _take_snapshot_locked(self) -> None:
        state_serializable = self._convert_sets_to_lists(self.state)
        snapshot_data = {
            'last_index': self.last_snapshot_index + self.op_count_since_snapshot,
            'data': state_serializable
        }
        temp_path = self.snapshot_path + '.tmp'
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(snapshot_data, f)
        shutil.move(temp_path, self.snapshot_path)

        # Compactar WAL
        if os.path.exists(self.wal_path):
            with open(self.wal_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            new_lines = []
            for line in lines:
                op = json.loads(line.strip())
                if op.get('index', 0) > snapshot_data['last_index']:
                    new_lines.append(line)
            with open(self.wal_path, 'w', encoding='utf-8') as f:
                f.writelines(new_lines)

        self.last_snapshot_index = snapshot_data['last_index']
        self.op_count_since_snapshot = 0

    def _convert_sets_to_lists(self, obj):
        if isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, dict):
            return {k: self._convert_sets_to_lists(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_sets_to_lists(item) for item in obj]
        else:
            return obj

    def _convert_lists_to_sets(self, obj, set_keys):
        if isinstance(obj, dict):
            new_dict = {}
            for k, v in obj.items():
                if k in set_keys and isinstance(v, list):
                    new_dict[k] = set(v)
                else:
                    new_dict[k] = self._convert_lists_to_sets(v, set_keys)
            return new_dict
        elif isinstance(obj, list):
            return [self._convert_lists_to_sets(item, set_keys) for item in obj]
        else:
            return obj

    def get_state(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self.state)
    
    def apply_batch(self, ops: List[Dict]) -> None:
        with self._lock:
            lines = []
            for op in ops:
                self.op_count_since_snapshot += 1
                op['index'] = self.last_snapshot_index + self.op_count_since_snapshot
                lines.append(json.dumps(op))
                self._apply_op_no_wal(op)
            
            with open(self.wal_path, 'a', encoding='utf-8') as f:
                f.write('\n'.join(lines) + '\n')
            
            if self.op_count_since_snapshot >= self.snapshot_interval_ops:
                self._take_snapshot_locked()
