import hashlib


def stable_hash(value: str) -> int:
    return int(hashlib.sha256(value.encode("utf-8")).hexdigest(), 16)


# Si es un nodo comun, el nodo responsable es uno correspondiente al hasheo
# Si es un nodo de recovery, el nodo responsable es el nodo anterior en el anillo logico de nodos de recovery: 3 -> 2, 2 -> 1, 1 -> 0, 0 -> 3
def recovery_node_id_responsible_of_recovery(my_id: int, container_name: str, recovery_prefix: str, recovery_amount: int) -> int:
    if container_name.startswith(recovery_prefix):
        return (my_id - 1) % recovery_amount
    return stable_hash(container_name) % recovery_amount