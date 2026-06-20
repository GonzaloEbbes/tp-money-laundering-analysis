import logging
from threading import Lock

from .ranges import ProcessedRanges


class InMemoryDeduplicator:
    def __init__(self):
        self._processed_by_client = {}
        self._lock = Lock()

    def should_process(self, client_id, message_id):
        if message_id is None:
            return True

        with self._lock:
            ranges = self._processed_by_client.get(str(client_id))
            if ranges is None:
                return True
            should_process = not ranges.contains(message_id)
            if not should_process:
                logging.info(
                    "Discarding duplicate message. client=%s message_id=%s",
                    client_id,
                    message_id,
                )
            return should_process

    def mark_processed(self, client_id, message_id):
        if message_id is None:
            return

        with self._lock:
            ranges = self._processed_by_client.setdefault(str(client_id), ProcessedRanges())
            ranges.add(message_id)

    def remove_client(self, client_id):
        # TODO: Call this after the client's EOF is fully acknowledged by the pipeline.
        pass
