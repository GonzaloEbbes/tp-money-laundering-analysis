import logging
from dataclasses import dataclass
from threading import Lock

from .ranges import ProcessedRanges


@dataclass(frozen=True)
class DedupKey:
    input_queue: str
    source_client_uuid: str
    message_type: int


class InMemoryDeduplicator:
    def __init__(self):
        self._processed_by_key = {}
        self._lock = Lock()

    def _build_key(self, input_queue, message):
        return DedupKey(
            input_queue=str(input_queue),
            source_client_uuid=str(message.source_client_uuid),
            message_type=int(message.type),
        )

    def should_process(self, input_queue, message):
        if message.message_id is None:
            return True

        key = self._build_key(input_queue, message)
        with self._lock:
            ranges = self._processed_by_key.get(key)
            if ranges is None:
                return True
            return not ranges.contains(message.message_id)

    def mark_processed(self, input_queue, message):
        if message.message_id is None:
            return

        key = self._build_key(input_queue, message)
        with self._lock:
            ranges = self._processed_by_key.setdefault(key, ProcessedRanges())
            ranges.add(message.message_id)

    def process_once(self, input_queue, message, process):
        if message.message_id is None:
            process()
            return True

        key = self._build_key(input_queue, message)
        with self._lock:
            ranges = self._processed_by_key.setdefault(key, ProcessedRanges())
            if ranges.contains(message.message_id):
                logging.info(
                    "Discarding duplicate message. input_queue=%s client=%s "
                    "type=%s message_id=%s",
                    input_queue,
                    message.source_client_uuid,
                    message.type,
                    message.message_id,
                )
                return False

            process()
            ranges.add(message.message_id)
        return True
