import logging

from .ranges import ProcessedRanges


class InMemoryDeduplicator:
    def __init__(self):
        self._processed_by_client = {}
        self._processed_strings_by_client = {}

    def should_process(self, client_id, message_id):
        if message_id is None:
            return True

        parsed_message_id = self._parse_int(message_id)
        if parsed_message_id is None:
            processed = self._processed_strings_by_client.get(str(client_id), set())
            should_process = str(message_id) not in processed
            if not should_process:
                logging.info(
                    "Discarding duplicate message. client=%s message_id=%s",
                    client_id,
                    message_id,
                )
            return should_process

        ranges = self._processed_by_client.get(str(client_id))
        if ranges is None:
            return True
        should_process = not ranges.contains(parsed_message_id)
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

        parsed_message_id = self._parse_int(message_id)
        if parsed_message_id is None:
            processed = self._processed_strings_by_client.setdefault(str(client_id), set())
            processed.add(str(message_id))
            return

        ranges = self._processed_by_client.setdefault(str(client_id), ProcessedRanges())
        ranges.add(parsed_message_id)

    def remove_client(self, client_id):
        client_key = str(client_id)
        self._processed_by_client.pop(client_key, None)
        self._processed_strings_by_client.pop(client_key, None)

    def _parse_int(self, value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
