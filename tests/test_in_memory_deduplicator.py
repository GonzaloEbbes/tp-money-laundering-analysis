import unittest

from common.dedup import InMemoryDeduplicator
from common.message_protocol import internal


class InMemoryDeduplicatorTest(unittest.TestCase):
    def _message(self, message_id, message_type=None, client="client-1"):
        return internal.InternalMessage(
            type=message_type or internal.InternalMessageType.USD_FILTER_Q1Q2_TO_AMOUNT_FILTER_Q1,
            source_client_uuid=client,
            data_id=10,
            data={"amount_received": "10"},
            message_id=message_id,
        )

    def test_processes_first_message_and_discards_duplicate(self):
        deduplicator = InMemoryDeduplicator()
        message = self._message(3)

        first_processed = deduplicator.should_process(
            message.source_client_uuid,
            message.message_id,
        )
        deduplicator.mark_processed(message.source_client_uuid, message.message_id)
        second_processed = deduplicator.should_process(
            message.source_client_uuid,
            message.message_id,
        )

        self.assertTrue(first_processed)
        self.assertFalse(second_processed)

    def test_same_message_id_with_different_client_is_not_duplicate(self):
        deduplicator = InMemoryDeduplicator()
        first_client_message = self._message(3, client="client-1")
        second_client_message = self._message(3, client="client-2")

        deduplicator.mark_processed(
            first_client_message.source_client_uuid,
            first_client_message.message_id,
        )
        processed = deduplicator.should_process(
            second_client_message.source_client_uuid,
            second_client_message.message_id,
        )

        self.assertTrue(processed)

    def test_legacy_message_without_message_id_always_processes(self):
        deduplicator = InMemoryDeduplicator()
        message = self._message(None)

        first_processed = deduplicator.should_process(
            message.source_client_uuid,
            message.message_id,
        )
        deduplicator.mark_processed(message.source_client_uuid, message.message_id)
        second_processed = deduplicator.should_process(
            message.source_client_uuid,
            message.message_id,
        )

        self.assertTrue(first_processed)
        self.assertTrue(second_processed)

    def test_mark_processed_collapses_contiguous_ranges(self):
        deduplicator = InMemoryDeduplicator()
        client_id = "client-1"

        deduplicator.mark_processed(client_id, 1)
        deduplicator.mark_processed(client_id, 3)
        deduplicator.mark_processed(client_id, 2)

        self.assertFalse(deduplicator.should_process(client_id, 1))
        self.assertFalse(deduplicator.should_process(client_id, 2))
        self.assertFalse(deduplicator.should_process(client_id, 3))
        self.assertTrue(deduplicator.should_process(client_id, 4))

    def test_string_message_id_is_deduplicated(self):
        deduplicator = InMemoryDeduplicator()
        client_id = "client-1"
        message_id = "SCATHER:6b62225f-d307-4065-9125-feb6da1e08a1"

        self.assertTrue(deduplicator.should_process(client_id, message_id))
        deduplicator.mark_processed(client_id, message_id)
        self.assertFalse(deduplicator.should_process(client_id, message_id))


if __name__ == "__main__":
    unittest.main()
