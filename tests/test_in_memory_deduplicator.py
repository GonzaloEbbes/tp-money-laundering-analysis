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
        calls = []
        message = self._message(3)

        first_processed = deduplicator.process_once(
            "amount_filter_q1_queue",
            message,
            lambda: calls.append("processed"),
        )
        second_processed = deduplicator.process_once(
            "amount_filter_q1_queue",
            message,
            lambda: calls.append("processed"),
        )

        self.assertTrue(first_processed)
        self.assertFalse(second_processed)
        self.assertEqual(calls, ["processed"])

    def test_same_message_id_with_different_input_queue_is_not_duplicate(self):
        deduplicator = InMemoryDeduplicator()
        calls = []
        message = self._message(3)

        deduplicator.process_once(
            "amount_filter_q1_queue",
            message,
            lambda: calls.append("first-key"),
        )
        processed = deduplicator.process_once(
            "other_input_queue",
            message,
            lambda: calls.append("second-key"),
        )

        self.assertTrue(processed)
        self.assertEqual(calls, ["first-key", "second-key"])

    def test_legacy_message_without_message_id_always_processes(self):
        deduplicator = InMemoryDeduplicator()
        calls = []
        message = self._message(None)

        deduplicator.process_once(
            "amount_filter_q1_queue",
            message,
            lambda: calls.append("processed"),
        )
        deduplicator.process_once(
            "amount_filter_q1_queue",
            message,
            lambda: calls.append("processed"),
        )

        self.assertEqual(calls, ["processed", "processed"])


if __name__ == "__main__":
    unittest.main()
