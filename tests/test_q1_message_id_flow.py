import importlib.util
import unittest
from pathlib import Path

from common.message_protocol import internal


REPO_ROOT = Path(__file__).resolve().parents[1]


def load_module(name, relative_path):
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gateway_message_handler = load_module(
    "gateway_message_handler",
    "src/gateway/message_handler/message_handler.py",
)
usd_q1q2_message_handler = load_module(
    "usd_q1q2_message_handler",
    "src/entities/filters/usd_filter_q1q2/message_handler/message_handler.py",
)
amount_q1_message_handler = load_module(
    "amount_q1_message_handler",
    "src/entities/filters/amount_filter_q1/message_handler/message_handler.py",
)


class Q1MessageIdFlowTest(unittest.TestCase):
    def test_gateway_assigns_incremental_message_id_to_q1_input(self):
        handler = gateway_message_handler.MessageHandler()
        first = handler.serialize_transaction_currency(
            (
                "2024-01-01",
                "bank-a",
                "origin-a",
                "bank-b",
                "destination-a",
                "10",
                "US Dollar",
                "10",
                "US Dollar",
                "credit card",
            )
        )
        second = handler.serialize_transaction_currency(
            (
                "2024-01-02",
                "bank-a",
                "origin-b",
                "bank-b",
                "destination-b",
                "20",
                "US Dollar",
                "20",
                "US Dollar",
                "credit card",
            )
        )

        first_msg = internal.deserialize(first)
        second_msg = internal.deserialize(second)

        self.assertEqual(first_msg.message_id, 1)
        self.assertEqual(second_msg.message_id, 2)
        self.assertEqual(first_msg.data_id, 1)
        self.assertEqual(second_msg.data_id, 2)

    def test_gateway_does_not_assign_message_id_to_date_path(self):
        handler = gateway_message_handler.MessageHandler()
        raw = handler.serialize_transaction_date(
            (
                "2024-01-01",
                "bank-a",
                "origin-a",
                "bank-b",
                "destination-a",
                "10",
                "US Dollar",
                "10",
                "US Dollar",
                "credit card",
            )
        )

        message = internal.deserialize(raw)

        self.assertIsNone(message.message_id)

    def test_usd_filter_q1_message_preserves_message_id(self):
        raw = usd_q1q2_message_handler.MessageHandler.serialize_amount_filter_q1_message(
            "client-1",
            7,
            {
                "account_origin": "origin-a",
                "account_destination": "destination-a",
                "amount_received": "10",
            },
            message_id=3,
        )

        message = internal.deserialize(raw)

        self.assertEqual(message.data_id, 7)
        self.assertEqual(message.message_id, 3)

    def test_usd_filter_q2_message_preserves_message_id(self):
        raw = usd_q1q2_message_handler.MessageHandler.serialize_data_per_bank_shuffler_message(
            "client-1",
            7,
            {
                "account_origin": "origin-a",
                "from_bank": "bank-a",
                "amount_received": "10",
            },
            message_id=3,
        )

        message = internal.deserialize(raw)

        self.assertEqual(message.data_id, 7)
        self.assertEqual(message.message_id, 3)

    def test_amount_filter_q1_result_preserves_message_id(self):
        raw = amount_q1_message_handler.MessageHandler.serialize_gateway_query_message(
            "client-1",
            7,
            {
                "account_origin": "origin-a",
                "account_destination": "destination-a",
                "amount_received": "10",
            },
            message_id=3,
        )

        message = internal.deserialize(raw)

        self.assertEqual(message.data_id, 7)
        self.assertEqual(message.message_id, 3)


if __name__ == "__main__":
    unittest.main()
