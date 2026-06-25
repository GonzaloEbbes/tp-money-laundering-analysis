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


mapper_message_handler = load_module(
    "average_per_pay_format_mapper_message_handler",
    "src/entities/average_per_pay_format/average_per_pay_format_mapper/message_handler/message_handler.py",
)


class Q3AveragePerPayMessageIdFlowTest(unittest.TestCase):
    def test_mapper_messages_for_distinct_payment_formats_must_not_share_message_id(self):
        base_data_id = "batch-1"

        credit_card = mapper_message_handler.MessageHandler.serialize_average_per_pay_joiner_message(
            "client-1",
            base_data_id,
            "Credit Card",
            {"sum_total": 10, "count": 1},
            message_id=f"{base_data_id}:Credit Card",
        )
        cash = mapper_message_handler.MessageHandler.serialize_average_per_pay_joiner_message(
            "client-1",
            base_data_id,
            "Cash",
            {"sum_total": 30, "count": 2},
            message_id=f"{base_data_id}:Cash",
        )

        credit_card_msg = internal.deserialize(credit_card)
        cash_msg = internal.deserialize(cash)

        self.assertEqual(credit_card_msg.data_id, base_data_id)
        self.assertEqual(cash_msg.data_id, base_data_id)
        self.assertNotEqual(credit_card_msg.message_id, cash_msg.message_id)


if __name__ == "__main__":
    unittest.main()
