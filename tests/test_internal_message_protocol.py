import json
import unittest

from common.message_protocol import internal


class InternalMessageProtocolTest(unittest.TestCase):
    def test_serializes_message_id_when_present(self):
        raw = internal.serialize(
            internal.InternalMessageType.GATEWAY_TO_DATE_FILTER,
            "client-1",
            "business-1",
            {"amount": 10},
            message_id=42,
        )

        decoded = json.loads(raw.decode("utf-8"))

        self.assertEqual(decoded["message_id"], 42)
        self.assertEqual(decoded["data_id"], "business-1")

    def test_deserializes_legacy_message_without_message_id(self):
        raw = json.dumps(
            {
                "type": internal.InternalMessageType.EOF_GENERIC_MESSAGE,
                "source_client_uuid": "client-1",
                "data_id": 10,
            }
        ).encode("utf-8")

        message = internal.deserialize(raw)

        self.assertEqual(message.data_id, 10)
        self.assertIsNone(message.message_id)

    def test_deserializes_message_id_when_present(self):
        raw = internal.serialize(
            internal.InternalMessageType.GATEWAY_TO_DATE_FILTER,
            "client-1",
            10,
            None,
            message_id=11,
        )

        message = internal.deserialize(raw)

        self.assertEqual(message.data_id, 10)
        self.assertEqual(message.message_id, 11)


if __name__ == "__main__":
    unittest.main()
