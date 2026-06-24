import unittest

from common.message_protocol import internal
from common.middleware.testing.toxic_rabbitmq import ToxicRabbitConfig, ToxicRabbitMQ


class FakeMiddleware:
    def __init__(self):
        self.sent = []

    def send(self, message, *args, **kwargs):
        self.sent.append((message, args, kwargs))

    def start_consuming(self, callback):
        self.callback = callback

    def stop_consuming(self):
        self.stopped = True

    def close(self):
        self.closed = True


class ToxicRabbitMQTest(unittest.TestCase):
    def test_duplicates_matching_message(self):
        inner = FakeMiddleware()
        toxic = ToxicRabbitMQ(
            inner,
            ToxicRabbitConfig(
                send_count=4,
                duplicate_rate=1.0,
                message_ids=[10],
                random_seed=1,
            ),
        )
        message = internal.serialize(
            internal.InternalMessageType.GATEWAY_TO_DATE_FILTER,
            "client",
            "business-id",
            None,
            message_id=10,
        )

        toxic.send(message)

        self.assertEqual(len(inner.sent), 4)

    def test_does_not_duplicate_non_matching_message(self):
        inner = FakeMiddleware()
        toxic = ToxicRabbitMQ(
            inner,
            ToxicRabbitConfig(
                send_count=4,
                duplicate_rate=1.0,
                message_ids=[10],
                random_seed=1,
            ),
        )
        message = internal.serialize(
            internal.InternalMessageType.GATEWAY_TO_DATE_FILTER,
            "client",
            "business-id",
            None,
            message_id=11,
        )

        toxic.send(message)

        self.assertEqual(len(inner.sent), 1)

    def test_preserves_exchange_routing_arguments(self):
        inner = FakeMiddleware()
        toxic = ToxicRabbitMQ(
            inner,
            ToxicRabbitConfig(send_count=2, duplicate_rate=1.0, random_seed=1),
        )

        toxic.send(b"raw-message", routing_key="partition-1")

        self.assertEqual(len(inner.sent), 2)
        self.assertEqual(inner.sent[0][2], {"routing_key": "partition-1"})


if __name__ == "__main__":
    unittest.main()
