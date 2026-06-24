import json
import logging
import os
import random
from pathlib import Path

from common.message_protocol import internal


DEFAULT_CONFIG_PATH = Path(__file__).with_name("toxic-rabbit.json")


class ToxicRabbitConfig:
    def __init__(
        self,
        send_count=1,
        duplicate_rate=1.0,
        message_ids=None,
        message_types=None,
        random_seed=None,
    ):
        self.send_count = max(1, int(send_count))
        self.duplicate_rate = float(duplicate_rate)
        self.message_ids = {int(value) for value in (message_ids or [])}
        self.message_types = {int(value) for value in (message_types or [])}
        self.random = random.Random(random_seed)

    @classmethod
    def from_env(cls):
        config_path = Path(
            os.environ.get("TOXIC_RABBIT_CONFIG_PATH", DEFAULT_CONFIG_PATH)
        )
        with config_path.open("r", encoding="utf-8") as config_file:
            data = json.load(config_file)

        return cls(
            send_count=data.get("send_count", 1),
            duplicate_rate=data.get("duplicate_rate", 1.0),
            message_ids=data.get("message_ids", []),
            message_types=data.get("message_types", []),
            random_seed=data.get("random_seed"),
        )

    def send_count_for(self, message):
        if not self._matches_filters(message):
            return 1
        if self.random.random() > self.duplicate_rate:
            return 1
        return self.send_count

    def _matches_filters(self, message):
        if not self.message_ids and not self.message_types:
            return True

        decoded = _try_deserialize_internal_message(message)
        if decoded is None:
            return False

        if self.message_ids:
            message_id = getattr(decoded, "message_id", None)
            if message_id is None or int(message_id) not in self.message_ids:
                return False

        if self.message_types:
            message_type = getattr(decoded, "type", None)
            if message_type is None or int(message_type) not in self.message_types:
                return False

        return True


class ToxicRabbitMQ:
    def __init__(self, inner, config=None):
        self._inner = inner
        self._config = config or ToxicRabbitConfig.from_env()

    def send(self, message, *args, **kwargs):
        send_count = self._config.send_count_for(message)
        if send_count > 1:
            logging.warning("ToxicRabbitMQ sending message %s times", send_count)

        for _ in range(send_count):
            self._inner.send(message, *args, **kwargs)

    def start_consuming(self, on_message_callback):
        return self._inner.start_consuming(on_message_callback)

    def stop_consuming(self):
        return self._inner.stop_consuming()

    def close(self):
        return self._inner.close()

    def __getattr__(self, name):
        return getattr(self._inner, name)


def _try_deserialize_internal_message(message):
    if not isinstance(message, bytes):
        return None
    try:
        decoded = internal.deserialize(message)
    except Exception:
        return None
    if isinstance(decoded, dict):
        return None
    return decoded
