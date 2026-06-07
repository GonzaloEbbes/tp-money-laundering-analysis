# common/middleware/rabbitmq_config.py

import os
from dataclasses import dataclass

import pika


def _read_int_env(name, default):
	value = os.environ.get(name)

	if value is None:
		return default

	try:
		return int(value)
	except ValueError as e:
		raise ValueError(f"Environment variable {name} must be an integer. Current value: {value}") from e


def _read_float_env(name, default):
	value = os.environ.get(name)

	if value is None:
		return default

	try:
		return float(value)
	except ValueError as e:
		raise ValueError(f"Environment variable {name} must be a float. Current value: {value}") from e


@dataclass(frozen=True)
class RabbitMQSettings:
	max_unacked_messages: int
	heartbeat: int
	blocked_connection_timeout_seconds: int
	batch_max_messages: int
	batch_max_seconds: float
	batch_header: str
	batch_header_value: str


def load_rabbitmq_settings():
	return RabbitMQSettings(
		max_unacked_messages=_read_int_env("RABBITMQ_MAX_UNACKED_MESSAGES", 1),
		heartbeat=_read_int_env("RABBITMQ_HEARTBEAT", 0),
		blocked_connection_timeout_seconds=_read_int_env("RABBITMQ_BLOCKED_CONNECTION_TIMEOUT_SECONDS", 300),
		batch_max_messages=_read_int_env("RABBITMQ_BATCH_MAX_MESSAGES", 10000),
		batch_max_seconds=_read_float_env("RABBITMQ_BATCH_MAX_SECONDS", 2),
		batch_header=os.environ.get("RABBITMQ_BATCH_HEADER", "x-middleware-batch"),
		batch_header_value=os.environ.get("RABBITMQ_BATCH_HEADER_VALUE", "v1"),
	)


RABBITMQ_SETTINGS = load_rabbitmq_settings()


def rabbitmq_connection_parameters(host):
	return pika.ConnectionParameters(
		host,
		heartbeat=RABBITMQ_SETTINGS.heartbeat,
		blocked_connection_timeout=RABBITMQ_SETTINGS.blocked_connection_timeout_seconds,
	)