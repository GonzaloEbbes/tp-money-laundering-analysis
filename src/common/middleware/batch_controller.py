import base64
import json
import queue
import threading
import time
from collections import defaultdict

import pika

from common.middleware.middleware import MessageMiddlewareCloseError
from common.middleware.rabbitmq_config import (
	RABBITMQ_SETTINGS,
	rabbitmq_connection_parameters,
)


def _message_to_bytes(message):
	if isinstance(message, bytes):
		return message
	if isinstance(message, bytearray):
		return bytes(message)
	if isinstance(message, str):
		return message.encode("utf-8")
	return json.dumps(message, separators=(",", ":")).encode("utf-8")


def _encode_batch(message_bodies):
	encoded_messages = [
		base64.b64encode(message_body).decode("ascii")
		for message_body in message_bodies
	]
	return json.dumps({"messages": encoded_messages}, separators=(",", ":")).encode("utf-8")


def _decode_batch_body(body):
	batch = json.loads(body.decode("utf-8"))
	return [
		base64.b64decode(encoded_message.encode("ascii"))
		for encoded_message in batch["messages"]
	]


def _is_batch_message(properties):
	if properties is None or properties.headers is None:
		return False
	return properties.headers.get(RABBITMQ_SETTINGS.batch_header) == RABBITMQ_SETTINGS.batch_header_value


class _BatchAckController:
	def __init__(self, channel, delivery_tag, total_messages):
		self._channel = channel
		self._delivery_tag = delivery_tag
		self._pending_acks = total_messages
		self._finished = False

	@property
	def finished(self):
		return self._finished

	def ack_one(self):
		if self._finished:
			return

		self._pending_acks -= 1

		if self._pending_acks == 0:
			self._finished = True
			self._channel.basic_ack(delivery_tag=self._delivery_tag)

	def nack_all(self):
		if self._finished:
			return

		self._finished = True
		self._channel.basic_nack(delivery_tag=self._delivery_tag)


def _deliver_to_user_callback_transparently(ch, method, properties, body, on_message_callback):
	if _is_batch_message(properties):
		messages = _decode_batch_body(body)
	else:
		messages = [body]

	ack_controller = _BatchAckController(
		channel=ch,
		delivery_tag=method.delivery_tag,
		total_messages=len(messages),
	)

	try:
		for message in messages:
			if ack_controller.finished:
				break
			on_message_callback(message, ack_controller.ack_one, ack_controller.nack_all)
	except Exception:
		ack_controller.nack_all()
		raise


class _DestinationBatchPublisher:
	def __init__(self, host, exchange_name, declare_exchange):
		self._host = host
		self._exchange_name = exchange_name
		self._declare_exchange = declare_exchange
		self._input_queue = queue.Queue()
		self._closed = False
		self._publisher_error = None
		self._ready_event = threading.Event()

		self._publisher_thread = threading.Thread(
			target=self._run,
			name=f"rabbit-batch-publisher-{exchange_name or 'default'}",
			daemon=True,
		)
		self._publisher_thread.start()
		self._ready_event.wait()

		if self._publisher_error is not None:
			raise self._publisher_error

	def enqueue(self, destination, message):
		if self._closed:
			raise MessageMiddlewareCloseError("Cannot send using a closed publisher")
		if self._publisher_error is not None:
			raise self._publisher_error
		self._input_queue.put((destination, _message_to_bytes(message)))

	def close(self):
		if self._closed:
			return

		self._closed = True
		self._input_queue.put(None)
		self._publisher_thread.join()

		if self._publisher_error is not None:
			raise self._publisher_error

	def _run(self):
		import logging
		connection = None
		channel = None
		buffers_by_destination = defaultdict(list)
		first_message_time_by_destination = {}

		try:
			connection = pika.BlockingConnection(rabbitmq_connection_parameters(self._host))
			channel = connection.channel()

			if self._declare_exchange:
				channel.exchange_declare(
					exchange=self._exchange_name,
					exchange_type='direct',
					durable=True,
				)

			self._ready_event.set()

			while True:
				timeout = self._seconds_until_next_flush(first_message_time_by_destination)

				try:
					item = self._input_queue.get(timeout=timeout)
				except queue.Empty:
					try:
						self._flush_expired_destinations(
							channel,
							buffers_by_destination,
							first_message_time_by_destination,
						)
					except Exception as e:
						logging.error(f"Error flushing expired destinations: {e}")
						self._publisher_error = e
						self._ready_event.set()
						raise
					continue

				if item is None:
					break

				destination, message_body = item

				self._append_to_destination_buffer(
					destination,
					message_body,
					buffers_by_destination,
					first_message_time_by_destination,
				)

				if len(buffers_by_destination[destination]) >= RABBITMQ_SETTINGS.batch_max_messages:
					try:
						self._flush_destination(
							channel,
							destination,
							buffers_by_destination,
							first_message_time_by_destination,
						)
					except Exception as e:
						logging.error(f"Error flushing destination {destination}: {e}")
						self._publisher_error = e
						self._ready_event.set()
						raise

				try:
					self._flush_expired_destinations(
						channel,
						buffers_by_destination,
						first_message_time_by_destination,
					)
				except Exception as e:
					logging.error(f"Error flushing expired destinations: {e}")
					self._publisher_error = e
					self._ready_event.set()
					raise

			try:
				self._flush_all_destinations(
					channel,
					buffers_by_destination,
					first_message_time_by_destination,
				)
			except Exception as e:
				logging.error(f"Error flushing all destinations: {e}")
				self._publisher_error = e
				self._ready_event.set()
				raise

		except Exception as e:
			if self._publisher_error is None:
				self._publisher_error = e
			self._ready_event.set()

		finally:
			try:
				if channel is not None and channel.is_open:
					channel.close()
			except Exception:
				pass

			try:
				if connection is not None and connection.is_open:
					connection.close()
			except Exception:
				pass

	def _append_to_destination_buffer(self, destination, message_body, buffers_by_destination, first_message_time_by_destination):
		if not buffers_by_destination[destination]:
			first_message_time_by_destination[destination] = time.monotonic()
		buffers_by_destination[destination].append(message_body)

	def _seconds_until_next_flush(self, first_message_time_by_destination):
		if not first_message_time_by_destination:
			return 0.5

		now = time.monotonic()
		next_deadline = min(
			first_message_time + RABBITMQ_SETTINGS.batch_max_seconds
			for first_message_time in first_message_time_by_destination.values()
		)
		return max(0.0, next_deadline - now)

	def _flush_expired_destinations(self, channel, buffers_by_destination, first_message_time_by_destination):
		now = time.monotonic()
		expired_destinations = [
			destination
			for destination, first_message_time in first_message_time_by_destination.items()
			if now - first_message_time >= RABBITMQ_SETTINGS.batch_max_seconds
		]

		for destination in expired_destinations:
			self._flush_destination(
				channel,
				destination,
				buffers_by_destination,
				first_message_time_by_destination,
			)

	def _flush_all_destinations(self, channel, buffers_by_destination, first_message_time_by_destination):
		for destination in list(buffers_by_destination.keys()):
			self._flush_destination(
				channel,
				destination,
				buffers_by_destination,
				first_message_time_by_destination,
			)

	def _flush_destination(self, channel, destination, buffers_by_destination, first_message_time_by_destination):
		messages = buffers_by_destination[destination]
		if not messages:
			return

		body = _encode_batch(messages)
		properties = pika.BasicProperties(
			headers={RABBITMQ_SETTINGS.batch_header: RABBITMQ_SETTINGS.batch_header_value},
			delivery_mode=1  # Keep batched messages transient to reduce disk pressure.
		)
		channel.basic_publish(
			exchange=self._exchange_name,
			routing_key=destination,
			body=body,
			properties=properties,
		)

		buffers_by_destination[destination] = []
		first_message_time_by_destination.pop(destination, None)
