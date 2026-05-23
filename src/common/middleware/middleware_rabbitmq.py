import time

import pika

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.consumer_tag = None
        self._connect()

    def _connect(self):
        last_error = None
        for _ in range(20):
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.host, heartbeat=0)
                )
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=self.queue_name, durable=False)
                self.channel.basic_qos(prefetch_count=1)
                return
            except pika.exceptions.AMQPConnectionError as error:
                last_error = error
                time.sleep(1)
        raise MessageMiddlewareDisconnectedError(last_error)

    def start_consuming(self, on_message_callback):
        if not self.channel:
            raise MessageMiddlewareDisconnectedError()

        def _callback(channel, method, properties, body):
            def ack():
                channel.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            on_message_callback(body, ack, nack)

        try:
            self.consumer_tag = self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=_callback,
                auto_ack=False,
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError(error) from error
        except Exception as error:
            raise MessageMiddlewareMessageError(error) from error

    def stop_consuming(self):
        if self.channel and self.channel.is_open and self.consumer_tag:
            self.channel.basic_cancel(self.consumer_tag)
            self.consumer_tag = None

    def send(self, message, routing_key=None):
        if not self.channel or self.channel.is_closed:
            raise MessageMiddlewareDisconnectedError()
        try:
            target_routing_key = routing_key if routing_key else self.queue_name
            self.channel.basic_publish(
                exchange="",
                routing_key=target_routing_key,
                body=message,
                properties=pika.BasicProperties(content_type="application/json"),
            )
        except pika.exceptions.AMQPConnectionError as error:
            raise MessageMiddlewareDisconnectedError(error) from error
        except Exception as error:
            raise MessageMiddlewareMessageError(error) from error

    def close(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as error:
            raise MessageMiddlewareCloseError(error) from error
