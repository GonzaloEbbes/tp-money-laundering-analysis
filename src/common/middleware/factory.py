import os

DEFAULT_IMPL = "rabbitmq"
TOXIC_IMPL = "toxic_rabbitmq"


def _middleware_impl():
    return os.environ.get("MIDDLEWARE_IMPL", DEFAULT_IMPL).strip() or DEFAULT_IMPL


def _wrap_if_toxic(inner):
    impl = _middleware_impl()
    if impl == DEFAULT_IMPL:
        return inner
    if impl == TOXIC_IMPL:
        from common.middleware.testing.toxic_rabbitmq import ToxicRabbitMQ

        return ToxicRabbitMQ(inner)
    raise ValueError(f"Unsupported MIDDLEWARE_IMPL: {impl}")


def MessageMiddlewareQueueRabbitMQ(host, queue_name):
    from common.middleware.middleware_rabbitmq import (
        MessageMiddlewareQueueRabbitMQ as RabbitMQQueue,
    )

    return _wrap_if_toxic(RabbitMQQueue(host, queue_name))


def MessageMiddlewareExchangeRabbitMQ(
    host,
    exchange_name,
    routing_keys,
    queue_name=None,
    exclusive=True,
    queue_arguments=None
):
    from common.middleware.middleware_rabbitmq import (
        MessageMiddlewareExchangeRabbitMQ as RabbitMQExchange,
    )

    return _wrap_if_toxic(
        RabbitMQExchange(
            host,
            exchange_name,
            routing_keys,
            queue_name=queue_name,
            exclusive=exclusive,
            queue_arguments=queue_arguments if queue_arguments else None
        )
    )


def MessageMiddlewareExchangePublisherRabbitMQ(host, exchange_name, bindings=None):
    from common.middleware.middleware_rabbitmq import (
        MessageMiddlewareExchangePublisherRabbitMQ as RabbitMQExchangePublisher,
    )

    return _wrap_if_toxic(RabbitMQExchangePublisher(host, exchange_name, bindings))
