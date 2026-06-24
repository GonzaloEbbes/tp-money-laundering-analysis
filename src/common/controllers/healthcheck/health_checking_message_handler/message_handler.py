from time import time
import uuid

from common import message_protocol
from common.message_protocol.internal import HealthCheckData, InternalMessage, TransactionData


class HealthCheckingMessageHandler:

    @staticmethod
    def serialize_heartbeat_message(node_prefix,node_index=""):
        parsedMessage = HealthCheckData()
        parsedMessage.container_name = node_prefix + str(node_index)
        return message_protocol.internal.serialize(message_protocol.internal.InternalMessageType.HEARTBEAT_MESSAGE, None, None, parsedMessage)

    @staticmethod
    def deserialize_healthcheck_message(message):
        internal_message = message_protocol.internal.deserialize(message)
        return internal_message
