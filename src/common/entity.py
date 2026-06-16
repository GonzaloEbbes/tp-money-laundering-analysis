import logging
import os
import time
from abc import ABC, abstractmethod

from common import message_protocol
from common.middleware import MessageMiddlewareQueueRabbitMQ


class PipelineEntity(ABC):
    def __init__(self, mom_host, input_queue, output_queue=None):
        self.input_queue_name = input_queue
        self.output_queue_name = output_queue
        self.processing_delay_seconds = float(
            os.environ.get("PROCESSING_DELAY_SECONDS", "0")
        )
        self.input_queue = MessageMiddlewareQueueRabbitMQ(mom_host, input_queue)
        self.output_queue = (
            MessageMiddlewareQueueRabbitMQ(mom_host, output_queue)
            if output_queue
            else None
        )

    @abstractmethod
    def entity_type(self):
        pass

    @abstractmethod
    def process_message(self, message):
        pass

    def start(self):
        logging.info(
            "Starting %s. input_queue=%s output_queue=%s",
            self.entity_type(),
            self.input_queue_name,
            self.output_queue_name,
        )
        self.input_queue.start_consuming(self._handle_raw_message)

    def _handle_raw_message(self, raw_message, ack, nack):
        try:
            message = message_protocol.deserialize(raw_message)
            processed = self.process_message(message)
            if self.processing_delay_seconds > 0:
                time.sleep(self.processing_delay_seconds)

            if self.output_queue and processed is not None:
                if isinstance(processed, tuple):
                    msg_to_send, r_key = processed
                else:
                    msg_to_send, r_key = processed, None
                serialized_bytes = message_protocol.internal.serialize(
                    msg_to_send.type,
                    msg_to_send.source_client_uuid,
                    msg_to_send.data_id,
                    msg_to_send.data
                )
                
                self.output_queue.send(serialized_bytes)
            ack()
        except Exception:
            logging.exception("%s failed while processing message", self.entity_type())
            nack()

    def close(self):
        self.input_queue.close()
        if self.output_queue:
            self.output_queue.close()
