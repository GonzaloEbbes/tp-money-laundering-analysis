import logging
import os
import socket
import multiprocessing
import signal

from common import message_protocol
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from message_handler import message_handler

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]

BANK_DEDUPLICATOR_QUEUE = os.environ.get("BANK_DEDUPLICATOR_QUEUE", "bank_deduplicator_queue")
CURRENCY_FILTER_QUEUE = os.environ.get("CURRENCY_FILTER_QUEUE", "currency_filter_queue")
DATE_FILTER_QUEUE = os.environ.get("DATE_FILTER_QUEUE", "date_filter_queue")


def handle_client_request(client_socket, message_handler_instance, client_list):

    currency_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, CURRENCY_FILTER_QUEUE)
    date_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, DATE_FILTER_QUEUE)
    deduplicator_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, BANK_DEDUPLICATOR_QUEUE)

    def _send_internal(queue, serialized_message):
        queue.send(serialized_message)

    def _handle_account(msg_data):
        serialized = message_handler_instance.serialize_account_data(msg_data)
        _send_internal(deduplicator_queue, serialized)

    def _handle_transaction(msg_data):
        if message_handler_instance.transaction_is_reinvestment(msg_data):
            return
        serialized_currency = message_handler_instance.serialize_transaction_currency(msg_data)
        _send_internal(currency_queue, serialized_currency)
        serialized_date = message_handler_instance.serialize_transaction_date(msg_data)
        _send_internal(date_queue, serialized_date)

    def _handle_eof(msg_data):
        message_handler_instance.eof_count += 1
        if message_handler_instance.eof_count == 1:
            _send_internal(deduplicator_queue, message_handler_instance.serialize_eof())
        elif message_handler_instance.eof_count == 2:
            eof_bytes = message_handler_instance.serialize_eof()
            _send_internal(currency_queue, eof_bytes)
            _send_internal(date_queue, eof_bytes)

    def _handle_close(msg_data):
        currency_queue.close()
        date_queue.close()
        deduplicator_queue.close()
        for idx, (_, sock) in enumerate(client_list):
            if sock == client_socket:
                client_list.pop(idx)
                break
        return "CLOSE"

    REQUEST_HANDLERS = {
        message_protocol.external.MsgType.ACCOUNT_RECORD: _handle_account,
        message_protocol.external.MsgType.TRANSACTION_RECORD: _handle_transaction,
        message_protocol.external.MsgType.EOF: _handle_eof,
        message_protocol.external.MsgType.CLOSE: _handle_close,
    }

    try:
        while True:
            msg_type, msg_data = message_protocol.external.recv_msg(client_socket)
            handler = REQUEST_HANDLERS.get(msg_type)
            if handler is None:
                logging.warning(f"Unhandled message type: {msg_type}")
                message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK)
                continue
            result = handler(msg_data)
            if result == "CLOSE":
                break
            message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK)
    except socket.error:
        logging.error("REQUEST| The connection with the client was lost")
    except Exception as e:
        logging.error(e)
    finally:
        currency_queue.close()
        date_queue.close()
        deduplicator_queue.close()
        client_socket.close()


MAP_OUTPUT_TYPES = {
    message_protocol.internal.InternalMessageType.AMOUNT_FILTER_Q1_TO_GATEWAY: message_protocol.external.MsgType.QUERY_1_RESULT,
    message_protocol.internal.InternalMessageType.DATE_PER_BANK_REDUCER_TO_GATEWAY: message_protocol.external.MsgType.QUERY_2_RESULT,
    message_protocol.internal.InternalMessageType.AMOUNT_FILTER_Q3_TO_GATEWAY: message_protocol.external.MsgType.QUERY_3_RESULT,
    message_protocol.internal.InternalMessageType.SCATHER_GATHER_JOINER_TO_GATEWAY: message_protocol.external.MsgType.QUERY_4_RESULT,
    message_protocol.internal.InternalMessageType.AMOUNT_FILTER_Q5_TO_GATEWAY: message_protocol.external.MsgType.QUERY_5_RESULT,
    message_protocol.internal.InternalMessageType.EOF_GENERIC_MESSAGE: message_protocol.external.MsgType.EOF
}

def handle_client_response(client_list):
    input_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

    def _consume_result(message, ack, nack):
        client_index = 0
        response_msg_type = None
        try:
            for [message_handler_instance, client_socket] in client_list:
                deserialized_message = (
                    message_handler_instance.deserialize_result_message(message)
                )

                if deserialized_message is None:
                    client_index += 1
                    continue

                msg_type = deserialized_message.type

                if int(msg_type) not in MAP_OUTPUT_TYPES:
                    logging.warning("Received message with unknown type: %s", msg_type)
                    client_index += 1
                    continue

                response_msg_type = MAP_OUTPUT_TYPES[deserialized_message.type]
                if response_msg_type == message_protocol.external.MsgType.EOF:
                    logging.info("Received EOF message, sending EOF to client")
                    message_protocol.external.send_msg(
                        client_socket,
                        response_msg_type
                    )
                else:
                    if deserialized_message.data is None:
                        logging.warning("Received message with no data, skipping: %s", deserialized_message)
                        continue
                    message_protocol.external.send_msg(
                        client_socket,
                        response_msg_type,
                        *deserialized_message.data
                    )
                ack()
                return
            logging.warning("Received message with no matching client handler: %s", message)
            nack()
        except socket.error:
            logging.error("RESPONSE | The connection with the server was lost")
            client_list.pop(client_index)
            ack()
        except Exception as e:
            logging.error("RESPONSE | Error processing message: %s", e)
            nack()
            input_queue.stop_consuming()

    input_queue.start_consuming(_consume_result)
    input_queue.close()


def handle_sigterm(server_socket, client_list, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for [_, client_socket] in client_list:
        client_socket.shutdown(socket.SHUT_RDWR)
    sigterm_received.value = 1


def main():
    logging.basicConfig(level=logging.INFO)

    with multiprocessing.Manager() as manager:
        client_list = manager.list()
        sigterm_received = manager.Value("c_short", 0)
        with multiprocessing.Pool(processes=os.process_cpu_count()) as processes_pool:
            processes_pool.apply_async(handle_client_response, (client_list,))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                logging.info("Listening to connections")
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                signal.signal(
                    signal.SIGTERM,
                    lambda signum, frame: handle_sigterm(
                        server_socket, client_list, sigterm_received
                    ),
                )
                while True:
                    try:
                        client_socket, _ = server_socket.accept()

                        logging.info("A new client has connected")
                        message_handler_instance = message_handler.MessageHandler()
                        client_list.append([message_handler_instance, client_socket])
                        processes_pool.apply_async(
                            handle_client_request,
                            (client_socket, message_handler_instance, client_list),
                        )
                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("The connection with the client was lost")
                            return 1
                        else:
                            return 0
                    except Exception as e:
                        logging.error(e)
                        return 2
    return 0


if __name__ == "__main__":
    main()
