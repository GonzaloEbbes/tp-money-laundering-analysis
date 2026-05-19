import json
import uuid


def new_message(payload):
    return {
        "message_id": str(uuid.uuid4()),
        "payload": payload,
        "visited": [],
    }


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    if isinstance(message, bytes):
        raw_message = message.decode("utf-8")
    else:
        raw_message = message
    return json.loads(raw_message)
