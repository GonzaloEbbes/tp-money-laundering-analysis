MESSAGE_TYPE_FACTOR = 10**12


def message_dedup_key(message):
    message_id = getattr(message, "message_id", None)
    if message_id is None:
        return None

    try:
        numeric_message_id = int(message_id)
        numeric_message_type = int(message.type)
    except (TypeError, ValueError):
        return message_id

    return numeric_message_type * MESSAGE_TYPE_FACTOR + numeric_message_id
