def message_dedup_key(message):
    message_id = getattr(message, "message_id", None)
    return message_id
