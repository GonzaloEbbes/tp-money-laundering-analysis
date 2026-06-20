def message_dedup_key(message):
    message_id = getattr(message, "message_id", None)
    if message_id is None:
        message_id = getattr(message, "data_id", None)
    if message_id is None:
        return None
    return f"{getattr(message, 'type', None)}:{message_id}"
