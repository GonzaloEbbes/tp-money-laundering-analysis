import logging
import os


def configure_logging_from_env(default_level: str = "INFO") -> None:
    log_level_name = os.getenv("LOG_LEVEL", default_level).upper()

    log_level = getattr(logging, log_level_name, None)
    if not isinstance(log_level, int):
        raise ValueError(
            f"Invalid LOG_LEVEL={log_level_name}. "
            "Expected DEBUG, INFO, WARNING, ERROR or CRITICAL."
        )

    log_format = os.getenv(
        "LOG_FORMAT",
        "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
    )

    log_datefmt = os.getenv("LOG_DATEFMT", "%H:%M:%S")

    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt=log_datefmt,
        force=True,
    )