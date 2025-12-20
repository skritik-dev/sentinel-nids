import os
import sys
import logging
from logging.handlers import RotatingFileHandler

LOG_FORMAT = "[%(asctime)s] [%(levelname)s] [%(name)s] [%(module)s] %(message)s"

def get_logger(
    name: str = "sentinel",
    log_dir: str = "logs",
    log_file: str = "sentinel.log",
    level: int = logging.INFO
):
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False  # avoids duplicate logs
    formatter = logging.Formatter(LOG_FORMAT)

    # File handler (rotates logs)
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, log_file),
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
