import logging
import os
from logging.handlers import RotatingFileHandler


def get_logger(name: str = "tracker") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    os.makedirs("logs", exist_ok=True)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    fh = RotatingFileHandler("logs/tracker.log", maxBytes=5 * 1024 * 1024, backupCount=5)
    fh.setFormatter(formatter)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger
