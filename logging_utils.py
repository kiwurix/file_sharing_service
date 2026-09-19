"""Shared, privacy-conscious rotating log configuration."""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_SIZE = 5 * 1024 * 1024
LOG_BACKUPS = 3


class ClosingRotatingFileHandler(RotatingFileHandler):
    """Rotate normally but release the file between sparse log events."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            super().emit(record)
        finally:
            if self.stream:
                self.stream.close()
                self.stream = None


def configure_logging(name: str, path: Path, *, console: bool = False) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    resolved = str(path.resolve())
    if any(isinstance(handler, RotatingFileHandler) and handler.baseFilename == resolved for handler in logger.handlers):
        return logger

    for handler in list(logger.handlers):
        if isinstance(handler, RotatingFileHandler):
            logger.removeHandler(handler)
            handler.close()

    path.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(threadName)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler = ClosingRotatingFileHandler(path, maxBytes=LOG_SIZE, backupCount=LOG_BACKUPS,
                                              encoding="utf-8", delay=True)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    if console and not any(isinstance(handler, logging.StreamHandler) and not isinstance(handler, RotatingFileHandler)
                           for handler in logger.handlers):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    return logger
