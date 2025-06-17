import logging
import os

import structlog
from structlog import DropEvent


class Logger:
    def __init__(self, level=None, buffer_size=0, chain=[]):
        logLevel = level or self._readEnvLevel() or logging.INFO

        processors = chain or [
            structlog.threadlocal.merge_threadlocal_context,
            structlog.processors.add_log_level,
            structlog.processors.format_exc_info,
            structlog.processors.TimeStamper(fmt="iso", utc=False),
            self._process_buffer,
            structlog.processors.JSONRenderer(sort_keys=True),
        ]

        structlog.configure(
            wrapper_class=structlog.make_filtering_bound_logger(logLevel),
            processors=processors,
        )
        self._logger = structlog.get_logger()
        self._buffer = []
        self._buffer_size = buffer_size
        self._force_log = False

    # clear structured log bindings
    def clear(self, **kwargs):
        self._logger.new(**kwargs)
        return self._logger

    # add a new structured log binding
    def bind(self, **kwargs):
        self._logger = self._logger.bind(**kwargs)
        return self._logger

    # remove a specific binding
    def unbind(self, *args):
        self._logger = self._logger.unbind(*args)
        return self._logger

    def critical(self, *args, **kwargs):
        self._logger.critical(*args, **kwargs)

    def error(self, *args, **kwargs):
        self._logger.error(*args, **kwargs)

    def warning(self, *args, **kwargs):
        self._logger.warning(*args, **kwargs)

    def info(self, *args, **kwargs):
        self._logger.info(*args, **kwargs)

    def debug(self, *args, **kwargs):
        self._logger.debug(*args, **kwargs)

    def _process_buffer(self, logger, method_name, event_dict):
        # if buffer is disabled, just log the message
        if self._buffer_size == 0:
            return event_dict

        # queue messages regardless of the log level
        self._buffer.append(event_dict)

        # if log level is error or critical, or if the buffer is full, log all queued messages
        if (
                event_dict["level"] == "error"
                or event_dict["level"] == "critical"
                or len(self._buffer) >= self._buffer_size
        ):
            new_event_dict = {"events": self._buffer}

            self._clear_buffer()
            return new_event_dict

        # if buffer is not full and log level is less than error, skip the message
        return DropEvent

    def _clear_buffer(self):
        self._buffer = []

    def _readEnvLevel(self):
        levels = {
            "CRITICAL": logging.CRITICAL,
            "ERROR": logging.ERROR,
            "WARNING": logging.WARNING,
            "INFO": logging.INFO,
            "DEBUG": logging.DEBUG,
        }
        level = os.environ.get("LOG_LEVEL")

        if not level:
            return logging.DEBUG

        return levels.get(level)


slog = Logger(buffer_size=0)
