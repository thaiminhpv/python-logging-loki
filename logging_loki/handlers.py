# -*- coding: utf-8 -*-

import logging
import warnings
from logging.handlers import QueueHandler, QueueListener
from queue import Queue
from typing import Dict, Type

from logging_loki import const, emitter


class LokiQueueHandler(QueueHandler):
    """This handler automatically creates listener and `LokiHandler` to handle logs queue."""

    def __init__(self, queue: Queue, **kwargs):
        """Create new logger handler with the specified queue and kwargs for the `LokiHandler`."""
        super().__init__(queue)
        self.handler = LokiHandler(**kwargs)
        self.listener = QueueListener(self.queue, self.handler)
        self.listener.start()


class LokiHandler(logging.Handler):
    """
    Log handler that sends log records to Loki.

    `Loki API <https://github.com/grafana/loki/blob/master/docs/api.md>`_
    """

    emitters: Dict[str, Type[emitter.LokiEmitter]] = {"0": emitter.LokiEmitterV0, "1": emitter.LokiEmitterV1, "2": emitter.LokiEmitterV2}

    def __init__(self, url: str, tags: dict | None = None, auth: emitter.BasicAuth | None = None, version: str | None = None, headers: dict | None = None, verify_ssl: bool = True):
        """
        Create new Loki logging handler.

        Arguments:
            url: Endpoint used to send log entries to Loki (e.g. `https://my-loki-instance/loki/api/v1/push`).
            tags: Default tags added to every log record.
            auth: Optional tuple with username and password for basic HTTP authentication.
            version: Version of Loki emitter to use.
            verify_ssl: If set to False, the endpoint's SSL certificates are not verified

        """
        super().__init__()

        if version is None and const.emitter_ver == "0":
            msg = (
                "Loki /api/prom/push endpoint is in the depreciation process starting from version 0.4.0.",
                "Explicitly set the emitter version to '0' if you want to use the old endpoint.",
                "Or specify '1' if you have Loki version> = 0.4.0.",
                "When the old API is removed from Loki, the handler will use the new version by default.",
            )
            warnings.warn(" ".join(msg), DeprecationWarning)

        version = version or const.emitter_ver
        if version not in self.emitters:
            raise ValueError("Unknown emitter version: {0}".format(version))
        self.emitter = self.emitters[version](url, tags, auth, headers, verify_ssl)

    def handleError(self, record):  # noqa: N802
        """Close emitter and let default handler take actions on error."""
        self.emitter.close()
        super().handleError(record)

    def emit(self, record: logging.LogRecord):
        """Send log record to Loki."""
        # noinspection PyBroadException
        try:
            self.emitter(record, self.format(record))
        except Exception:
            self.handleError(record)


class LokiBatchHandler(logging.Handler):
    """
    Log handler that batches log records and sends them to Loki at configurable intervals.

    `Loki API <https://github.com/grafana/loki/blob/master/docs/api.md>`_
    """

    def __init__(
        self,
        url: str,
        tags: dict | None = None,
        auth: emitter.BasicAuth | None = None,
        headers: dict | None = None,
        verify_ssl: bool = True,
        flush_interval: float = 5.0,
    ):
        """
        Create new Loki batch logging handler.

        Arguments:
            url: Endpoint used to send log entries to Loki (e.g. `https://my-loki-instance/loki/api/v1/push`).
            tags: Default tags added to every log record.
            auth: Optional tuple with username and password for basic HTTP authentication.
            headers: Optional dict with HTTP headers to send.
            verify_ssl: If set to False, the endpoint's SSL certificates are not verified.
            flush_interval: Time interval in seconds between automatic flushes (default: 5.0).
        """
        super().__init__()
        self.emitter = emitter.LokiBatchEmitter(url, tags, auth, headers, verify_ssl, flush_interval)

    def handleError(self, record):  # noqa: N802
        """Close emitter and let default handler take actions on error."""
        self.emitter.close()
        super().handleError(record)

    def emit(self, record: logging.LogRecord):
        """Buffer log record for batch sending to Loki."""
        # noinspection PyBroadException
        try:
            self.emitter(record, self.format(record))
        except Exception:
            self.handleError(record)

    def flush(self):
        """Manually flush all buffered records to Loki."""
        self.emitter.flush()

    def close(self):
        """Close the handler and flush remaining records."""
        self.emitter.close()
        super().close()


class LokiBatchQueueHandler(QueueHandler):
    """
    Queue-based handler that batches log records and sends them to Loki at configurable intervals.
    This handler automatically creates a listener and `LokiBatchHandler` to handle logs queue.
    """

    def __init__(self, queue: Queue, flush_interval: float = 5.0, **kwargs):
        """
        Create new logger handler with the specified queue and kwargs for the `LokiBatchHandler`.

        Arguments:
            queue: Queue instance to buffer log records.
            flush_interval: Time interval in seconds between automatic flushes (default: 5.0).
            **kwargs: Additional arguments passed to LokiBatchHandler (url, tags, auth, headers, verify_ssl).
        """
        super().__init__(queue)
        self.handler = LokiBatchHandler(flush_interval=flush_interval, **kwargs)
        self.listener = QueueListener(self.queue, self.handler)
        self.listener.start()

    def close(self):
        """Stop the listener and close the handler."""
        # Only stop if the listener's thread is running (guard against multiple close calls)
        if self.listener._thread is not None:
            self.listener.stop()
        self.handler.close()
        super().close()
