# -*- coding: utf-8 -*-

import abc
import copy
import functools
import json
import logging
import threading
import time
from logging.config import ConvertingDict
from typing import Any, Dict, List, Optional, Tuple

import requests
import rfc3339

from logging_loki import const

BasicAuth = Optional[Tuple[str, str]]


class LokiEmitter(abc.ABC):
    """Base Loki emitter class."""

    success_response_code = const.success_response_code
    level_tag = const.level_tag
    logger_tag = const.logger_tag
    label_allowed_chars = const.label_allowed_chars
    label_replace_with = const.label_replace_with
    session_class = requests.Session

    def __init__(self, url: str, tags: dict | None = None, auth: BasicAuth = None, headers: dict | None = None, verify_ssl: bool = True):
        """
        Create new Loki emitter.

        Arguments:
            url: Endpoint used to send log entries to Loki (e.g. `https://my-loki-instance/loki/api/v1/push`).
            tags: Default tags added to every log record.
            auth: Optional tuple with username and password for basic HTTP authentication.
            headers: Optional dict with HTTP headers to send.
        """
        #: Tags that will be added to all records handled by this handler.
        self.tags = tags or {}
        #: Loki JSON push endpoint (e.g `http://127.0.0.1/loki/api/v1/push`)
        self.url = url
        #: Optional tuple with username and password for basic authentication.
        self.auth = auth
        #: Optional headers for post request
        self.headers = headers or {}
        #: Verify the host's ssl certificate
        self.verify_ssl = verify_ssl

        self._session: requests.Session | None = None

    def __call__(self, record: logging.LogRecord, line: str):
        """Send log record to Loki."""
        payload = self.build_payload(record, line)
        resp = self.session.post(self.url, json=payload, headers=self.headers, verify=self.verify_ssl)
        if resp.status_code != self.success_response_code:
            raise ValueError("Unexpected Loki API response status code: {0}".format(resp.status_code))

    @abc.abstractmethod
    def build_payload(self, record: logging.LogRecord, line) -> dict:
        """Build JSON payload with a log entry."""
        raise NotImplementedError  # pragma: no cover

    @property
    def session(self) -> requests.Session:
        """Create HTTP session."""
        if self._session is None:
            self._session = self.session_class()
            self._session.auth = self.auth or None
        return self._session

    def close(self):
        """Close HTTP session."""
        if self._session is not None:
            self._session.close()
            self._session = None

    @functools.lru_cache(const.format_label_lru_size)
    def format_label(self, label: str) -> str:
        """
        Build label to match prometheus format.

        `Label format <https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels>`_
        """
        for char_from, char_to in self.label_replace_with:
            label = label.replace(char_from, char_to)
        return "".join(char for char in label if char in self.label_allowed_chars)

    def build_tags(self, record: logging.LogRecord) -> Dict[str, Any]:
        """Return tags that must be send to Loki with a log record."""
        tags = dict(self.tags) if isinstance(self.tags, ConvertingDict) else self.tags
        tags = copy.deepcopy(tags)
        tags[self.level_tag] = record.levelname.lower()
        tags[self.logger_tag] = record.name

        extra_tags = getattr(record, "tags", {})
        if not isinstance(extra_tags, dict):
            return tags

        for tag_name, tag_value in extra_tags.items():
            cleared_name = self.format_label(tag_name)
            if cleared_name:
                tags[cleared_name] = tag_value

        return tags


class LokiEmitterV0(LokiEmitter):
    """Emitter for Loki < 0.4.0."""

    def build_payload(self, record: logging.LogRecord, line) -> dict:
        """Build JSON payload with a log entry."""
        labels = self.build_labels(record)
        ts = rfc3339.format_microsecond(record.created)
        stream = {
            "labels": labels,
            "entries": [{"ts": ts, "line": line}],
        }
        return {"streams": [stream]}

    def build_labels(self, record: logging.LogRecord) -> str:
        """Return Loki labels string."""
        labels: List[str] = []
        for label_name, label_value in self.build_tags(record).items():
            cleared_name = self.format_label(str(label_name))
            cleared_value = str(label_value).replace('"', r"\"")
            labels.append('{0}="{1}"'.format(cleared_name, cleared_value))
        return "{{{0}}}".format(",".join(labels))


class LokiEmitterV1(LokiEmitter):
    """Emitter for Loki >= 0.4.0."""

    def build_payload(self, record: logging.LogRecord, line) -> dict:
        """Build JSON payload with a log entry."""
        labels = self.build_tags(record)
        ns = 1e9
        ts = str(int(record.created * ns))
        stream = {
            "stream": labels,
            "values": [[ts, line]],
        }
        return {"streams": [stream]}


class LokiEmitterV2(LokiEmitterV1):
    """
    Emitter for Loki >= 0.4.0.
    Enables passing additional headers to requests
    """

    def __init__(self, url: str, tags: dict | None = None, auth: BasicAuth = None, headers: dict = None, verify_ssl: bool = True):
        super().__init__(url, tags, auth, headers, verify_ssl)

    def __call__(self, record: logging.LogRecord, line: str):
        """Send log record to Loki."""
        payload = self.build_payload(record, line)
        resp = self.session.post(self.url, json=payload, headers=self.headers, verify=self.verify_ssl)
        if resp.status_code != self.success_response_code:
            raise ValueError("Unexpected Loki API response status code: {0}".format(resp.status_code))


class LokiBatchEmitter(LokiEmitterV1):
    """
    Batch emitter for Loki >= 0.4.0.
    Buffers log records and sends them in batches at configurable time intervals.
    Logs are grouped by their labels for efficient Loki ingestion.
    """

    def __init__(
        self,
        url: str,
        tags: dict | None = None,
        auth: BasicAuth = None,
        headers: dict | None = None,
        verify_ssl: bool = True,
        flush_interval: float = 5.0,
        verbose: bool = False,
    ):
        """
        Create new Loki batch emitter.

        Arguments:
            url: Endpoint used to send log entries to Loki (e.g. `https://my-loki-instance/loki/api/v1/push`).
            tags: Default tags added to every log record.
            auth: Optional tuple with username and password for basic HTTP authentication.
            headers: Optional dict with HTTP headers to send.
            verify_ssl: Verify the host's ssl certificate.
            flush_interval: Time interval in seconds between automatic flushes.
            verbose: Enable verbose debug logging for the emitter.
        """
        super().__init__(url, tags, auth, headers, verify_ssl)
        self.flush_interval = flush_interval
        self.verbose = verbose
        self._logger = logging.getLogger("logging_loki.batch_emitter")
        # Buffer: maps label key (JSON string of sorted labels) to list of (timestamp, line) tuples
        self._buffer: Dict[str, List[Tuple[str, str]]] = {}
        # Store the actual labels dict for each label key
        self._labels_map: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None
        self._closed = False
        self._start_timer()
        if self.verbose:
            self._logger.debug("LokiBatchEmitter initialized: url=%s, flush_interval=%s", url, flush_interval)

    def _labels_to_key(self, labels: Dict[str, Any]) -> str:
        """Convert labels dict to a hashable key (sorted JSON string)."""
        return json.dumps(labels, sort_keys=True)

    def _start_timer(self):
        """Start the flush timer."""
        if self._closed:
            return
        self._timer = threading.Timer(self.flush_interval, self._flush_callback)
        self._timer.daemon = True
        self._timer.start()

    def _cancel_timer(self):
        """Cancel the flush timer if running."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _flush_callback(self):
        """Timer callback that flushes and restarts the timer."""
        self._flush()
        self._start_timer()

    def __call__(self, record: logging.LogRecord, line: str):
        """Buffer the log record for later batch sending."""
        labels = self.build_tags(record)
        ns = 1e9
        ts = str(int(record.created * ns))
        labels_key = self._labels_to_key(labels)

        with self._lock:
            if labels_key not in self._buffer:
                self._buffer[labels_key] = []
                self._labels_map[labels_key] = labels
            self._buffer[labels_key].append((ts, line))
            buffer_size = sum(len(v) for v in self._buffer.values())

        if self.verbose:
            self._logger.debug("Record buffered: logger=%s, level=%s, buffer_size=%d", record.name, record.levelname, buffer_size)

    def _flush(self):
        """Flush all buffered records to Loki."""
        with self._lock:
            if not self._buffer:
                if self.verbose:
                    self._logger.debug("Flush skipped: buffer is empty")
                return

            # Take ownership of current buffer and create new empty one
            buffer = self._buffer
            labels_map = self._labels_map
            self._buffer = {}
            self._labels_map = {}

        # Build and send the batched payload
        total_records = sum(len(v) for v in buffer.values())
        num_streams = len(buffer)
        payload = self._build_batch_payload(buffer, labels_map)

        if self.verbose:
            self._logger.debug("Flushing batch to Loki: streams=%d, total_records=%d, url=%s", num_streams, total_records, self.url)

        try:
            resp = self.session.post(self.url, json=payload, headers=self.headers, verify=self.verify_ssl)
            if resp.status_code != self.success_response_code:
                raise ValueError("Unexpected Loki API response status code: {0}".format(resp.status_code))
            if self.verbose:
                self._logger.debug("Flush successful: status_code=%d, streams=%d, records=%d", resp.status_code, num_streams, total_records)
        except Exception as e:
            if self.verbose:
                self._logger.debug("Flush failed: error=%s, restoring %d records to buffer", str(e), total_records)
            # On failure, try to restore the buffer so records aren't lost
            with self._lock:
                for labels_key, values in buffer.items():
                    if labels_key not in self._buffer:
                        self._buffer[labels_key] = []
                        self._labels_map[labels_key] = labels_map[labels_key]
                    self._buffer[labels_key].extend(values)
            raise

    def _build_batch_payload(self, buffer: Dict[str, List[Tuple[str, str]]], labels_map: Dict[str, Dict[str, Any]]) -> dict:
        """Build JSON payload with multiple streams for batch sending."""
        streams = []
        for labels_key, values in buffer.items():
            labels = labels_map[labels_key]
            stream = {
                "stream": labels,
                "values": [[ts, line] for ts, line in values],
            }
            streams.append(stream)
        return {"streams": streams}

    def flush(self):
        """Manually flush all buffered records to Loki."""
        self._flush()

    def close(self):
        """Cancel timer, flush remaining records, and close HTTP session."""
        if self.verbose:
            self._logger.debug("Closing LokiBatchEmitter, flushing remaining records...")
        self._closed = True
        self._cancel_timer()
        try:
            self._flush()
        except Exception as e:
            if self.verbose:
                self._logger.debug("Final flush on close failed: %s", str(e))
        super().close()
        if self.verbose:
            self._logger.debug("LokiBatchEmitter closed")
