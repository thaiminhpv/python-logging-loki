# -*- coding: utf-8 -*-

import logging
import threading
import time
from logging.config import dictConfig as loggingDictConfig
from queue import Queue
from typing import Tuple
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from logging_loki.emitter import LokiBatchEmitter

emitter_url: str = "https://example.net/loki/api/v1/push/"
record_kwargs = {
    "name": "test",
    "level": logging.WARNING,
    "fn": "",
    "lno": "",
    "msg": "Test",
    "args": None,
    "exc_info": None,
}


@pytest.fixture()
def batch_emitter() -> Tuple[LokiBatchEmitter, MagicMock]:
    """Create batch emitter with mocked http session."""
    response = MagicMock()
    response.status_code = LokiBatchEmitter.success_response_code
    session = MagicMock()
    session().post = MagicMock(return_value=response)

    # Use a long flush interval to prevent auto-flush during tests
    instance = LokiBatchEmitter(url=emitter_url, flush_interval=60.0)
    instance.session_class = session

    yield instance, session

    # Clean up timer
    instance.close()


def create_record(**kwargs) -> logging.LogRecord:
    """Create test logging record."""
    log = logging.Logger(__name__)
    return log.makeRecord(**{**record_kwargs, **kwargs})


def get_streams(session: MagicMock) -> list:
    """Return all streams from json payload."""
    kwargs = session().post.call_args[1]
    return kwargs["json"]["streams"]


def get_stream(session: MagicMock) -> dict:
    """Return first stream item from json payload."""
    return get_streams(session)[0]


def test_records_are_buffered_not_sent_immediately(batch_emitter):
    """Test that records are buffered and not sent until flush."""
    emitter, session = batch_emitter
    emitter(create_record(), "Test message 1")
    emitter(create_record(), "Test message 2")

    # Should not have called post yet
    session().post.assert_not_called()

    # After flush, should have called post
    emitter.flush()
    session().post.assert_called_once()


def test_records_with_same_labels_grouped_in_one_stream(batch_emitter):
    """Test that records with same labels are grouped together."""
    emitter, session = batch_emitter

    # Create multiple records with same labels
    emitter(create_record(), "Message 1")
    emitter(create_record(), "Message 2")
    emitter(create_record(), "Message 3")

    emitter.flush()

    streams = get_streams(session)
    assert len(streams) == 1
    assert len(streams[0]["values"]) == 3
    assert streams[0]["values"][0][1] == "Message 1"
    assert streams[0]["values"][1][1] == "Message 2"
    assert streams[0]["values"][2][1] == "Message 3"


def test_records_with_different_labels_go_to_separate_streams(batch_emitter):
    """Test that records with different labels go to different streams."""
    emitter, session = batch_emitter

    # Create records with different log levels (which affects labels)
    record_warning = create_record(level=logging.WARNING)
    record_error = create_record(level=logging.ERROR)

    emitter(record_warning, "Warning message")
    emitter(record_error, "Error message")

    emitter.flush()

    streams = get_streams(session)
    assert len(streams) == 2

    # Find streams by severity
    severities = {s["stream"]["severity"]: s for s in streams}
    assert "warning" in severities
    assert "error" in severities
    assert severities["warning"]["values"][0][1] == "Warning message"
    assert severities["error"]["values"][0][1] == "Error message"


def test_extra_tags_create_separate_streams(batch_emitter):
    """Test that records with different extra tags go to different streams."""
    emitter, session = batch_emitter

    record1 = create_record(extra={"tags": {"service": "api"}})
    record2 = create_record(extra={"tags": {"service": "worker"}})

    emitter(record1, "API message")
    emitter(record2, "Worker message")

    emitter.flush()

    streams = get_streams(session)
    assert len(streams) == 2


def test_flush_clears_buffer(batch_emitter):
    """Test that flush clears the buffer."""
    emitter, session = batch_emitter

    emitter(create_record(), "Message 1")
    emitter.flush()

    # First flush should send the record
    assert session().post.call_count == 1

    # Second flush should not send anything (buffer is empty)
    emitter.flush()
    assert session().post.call_count == 1


def test_flush_triggered_by_interval():
    """Test that flush is triggered after the specified interval."""
    response = MagicMock()
    response.status_code = LokiBatchEmitter.success_response_code
    session = MagicMock()
    session().post = MagicMock(return_value=response)

    # Use a short flush interval
    emitter = LokiBatchEmitter(url=emitter_url, flush_interval=0.1)
    emitter.session_class = session

    try:
        emitter(create_record(), "Test message")

        # Should not have called post yet
        session().post.assert_not_called()

        # Wait for the flush interval
        time.sleep(0.3)

        # Should have called post after interval
        session().post.assert_called()
    finally:
        emitter.close()


def test_remaining_records_flushed_on_close(batch_emitter):
    """Test that remaining records are flushed when emitter is closed."""
    emitter, session = batch_emitter

    emitter(create_record(), "Message 1")
    emitter(create_record(), "Message 2")

    # Should not have called post yet
    session().post.assert_not_called()

    # Close should trigger flush
    emitter.close()
    session().post.assert_called_once()

    streams = get_streams(session)
    assert len(streams[0]["values"]) == 2


def test_timer_cancelled_on_close():
    """Test that the timer is cancelled when emitter is closed."""
    emitter = LokiBatchEmitter(url=emitter_url, flush_interval=60.0)

    # Timer should be running
    assert emitter._timer is not None
    assert emitter._timer.is_alive()

    emitter.close()

    # Timer should be cancelled
    assert emitter._timer is None or not emitter._timer.is_alive()


@freeze_time("2019-11-04 00:25:08.123456")
def test_timestamp_preserved_in_batch(batch_emitter):
    """Test that timestamps are preserved correctly in batched records."""
    emitter, session = batch_emitter
    emitter(create_record(), "Test message")
    emitter.flush()

    stream = get_stream(session)
    expected = 1572827108123456000
    assert stream["values"][0][0] == str(expected)


def test_default_tags_preserved_in_batch(batch_emitter):
    """Test that default tags are preserved in batched records."""
    emitter, session = batch_emitter
    emitter.tags = {"app": "test_app"}
    emitter(create_record(), "Test message")
    emitter.flush()

    stream = get_stream(session)
    assert stream["stream"]["app"] == "test_app"


def test_thread_safety_of_buffer():
    """Test that buffer operations are thread-safe."""
    response = MagicMock()
    response.status_code = LokiBatchEmitter.success_response_code
    session = MagicMock()
    session().post = MagicMock(return_value=response)

    emitter = LokiBatchEmitter(url=emitter_url, flush_interval=60.0)
    emitter.session_class = session

    try:
        num_threads = 10
        records_per_thread = 100
        threads = []

        def emit_records(thread_id):
            for i in range(records_per_thread):
                record = create_record(msg=f"Thread {thread_id} message {i}")
                emitter(record, f"Thread {thread_id} message {i}")

        # Start multiple threads emitting records
        for i in range(num_threads):
            t = threading.Thread(target=emit_records, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # Flush and verify all records were captured
        emitter.flush()

        streams = get_streams(session)
        total_values = sum(len(s["values"]) for s in streams)
        assert total_values == num_threads * records_per_thread
    finally:
        emitter.close()


def test_raises_value_error_on_non_successful_response(batch_emitter):
    """Test that ValueError is raised on non-successful Loki response."""
    emitter, session = batch_emitter
    session().post().status_code = 500

    emitter(create_record(), "Test message")

    with pytest.raises(ValueError):
        emitter.flush()


def test_buffer_restored_on_flush_failure(batch_emitter):
    """Test that buffer is restored if flush fails."""
    emitter, session = batch_emitter
    session().post.side_effect = Exception("Connection error")

    emitter(create_record(), "Test message")

    # Flush should fail
    with pytest.raises(Exception):
        emitter.flush()

    # Buffer should still contain the record
    assert len(emitter._buffer) > 0

    # Fix the session and retry
    session().post.side_effect = None
    response = MagicMock()
    response.status_code = LokiBatchEmitter.success_response_code
    session().post.return_value = response

    emitter.flush()
    session().post.assert_called()


def test_batch_emitter_sent_to_correct_url(batch_emitter):
    """Test that batched records are sent to the correct URL."""
    emitter, session = batch_emitter
    emitter(create_record(), "Test message")
    emitter.flush()

    got = session().post.call_args
    assert got[0][0] == emitter_url


def test_can_use_with_logging_dict_config():
    """Test that LokiBatchQueueHandler works with logging dictConfig."""
    logger_name = "batch_handler_test"
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            logger_name: {
                "class": "logging_loki.LokiBatchQueueHandler",
                "queue": Queue(-1),
                "url": emitter_url,
                "tags": {"test": "test"},
                "flush_interval": 60.0,
            },
        },
        "loggers": {logger_name: {"handlers": [logger_name], "level": "DEBUG"}},
    }
    loggingDictConfig(config)

    logger = logging.getLogger(logger_name)
    handler = logger.handlers[0]

    # Verify the handler was created correctly
    assert handler.__class__.__name__ == "LokiBatchQueueHandler"
    assert handler.handler.emitter.__class__.__name__ == "LokiBatchEmitter"
    assert handler.handler.emitter.flush_interval == 60.0

    # Clean up
    handler.close()
