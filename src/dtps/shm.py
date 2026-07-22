"""Optional shared-memory transport for DTPS ``RawData`` messages."""

from __future__ import annotations

import asyncio
import math
from contextlib import suppress
from threading import Lock
from typing import Awaitable, Callable

import cbor2

from dtps_http import ContentType, EveryOnceInAWhile, RawData
from dtps_http.shm import ShmReader, ShmWriter

from . import logger
from .ergo_ui import SubscriptionInterface

__all__ = [
    "ShmWriterPool",
    "create_shm_subscription",
    "decode_shm_raw_data",
    "encode_shm_raw_data",
    "should_publish_http",
    "validate_max_frequency",
    "validate_shm_configuration",
]

_WIRE_CONTENT = "content"
_WIRE_CONTENT_TYPE = "content_type"
_WIRE_VERSION = "version"
_WIRE_VERSION_VALUE = 1
_ERROR_SHM_PATH_REQUIRED = "shm_path is required when shm_only is True."
_ERROR_PAYLOAD_NOT_MAPPING = "DTPS shared-memory payload is not a mapping."
_ERROR_PAYLOAD_VERSION = "Unsupported DTPS shared-memory payload version."
_ERROR_PAYLOAD_CONTENT = "DTPS shared-memory payload content is not bytes."
_ERROR_PAYLOAD_CONTENT_TYPE = (
    "DTPS shared-memory payload content type is not text."
)
_ERROR_MAX_FREQUENCY = "max_frequency must be a finite positive number."


def validate_shm_configuration(
    shm_path: str | None,
    shm_only: bool,  # noqa: FBT001
) -> None:
    """Reject an SHM-only request that does not identify a channel."""
    if shm_only and not shm_path:
        raise ValueError(_ERROR_SHM_PATH_REQUIRED)


def validate_max_frequency(max_frequency: float | None) -> None:
    """Reject subscription rate limits that cannot define an interval."""
    if max_frequency is None:
        return
    if not math.isfinite(max_frequency) or max_frequency <= 0:
        raise ValueError(_ERROR_MAX_FREQUENCY)


def encode_shm_raw_data(raw_data: RawData) -> bytes:
    """Encode a ``RawData`` message for the bytes-only SHM transport."""
    return cbor2.dumps(
        {
            _WIRE_VERSION: _WIRE_VERSION_VALUE,
            _WIRE_CONTENT: raw_data.content,
            _WIRE_CONTENT_TYPE: str(raw_data.content_type),
        },
    )


def decode_shm_raw_data(payload: bytes) -> RawData:
    """Decode a ``RawData`` message received through shared memory."""
    wire_data = cbor2.loads(payload)
    if not isinstance(wire_data, dict):
        raise TypeError(_ERROR_PAYLOAD_NOT_MAPPING)
    if wire_data.get(_WIRE_VERSION) != _WIRE_VERSION_VALUE:
        raise ValueError(_ERROR_PAYLOAD_VERSION)

    content = wire_data.get(_WIRE_CONTENT)
    content_type = wire_data.get(_WIRE_CONTENT_TYPE)
    if not isinstance(content, bytes):
        raise TypeError(_ERROR_PAYLOAD_CONTENT)
    if not isinstance(content_type, str):
        raise TypeError(_ERROR_PAYLOAD_CONTENT_TYPE)
    raw_content_type = ContentType(content_type)
    return RawData(
        content=content,
        content_type=raw_content_type,
    )


class _ShmWriterEntry:
    """Coordinate access to one reusable shared-memory writer."""

    def __init__(self, writer: ShmWriter) -> None:
        """Store a writer and its lifecycle lock."""
        self.writer = writer
        self.lock = Lock()
        self.closed = False


class ShmWriterPool:
    """Manage reusable SHM writers with synchronized lifecycle."""

    def __init__(self) -> None:
        """Initialize an empty writer pool."""
        self._lock = Lock()
        self._writers: dict[str, _ShmWriterEntry] = {}
        self._closed = False

    def _get_writer_entry(self, shm_path: str) -> _ShmWriterEntry | None:
        """Get or create the active entry for one channel."""
        with self._lock:
            if self._closed:
                return None
            writer_entry = self._writers.get(shm_path)
            if writer_entry is not None:
                return writer_entry
            writer = ShmWriter(
                shm_path,
                logger.warning,
                logger.error,
            )
            writer_entry = _ShmWriterEntry(writer)
            self._writers[shm_path] = writer_entry
            return writer_entry

    def _discard_writer_entry_locked(
        self,
        shm_path: str,
        writer_entry: _ShmWriterEntry,
    ) -> None:
        """Close a failed entry before detaching it from the pool."""
        try:
            writer_entry.writer.close()
        finally:
            with self._lock:
                if self._writers.get(shm_path) is writer_entry:
                    self._writers.pop(shm_path)

    def _close_writer_entry(self, writer_entry: _ShmWriterEntry) -> None:
        """Close an entry after its channel publish completes."""
        with writer_entry.lock:
            if writer_entry.closed:
                return
            writer_entry.closed = True
            writer_entry.writer.close()

    def publish(self, raw_data: RawData, shm_path: str) -> bool:
        """Publish ``raw_data`` and report SHM delivery."""
        cleanup_error: Exception | None = None
        try:
            encoded_raw_data = encode_shm_raw_data(raw_data)
            writer_entry = self._get_writer_entry(shm_path)
            if writer_entry is None:
                return False
            with writer_entry.lock:
                if writer_entry.closed:
                    return False
                try:
                    writer_entry.writer.publish(encoded_raw_data)
                except Exception:
                    writer_entry.closed = True
                    try:
                        self._discard_writer_entry_locked(
                            shm_path,
                            writer_entry,
                        )
                    except Exception as discard_error:  # noqa: BLE001
                        cleanup_error = discard_error
                    raise
        except Exception as publish_error:  # noqa: BLE001
            if cleanup_error is None:
                logger.warning(
                    "DTPS shared-memory publish to %r failed: %s; "
                    "falling back to normal DTPS delivery.",
                    shm_path,
                    publish_error,
                )
            else:
                logger.warning(
                    "DTPS shared-memory publish to %r failed: %s; "
                    "writer cleanup also failed: %s; falling back to normal "
                    "DTPS delivery.",
                    shm_path,
                    publish_error,
                    cleanup_error,
                )
            return False
        return True

    def close(self) -> None:
        """Close all writers managed by this pool."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            writer_entry_values = self._writers.values()
            writer_entries = list(writer_entry_values)
            self._writers.clear()
        for writer_entry in writer_entries:
            self._close_writer_entry(writer_entry)


def should_publish_http(
    writer_pool: ShmWriterPool,
    raw_data: RawData,
    *,
    shm_path: str | None,
    shm_only: bool,
) -> bool:
    """Publish to SHM and indicate whether HTTP delivery is required."""
    validate_shm_configuration(shm_path, shm_only=shm_only)
    if not shm_path:
        return True
    shm_published = writer_pool.publish(raw_data, shm_path)
    if not shm_only:
        return True
    return not shm_published


class _ShmSubscription(SubscriptionInterface):
    """Deliver an SHM channel through DTPS's async callback contract."""

    def __init__(
        self,
        on_data: Callable[[RawData], Awaitable[None]],
        shm_path: str,
        queue_size: int,
        *,
        max_frequency: float | None = None,
        on_unsubscribe: Callable[[SubscriptionInterface], None] | None = None,
    ) -> None:
        """Start a reader and processor for a single SHM channel."""
        validate_max_frequency(max_frequency)
        self._closed = False
        self._loop = asyncio.get_running_loop()
        self._on_data = on_data
        self._on_unsubscribe = on_unsubscribe
        self._when = EveryOnceInAWhile(
            1 / max_frequency if max_frequency is not None else 0,
        )
        self._queue: asyncio.Queue[RawData] = asyncio.Queue(
            maxsize=queue_size,
        )
        self._reader = ShmReader(
            shm_path,
            self._on_payload,
            logger.warning,
            logger.error,
        )
        self._reader.start()
        processor = self._process()
        try:
            self._processor_task = self._loop.create_task(processor)
        except Exception:
            self._closed = True
            processor.close()
            self._reader.stop()
            raise

    def _on_payload(self, payload: bytes) -> None:
        """Decode one SHM payload and transfer it to the event loop."""
        try:
            raw_data = decode_shm_raw_data(payload)
        except Exception as error:  # noqa: BLE001
            logger.warning(
                "Ignoring invalid DTPS shared-memory payload: %s",
                error,
            )
            return
        try:
            self._loop.call_soon_threadsafe(self._enqueue, raw_data)
        except RuntimeError:
            # The loop can close while the reader callback is returning.
            return

    def _enqueue(self, raw_data: RawData) -> None:
        """Keep only the latest pending callback payload."""
        if self._closed or not self._when.now():
            return
        while True:
            with suppress(asyncio.QueueFull):
                self._queue.put_nowait(raw_data)
                return
            with suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
                self._queue.task_done()

    async def _process(self) -> None:
        """Run user callbacks in the subscribing event loop."""
        while True:
            raw_data = await self._queue.get()
            try:
                await self._on_data(raw_data)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Exception in DTPS shared-memory subscription callback.",
                )
            finally:
                self._queue.task_done()
            if self._closed:
                return

    async def unsubscribe(self) -> None:
        """Stop the reader and cancel its callback processor."""
        if self._closed:
            return
        self._closed = True
        try:
            self._reader.stop()
        finally:
            processor_task = self._processor_task
            if processor_task is not asyncio.current_task():
                processor_task.cancel()
                await asyncio.gather(
                    processor_task,
                    return_exceptions=True,
                )
            on_unsubscribe = self._on_unsubscribe
            if on_unsubscribe is not None:
                on_unsubscribe(self)


def create_shm_subscription(
    on_data: Callable[[RawData], Awaitable[None]],
    shm_path: str,
    queue_size: int,
    *,
    max_frequency: float | None = None,
    on_unsubscribe: Callable[[SubscriptionInterface], None] | None = None,
) -> SubscriptionInterface:
    """Build a subscription that receives from ``shm_path``."""
    return _ShmSubscription(
        on_data,
        shm_path,
        queue_size,
        max_frequency=max_frequency,
        on_unsubscribe=on_unsubscribe,
    )
