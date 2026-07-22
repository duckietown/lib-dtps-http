"""Latest-value shared-memory transport for one producer and one reader.

The channel stores opaque bytes in a memory-mapped file. A ``.p2c`` FIFO
notifies the reader after each write. Its header carries capacity and
current payload length. Zero length marks an update in progress.

A ``.lock`` file serializes shared-memory state. Writers hold exclusive
locks while initializing, resizing, or replacing payloads. Readers hold
shared locks while remapping and copying. This prevents overwrites while
another endpoint copies bytes.

This latest-frame transport is not a reliable queue. Writers can replace
unread data. FIFO notifications can be dropped after newer data arrives.
Consumers must tolerate drops or repeats. Payloads stay coherent for
cooperating endpoints.
"""

from __future__ import annotations

import errno
import os
import select
import stat
import struct
import time
from contextlib import contextmanager, suppress
from importlib import import_module
from mmap import mmap
from pathlib import Path
from threading import Thread, current_thread
from typing import Any, Callable, Iterator

from .constants import (
    SHM_CHANNEL_DIRECTORY_MODE,
    SHM_CHANNEL_FILE_MODE,
    SHM_DEFAULT_CAPACITY,
    SHM_ERROR_CAPACITY,
    SHM_ERROR_CAPACITY_EXCEEDS_MAXIMUM,
    SHM_ERROR_CHANNEL_DIRECTORY_NOT_DIRECTORY,
    SHM_ERROR_CHANNEL_DIRECTORY_NOT_OWNER,
    SHM_ERROR_CHANNEL_DIRECTORY_SYMLINK,
    SHM_ERROR_CHANNEL_DIRECTORY_WORLD_WRITABLE,
    SHM_ERROR_CHANNEL_PATH_HAS_MULTIPLE_LINKS,
    SHM_ERROR_CHANNEL_PATH_NOT_REGULAR_FILE,
    SHM_ERROR_CLOSE_LOCK_FILE,
    SHM_ERROR_CLOSE_MAP,
    SHM_ERROR_CLOSE_SIGNAL_FIFO,
    SHM_ERROR_HANDLE_PAYLOAD,
    SHM_ERROR_HEADER_MAGIC,
    SHM_ERROR_HEADER_SIZE,
    SHM_ERROR_HEADER_UNPACK,
    SHM_ERROR_PAYLOAD_LENGTH,
    SHM_ERROR_READ_PAYLOAD,
    SHM_ERROR_READER_LOCK_NOT_OPEN,
    SHM_ERROR_READER_NOT_OPEN,
    SHM_ERROR_READER_REMAP_FAILED,
    SHM_ERROR_READER_STOPPING,
    SHM_ERROR_SIGNAL_FIFO_ACCESSIBLE_BY_OTHERS,
    SHM_ERROR_SIGNAL_FIFO_NOT_OWNER,
    SHM_ERROR_SIGNAL_PATH_NOT_FIFO,
    SHM_ERROR_SIGNAL_POLL_FAILED,
    SHM_ERROR_SIGNAL_READ_FAILED,
    SHM_ERROR_UNSUPPORTED_PLATFORM,
    SHM_ERROR_UNSUPPORTED_VERSION,
    SHM_ERROR_WRITER_LOCK_NOT_OPEN,
    SHM_ERROR_WRITER_MAP_NOT_OPEN,
    SHM_ERROR_WRITER_NOT_OPEN,
    SHM_FIFO_FULL_WARNING,
    SHM_HEADER_FORMAT,
    SHM_HEADER_SIZE,
    SHM_LOCK_SUFFIX,
    SHM_MAGIC,
    SHM_MAX_CAPACITY,
    SHM_SIGNAL_DRAIN_READ_SIZE,
    SHM_SIGNAL_DRAIN_READS,
    SHM_SIGNAL_FORMAT,
    SHM_SIGNAL_POLL_TIMEOUT_SECONDS,
    SHM_SIGNAL_SUFFIX,
    SHM_STOP_JOIN_TIMEOUT_SECONDS,
    SHM_VERSION,
    SHM_WARNING_READER_STILL_STOPPING,
    SHM_WARNING_RESET_HEADER,
)

fcntl: Any = None
with suppress(ModuleNotFoundError):
    fcntl = import_module("fcntl")

_REQUIRED_FCNTL_CAPABILITIES = (
    "flock",
    "LOCK_EX",
    "LOCK_SH",
    "LOCK_UN",
)
_REQUIRED_OS_CAPABILITIES = (
    "O_CLOEXEC",
    "O_NOFOLLOW",
    "O_NONBLOCK",
    "chmod",
    "fchmod",
    "ftruncate",
    "geteuid",
    "mkfifo",
    "pread",
    "pwrite",
)


def _ensure_supported_platform() -> None:
    """Reject unsupported platform capability sets."""
    missing_capabilities: list[str] = []
    if os.name != "posix":
        missing_capabilities.append("POSIX")

    fcntl_module = fcntl
    if fcntl_module is None:
        missing_capabilities.append("fcntl")
    else:
        missing_fcntl_capabilities = [
            f"fcntl.{capability}"
            for capability in _REQUIRED_FCNTL_CAPABILITIES
            if getattr(fcntl_module, capability, None) is None
        ]
        missing_capabilities.extend(missing_fcntl_capabilities)

    missing_os_capabilities = [
        f"os.{capability}"
        for capability in _REQUIRED_OS_CAPABILITIES
        if getattr(os, capability, None) is None
    ]
    missing_capabilities.extend(missing_os_capabilities)

    if missing_capabilities:
        message = (
            f"{SHM_ERROR_UNSUPPORTED_PLATFORM}: "
            f"{', '.join(missing_capabilities)}."
        )
        raise OSError(message)


def _validate_capacity(capacity: int) -> None:
    """Reject channel capacities outside the supported range."""
    if capacity <= 0:
        raise ValueError(SHM_ERROR_CAPACITY)
    if capacity <= SHM_MAX_CAPACITY:
        return
    message = (
        f"{SHM_ERROR_CAPACITY_EXCEEDS_MAXIMUM}: "
        f"{capacity} > {SHM_MAX_CAPACITY}."
    )
    raise ValueError(message)


def _pack_header(capacity: int, payload_length: int) -> bytes:
    """Return the on-disk header for the current buffer state."""
    _validate_capacity(capacity)
    return struct.pack(
        SHM_HEADER_FORMAT,
        SHM_MAGIC,
        SHM_VERSION,
        capacity,
        payload_length,
    )


@contextmanager
def _channel_lock(
    lock_file_descriptor: int,
    lock_mode: int,
) -> Iterator[None]:
    """Hold a cooperative process-shared lock for one operation."""
    fcntl.flock(lock_file_descriptor, lock_mode)
    try:
        yield
    finally:
        fcntl.flock(lock_file_descriptor, fcntl.LOCK_UN)


def _validate_channel_directory(
    channel_directory: Path,
    directory_status: os.stat_result,
    *,
    allow_sticky_writable_directory: bool = False,
) -> None:
    """Reject unsafe channel directories."""
    directory_name = str(channel_directory)
    if stat.S_ISLNK(directory_status.st_mode):
        message = f"{SHM_ERROR_CHANNEL_DIRECTORY_SYMLINK}: '{directory_name}'"
        raise ValueError(message)
    if not stat.S_ISDIR(directory_status.st_mode):
        message = (
            f"{SHM_ERROR_CHANNEL_DIRECTORY_NOT_DIRECTORY}: '{directory_name}'"
        )
        raise ValueError(message)
    if directory_status.st_uid not in (0, os.geteuid()):
        message = (
            f"{SHM_ERROR_CHANNEL_DIRECTORY_NOT_OWNER}: '{directory_name}'"
        )
        raise ValueError(message)
    if directory_status.st_mode & (stat.S_IWGRP | stat.S_IWOTH) and not (
        allow_sticky_writable_directory
        and directory_status.st_mode & stat.S_ISVTX
    ):
        message = (
            f"{SHM_ERROR_CHANNEL_DIRECTORY_WORLD_WRITABLE}: '{directory_name}'"
        )
        raise ValueError(message)


def _channel_directory_components(
    channel_directory: Path,
) -> list[Path]:
    """Return the channel directory components from shallow to deep."""
    directory_components = [channel_directory]
    directory_components.extend(channel_directory.parents)
    directory_components.reverse()
    return directory_components


def _ensure_channel_directory(channel_path: str) -> None:
    """Create and validate a channel artifact directory."""
    channel_file_path = Path(channel_path)
    channel_directory = channel_file_path.parent
    directory_components = _channel_directory_components(channel_directory)
    for directory_component in directory_components:
        is_channel_directory = directory_component == channel_directory
        try:
            directory_component.mkdir(mode=SHM_CHANNEL_DIRECTORY_MODE)
        except FileExistsError:
            directory_status = os.lstat(directory_component)
        else:
            directory_component.chmod(SHM_CHANNEL_DIRECTORY_MODE)
            directory_status = os.lstat(directory_component)
        _validate_channel_directory(
            directory_component,
            directory_status,
            allow_sticky_writable_directory=not is_channel_directory,
        )


def _ensure_signal_fifo(
    signal_path: str,
) -> tuple[bool, tuple[int, int] | None]:
    """Create a FIFO or reject a conflicting existing path."""
    try:
        os.mkfifo(signal_path, SHM_CHANNEL_FILE_MODE)
    except FileExistsError:
        signal_status = os.lstat(signal_path)
        _validate_signal_fifo_status(signal_status, signal_path)
        return False, None
    signal_status = os.lstat(signal_path)
    signal_identity = (signal_status.st_dev, signal_status.st_ino)
    try:
        _validate_signal_fifo_status(signal_status, signal_path)
        signal_file_path = Path(signal_path)
        signal_file_path.chmod(SHM_CHANNEL_FILE_MODE)
        signal_status = os.lstat(signal_path)
        _validate_signal_fifo_status(signal_status, signal_path)
    except Exception:
        with suppress(OSError):
            _unlink_signal_fifo_if_unchanged(
                signal_path,
                signal_identity,
            )
        raise
    return True, signal_identity


def _unlink_signal_fifo_if_unchanged(
    signal_path: str,
    expected_identity: tuple[int, int],
) -> None:
    """Remove a newly created FIFO only when its path still names it."""
    try:
        signal_status = os.lstat(signal_path)
    except FileNotFoundError:
        return
    if not stat.S_ISFIFO(signal_status.st_mode):
        return
    if (signal_status.st_dev, signal_status.st_ino) != expected_identity:
        return
    signal_file_path = Path(signal_path)
    signal_file_path.unlink()


def _validate_signal_file_descriptor(
    signal_file_descriptor: int,
    signal_path: str,
) -> None:
    """Verify a signal descriptor meets the private FIFO contract."""
    signal_status = os.fstat(signal_file_descriptor)
    _validate_signal_fifo_status(signal_status, signal_path)


def _validate_signal_fifo_status(
    signal_status: os.stat_result,
    signal_path: str,
) -> None:
    """Verify a signal path status meets the private FIFO contract."""
    if stat.S_ISFIFO(signal_status.st_mode):
        if signal_status.st_uid not in (0, os.geteuid()):
            message = f"{SHM_ERROR_SIGNAL_FIFO_NOT_OWNER}: '{signal_path}'"
            raise ValueError(message)
        if signal_status.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            message = (
                f"{SHM_ERROR_SIGNAL_FIFO_ACCESSIBLE_BY_OTHERS}: "
                f"'{signal_path}'"
            )
            raise ValueError(message)
        return
    message = f"{SHM_ERROR_SIGNAL_PATH_NOT_FIFO}: '{signal_path}'"
    raise ValueError(message)


def _open_signal_fifo(signal_path: str, *, created: bool) -> int:
    """Open a signal FIFO without following a symlink substitution."""
    signal_file_descriptor = os.open(
        signal_path,
        os.O_RDWR | os.O_NONBLOCK | os.O_CLOEXEC | os.O_NOFOLLOW,
    )
    try:
        _validate_signal_file_descriptor(signal_file_descriptor, signal_path)
        if created:
            os.fchmod(signal_file_descriptor, SHM_CHANNEL_FILE_MODE)
    except Exception:
        os.close(signal_file_descriptor)
        raise
    return signal_file_descriptor


def _open_or_create_channel_file(
    channel_path: str,
    open_flags: int,
    mode: int,
) -> tuple[int, bool]:
    """Open a channel file and report whether this call created it."""
    try:
        file_descriptor = os.open(
            channel_path,
            open_flags | os.O_EXCL,
            mode,
        )
    except FileExistsError:
        file_descriptor = os.open(
            channel_path,
            open_flags & ~(os.O_CREAT | os.O_EXCL),
        )
        return file_descriptor, False
    return file_descriptor, True


def _open_channel_file_descriptor(
    channel_path: str,
    flags: int,
    mode: int | None,
) -> tuple[int, bool]:
    """Open a channel descriptor and report whether it was created."""
    open_flags = flags | os.O_CLOEXEC | os.O_NOFOLLOW
    if mode is None:
        return os.open(channel_path, open_flags), False
    if flags & os.O_CREAT:
        return _open_or_create_channel_file(channel_path, open_flags, mode)
    return os.open(channel_path, open_flags, mode), False


def _raise_channel_path_not_regular(
    channel_path: str,
    error: OSError | None = None,
) -> None:
    """Raise the unsafe artifact-path error."""
    message = f"{SHM_ERROR_CHANNEL_PATH_NOT_REGULAR_FILE}: '{channel_path}'"
    if error is None:
        raise ValueError(message)
    raise ValueError(message) from error


def _validate_channel_file_descriptor(
    file_descriptor: int,
    channel_path: str,
) -> None:
    """Verify that a channel descriptor is singly linked."""
    file_status = os.fstat(file_descriptor)
    if not stat.S_ISREG(file_status.st_mode):
        _raise_channel_path_not_regular(channel_path)
    if file_status.st_nlink == 1:
        return
    message = (
        f"{SHM_ERROR_CHANNEL_PATH_HAS_MULTIPLE_LINKS}: '{channel_path}'"
    )
    raise ValueError(message)


def _open_channel_file(
    channel_path: str,
    flags: int,
    mode: int | None = None,
) -> int:
    """Open an unlinked regular artifact without following symlinks."""
    try:
        file_descriptor, created = _open_channel_file_descriptor(
            channel_path,
            flags,
            mode,
        )
    except OSError as error:
        if error.errno == errno.ELOOP:
            _raise_channel_path_not_regular(channel_path, error)
        raise

    try:
        _validate_channel_file_descriptor(file_descriptor, channel_path)
        if created and mode is not None:
            os.fchmod(file_descriptor, mode)
    except Exception:
        os.close(file_descriptor)
        raise
    return file_descriptor


def _unpack_header(header: bytes) -> tuple[int, int]:
    """Validate a header and return capacity and payload length."""
    if len(header) != SHM_HEADER_SIZE:
        raise ValueError(SHM_ERROR_HEADER_SIZE)
    try:
        magic, version, capacity, payload_length = struct.unpack(
            SHM_HEADER_FORMAT,
            header,
        )
    except struct.error as error:
        raise ValueError(SHM_ERROR_HEADER_UNPACK) from error
    if magic != SHM_MAGIC:
        raise ValueError(SHM_ERROR_HEADER_MAGIC)
    if version != SHM_VERSION:
        message = f"{SHM_ERROR_UNSUPPORTED_VERSION}: {version}"
        raise ValueError(message)
    _validate_capacity(capacity)
    if payload_length > capacity:
        raise ValueError(SHM_ERROR_PAYLOAD_LENGTH)
    return capacity, payload_length


class _ShmEndpoint:
    """Manage resources shared by a shared-memory reader or writer."""

    def __init__(
        self,
        shm_path: str,
        logwarn: Callable[[str], None],
        logerr: Callable[[str], None],
        lock_not_open_message: str,
    ) -> None:
        """Initialize shared endpoint resources."""
        _ensure_supported_platform()
        self._capacity = SHM_DEFAULT_CAPACITY
        self._logerr = logerr
        self._logwarn = logwarn
        self._lock_not_open_message = lock_not_open_message
        self._shm_path = str(shm_path)
        self._lock_file_descriptor: int | None = None
        self._lock_path = self._shm_path + SHM_LOCK_SUFFIX
        self._map_size = SHM_HEADER_SIZE + self._capacity
        self._memory_map: mmap | None = None
        self._signal_file_descriptor: int | None = None
        self._signal_path = self._shm_path + SHM_SIGNAL_SUFFIX

    def _close_handles(self) -> None:
        """Release the mapping, FIFO descriptor, and lock file."""
        memory_map = self._memory_map
        self._memory_map = None
        try:
            if memory_map is not None:
                memory_map.close()
        except Exception as error:  # noqa: BLE001
            message = f"{SHM_ERROR_CLOSE_MAP}: {error}"
            self._logerr(message)

        signal_file_descriptor = self._signal_file_descriptor
        self._signal_file_descriptor = None
        try:
            if signal_file_descriptor is not None:
                os.close(signal_file_descriptor)
        except OSError as error:
            message = f"{SHM_ERROR_CLOSE_SIGNAL_FIFO}: {error}"
            self._logerr(message)

        lock_file_descriptor = self._lock_file_descriptor
        self._lock_file_descriptor = None
        try:
            if lock_file_descriptor is not None:
                os.close(lock_file_descriptor)
        except OSError as error:
            message = f"{SHM_ERROR_CLOSE_LOCK_FILE}: {error}"
            self._logerr(message)

    def _ensure_lock_open(self) -> None:
        """Open the dedicated advisory lock file for this channel."""
        if self._lock_file_descriptor is not None:
            return
        _ensure_channel_directory(self._shm_path)
        # Use a separate inode so locks survive backing-file resizing.
        self._lock_file_descriptor = _open_channel_file(
            self._lock_path,
            os.O_CREAT | os.O_RDWR,
            SHM_CHANNEL_FILE_MODE,
        )

    def _ensure_open(self) -> None:
        """Create and map the channel for this endpoint."""
        if (
            self._lock_file_descriptor is not None
            and self._memory_map is not None
            and self._signal_file_descriptor is not None
        ):
            return
        self._ensure_lock_open()
        lock_file_descriptor = self._lock_file_descriptor
        if lock_file_descriptor is None:
            raise RuntimeError(self._lock_not_open_message)

        # Initialization can reset shared state. Use one exclusive lock
        # for both endpoints.
        try:
            with _channel_lock(lock_file_descriptor, fcntl.LOCK_EX):
                self._open_channel_locked()
        except Exception:
            self._close_handles()
            raise

    def _open_channel_locked(self) -> None:
        """Prepare, map, and open the channel while holding its lock."""
        signal_created = False
        signal_identity: tuple[int, int] | None = None
        try:
            self._prepare_channel_locked()
            signal_created, signal_identity = _ensure_signal_fifo(
                self._signal_path,
            )
            self._memory_map = self._open_memory_map(self._map_size)
            self._signal_file_descriptor = _open_signal_fifo(
                self._signal_path,
                created=signal_created,
            )
        except Exception:
            if signal_created and signal_identity is not None:
                self._remove_created_signal_fifo(signal_identity)
            raise

    def _remove_created_signal_fifo(
        self,
        signal_identity: tuple[int, int],
    ) -> None:
        """Best-effort cleanup for the FIFO created by this endpoint."""
        try:
            _unlink_signal_fifo_if_unchanged(
                self._signal_path,
                signal_identity,
            )
        except OSError as error:
            message = (
                "Unable to remove newly created shared-memory signal FIFO "
                f"'{self._signal_path}': {error}"
            )
            self._logerr(message)

    def _open_memory_map(self, size: int) -> mmap:
        """Map *size* bytes without retaining a file descriptor."""
        file_descriptor = _open_channel_file(self._shm_path, os.O_RDWR)
        try:
            return mmap(file_descriptor, size)
        finally:
            # The mapping remains valid after this descriptor closes.
            os.close(file_descriptor)

    def _reset_channel_locked(
        self,
        file_descriptor: int,
        capacity: int,
    ) -> None:
        """Reset the channel to an empty buffer while holding a lock."""
        self._capacity = capacity
        self._map_size = SHM_HEADER_SIZE + self._capacity
        empty_header = _pack_header(self._capacity, 0)
        os.ftruncate(file_descriptor, self._map_size)
        os.pwrite(file_descriptor, empty_header, 0)

    def _prepare_channel_locked(self) -> None:
        """Create or recover the channel under an exclusive lock."""
        file_descriptor = _open_channel_file(
            self._shm_path,
            os.O_CREAT | os.O_RDWR,
            SHM_CHANNEL_FILE_MODE,
        )
        try:
            file_status = os.fstat(file_descriptor)
            file_size = file_status.st_size
            if file_size < SHM_HEADER_SIZE:
                # A new or truncated file has no valid channel state.
                self._reset_channel_locked(
                    file_descriptor,
                    SHM_DEFAULT_CAPACITY,
                )
                return

            header = os.pread(file_descriptor, SHM_HEADER_SIZE, 0)
            try:
                self._capacity, _ = _unpack_header(header)
            except ValueError as error:
                # A corrupt or incompatible header cannot be reused.
                message = (
                    f"{SHM_WARNING_RESET_HEADER} '{self._shm_path}': {error}"
                )
                self._logwarn(message)
                self._reset_channel_locked(
                    file_descriptor,
                    SHM_DEFAULT_CAPACITY,
                )
                return

            self._map_size = SHM_HEADER_SIZE + self._capacity
            if file_size < self._map_size:
                # Preserve a valid header while extending the partially
                # created backing file to its advertised capacity.
                os.ftruncate(file_descriptor, self._map_size)
        finally:
            os.close(file_descriptor)


class ShmWriter(_ShmEndpoint):
    """Publish the most recent payload from a single producer.

    Each call replaces current payload. It can briefly wait for readers
    to finish coherent copies, but has no per-frame delivery guarantee.
    """

    def __init__(
        self,
        shm_path: str,
        logwarn: Callable[[str], None],
        logerr: Callable[[str], None],
    ) -> None:
        """Initialize the writer endpoint."""
        super().__init__(
            shm_path,
            logwarn,
            logerr,
            SHM_ERROR_WRITER_LOCK_NOT_OPEN,
        )
        self._signal_backpressure_warned = False

    def _resize_locked(self, min_capacity: int) -> None:
        """Grow the backing buffer while holding the exclusive lock."""
        _validate_capacity(min_capacity)
        new_capacity = max(self._capacity * 2, min_capacity)
        new_capacity = min(new_capacity, SHM_MAX_CAPACITY)
        if self._memory_map is not None:
            self._memory_map.close()
            self._memory_map = None

        file_descriptor = _open_channel_file(self._shm_path, os.O_RDWR)
        try:
            empty_header = _pack_header(new_capacity, 0)
            os.ftruncate(file_descriptor, SHM_HEADER_SIZE + new_capacity)
            # Readers treat this as empty until they remap.
            # The next map uses the new capacity.
            os.pwrite(file_descriptor, empty_header, 0)
        finally:
            os.close(file_descriptor)

        self._capacity = new_capacity
        self._map_size = SHM_HEADER_SIZE + self._capacity
        self._memory_map = self._open_memory_map(self._map_size)

    def _write_payload_locked(
        self,
        payload: bytes,
        payload_length: int,
    ) -> None:
        """Write a payload while holding the exclusive channel lock."""
        if payload_length > self._capacity:
            self._resize_locked(payload_length)

        memory_map_ = self._memory_map
        if memory_map_ is None:
            raise RuntimeError(SHM_ERROR_WRITER_MAP_NOT_OPEN)

        # The exclusive lock prevents readers from copying during a
        # replacement. Zero length protects non-cooperating readers.
        memory_map_[:SHM_HEADER_SIZE] = _pack_header(self._capacity, 0)
        memory_map_[SHM_HEADER_SIZE : SHM_HEADER_SIZE + payload_length] = (
            payload
        )
        memory_map_[:SHM_HEADER_SIZE] = _pack_header(
            self._capacity,
            payload_length,
        )

    def _signal_reader(self, signal_file_descriptor: int) -> None:
        """Send a FIFO wake-up after committing a payload."""
        # The timestamp identifies a wake-up event only.
        # The reader takes the latest mapped payload snapshot.
        signal_timestamp = time.perf_counter_ns()
        signal = struct.pack(SHM_SIGNAL_FORMAT, signal_timestamp)
        try:
            os.write(signal_file_descriptor, signal)
            self._signal_backpressure_warned = False
        except BlockingIOError:
            # A queued token wakes the reader after it catches up.
            # Dropping a token preserves latest-frame semantics.
            if not self._signal_backpressure_warned:
                self._logwarn(SHM_FIFO_FULL_WARNING)
                self._signal_backpressure_warned = True

    def publish(self, payload: bytes) -> None:
        """Replace the current payload and signal a new snapshot."""
        payload_length = len(payload)
        if payload_length == 0:
            return
        self._ensure_open()
        signal_file_descriptor = self._signal_file_descriptor
        lock_file_descriptor = self._lock_file_descriptor
        if signal_file_descriptor is None:
            raise RuntimeError(SHM_ERROR_WRITER_NOT_OPEN)
        if lock_file_descriptor is None:
            raise RuntimeError(SHM_ERROR_WRITER_NOT_OPEN)

        try:
            with _channel_lock(lock_file_descriptor, fcntl.LOCK_EX):
                self._write_payload_locked(payload, payload_length)
            self._signal_reader(signal_file_descriptor)
        except Exception:
            self._close_handles()
            raise

    def clear(self) -> None:
        """Clear the current payload without publishing a wake-up."""
        self._ensure_open()
        lock_file_descriptor = self._lock_file_descriptor
        if lock_file_descriptor is None:
            raise RuntimeError(SHM_ERROR_WRITER_LOCK_NOT_OPEN)

        with _channel_lock(lock_file_descriptor, fcntl.LOCK_EX):
            memory_map_ = self._memory_map
            if memory_map_ is None:
                raise RuntimeError(SHM_ERROR_WRITER_MAP_NOT_OPEN)
            memory_map_[:SHM_HEADER_SIZE] = _pack_header(self._capacity, 0)

    def close(self) -> None:
        """Release this writer's map, FIFO descriptor, and lock file."""
        self._close_handles()


class ShmReader(_ShmEndpoint):
    """Deliver best-effort latest payloads from a worker thread.

    FIFO signals are wake-ups rather than frame ownership transfers. The
    callback receives only the latest payload. It runs in a worker
    thread and must be safe there.
    """

    def __init__(
        self,
        shm_path: str,
        on_payload: Callable[[bytes], None],
        logwarn: Callable[[str], None],
        logerr: Callable[[str], None],
    ) -> None:
        """Initialize the reader endpoint and its callback."""
        super().__init__(
            shm_path,
            logwarn,
            logerr,
            SHM_ERROR_READER_LOCK_NOT_OPEN,
        )
        self._on_payload = on_payload
        self._running = False
        self._thread = self._create_reader_thread()

    def _create_reader_thread(self) -> Thread:
        """Create the worker thread for one start-stop cycle."""
        return Thread(
            target=self._reader_loop,
            daemon=True,
            name="ShmReader",
        )

    def _prime_current_payload(self) -> None:
        """Deliver the newest payload when the reader starts late."""
        # A notification may predate this reader's FIFO opening.
        payload = self._read_payload()
        if payload is None:
            return
        self._dispatch_payload(payload)

    def _drain_signals(self, signal_file_descriptor: int) -> bool:
        """Drain a FIFO batch and report whether any token arrived."""
        signal_received = False
        for _ in range(SHM_SIGNAL_DRAIN_READS):
            try:
                signal_chunk = os.read(
                    signal_file_descriptor,
                    SHM_SIGNAL_DRAIN_READ_SIZE,
                )
            except BlockingIOError:
                return signal_received
            if not signal_chunk:
                return signal_received
            signal_received = True
            if len(signal_chunk) < SHM_SIGNAL_DRAIN_READ_SIZE:
                return signal_received
        return signal_received

    def _read_payload(self) -> bytes | None:
        """Copy one coherent payload snapshot under a shared lock."""
        lock_file_descriptor = self._lock_file_descriptor
        if lock_file_descriptor is None:
            raise RuntimeError(SHM_ERROR_READER_LOCK_NOT_OPEN)

        with _channel_lock(lock_file_descriptor, fcntl.LOCK_SH):
            capacity, payload_length = self._sync_map()
            if payload_length == 0:
                # The channel is empty or no writer copy is committed.
                return None

            memory_map_ = self._memory_map
            if memory_map_ is None:
                raise RuntimeError(SHM_ERROR_READER_NOT_OPEN)

            payload = bytes(
                memory_map_[
                    SHM_HEADER_SIZE : SHM_HEADER_SIZE + payload_length
                ],
            )
            latest_capacity, latest_payload_length = self._sync_map()
            # The lock makes this copy coherent.
            # Keep the header check for non-cooperating writers.
            if (
                latest_capacity != capacity
                or latest_payload_length != payload_length
            ):
                return None
            return payload

    def _wait_for_signal(
        self,
        signal_file_descriptor: int,
    ) -> bool | None:
        """Poll for a signal, returning ``None`` when polling fails."""
        try:
            readable_file_descriptors, _, _ = select.select(
                [signal_file_descriptor],
                [],
                [],
                SHM_SIGNAL_POLL_TIMEOUT_SECONDS,
            )
        except OSError as error:
            if self._running:
                message = f"{SHM_ERROR_SIGNAL_POLL_FAILED}: {error}"
                self._logerr(message)
            return None
        return bool(readable_file_descriptors)

    def _read_signaled_payload(
        self,
        signal_file_descriptor: int,
    ) -> bytes | None:
        """Drain FIFO wake-ups and return the latest payload."""
        try:
            signal_received = self._drain_signals(
                signal_file_descriptor,
            )
        except OSError as error:
            if self._running:
                message = f"{SHM_ERROR_SIGNAL_READ_FAILED}: {error}"
                self._logerr(message)
            self._running = False
            return None

        if not signal_received or not self._running:
            return None

        try:
            return self._read_payload()
        except Exception as error:  # noqa: BLE001
            # A malformed payload must not terminate the reader worker.
            message = f"{SHM_ERROR_READ_PAYLOAD}: {error}"
            self._logerr(message)
            return None

    def _dispatch_payload(self, payload: bytes) -> None:
        """Deliver a payload while keeping the reader worker alive."""
        try:
            self._on_payload(payload)
        except Exception as error:  # noqa: BLE001
            # Callback failures must not stop the reader worker.
            message = f"{SHM_ERROR_HANDLE_PAYLOAD}: {error}"
            self._logerr(message)

    def _reader_loop(self) -> None:
        """Poll FIFO wake-ups and forward valid snapshots."""
        signal_file_descriptor = self._signal_file_descriptor
        if signal_file_descriptor is None:
            raise RuntimeError(SHM_ERROR_READER_NOT_OPEN)

        try:
            while self._running:
                signal_ready = self._wait_for_signal(signal_file_descriptor)
                if signal_ready is None:
                    return
                if not signal_ready:
                    continue

                # FIFO tokens wake this loop.
                # Payload bytes remain in mmap.
                payload = self._read_signaled_payload(
                    signal_file_descriptor,
                )
                if payload is None:
                    continue
                if not self._running:
                    return
                self._dispatch_payload(payload)
        finally:
            self._running = False
            self._close_handles()

    def _remap(self, map_size: int) -> None:
        """Replace the mapping after a writer grows the backing file."""
        old_memory_map = self._memory_map
        self._memory_map = self._open_memory_map(map_size)
        self._map_size = map_size
        if old_memory_map is not None:
            # The old map cannot address bytes past its capacity.
            old_memory_map.close()

    def _sync_map(self) -> tuple[int, int]:
        """Read the header and remap under the caller's channel lock."""
        memory_map_ = self._memory_map
        if memory_map_ is None:
            raise RuntimeError(SHM_ERROR_READER_NOT_OPEN)

        # Copy the live header first to preserve a local snapshot.
        # The snapshot determines whether remapping is required.
        header = bytes(memory_map_[:SHM_HEADER_SIZE])
        capacity, payload_length = _unpack_header(header)
        target_size = SHM_HEADER_SIZE + capacity
        if target_size != self._map_size:
            # The writer may have grown the backing file since mapping.
            self._remap(target_size)
            memory_map_ = self._memory_map
            if memory_map_ is None:
                raise RuntimeError(SHM_ERROR_READER_REMAP_FAILED)
            header = bytes(memory_map_[:SHM_HEADER_SIZE])
            capacity, payload_length = _unpack_header(header)
        return capacity, payload_length

    def _drain_pending_signals(self) -> None:
        """Discard stale FIFO wake-ups before a fresh session."""
        signal_file_descriptor = self._signal_file_descriptor
        if signal_file_descriptor is None:
            raise RuntimeError(SHM_ERROR_READER_NOT_OPEN)
        signals_pending = True
        while signals_pending:
            signals_pending = self._drain_signals(signal_file_descriptor)

    def start(self, *, deliver_current: bool = True) -> None:
        """Open the channel and start the reader.

        By default, stored payload primes the reader before startup.
        Set ``deliver_current`` to ``False`` for a fresh session.
        Fresh sessions ignore prior producer snapshots and signals.
        """
        if self._running:
            return
        if self._thread.is_alive():
            raise RuntimeError(SHM_ERROR_READER_STOPPING)
        self._ensure_open()
        if self._thread.ident is not None:
            # Python threads cannot be started twice after a stop cycle.
            self._thread = self._create_reader_thread()
        self._running = True
        try:
            if deliver_current:
                self._prime_current_payload()
            else:
                self._drain_pending_signals()
            if not self._running:
                return
            self._thread.start()
        except Exception:
            self._running = False
            self._close_handles()
            raise

    def stop(self) -> None:
        """Request worker shutdown and release resources after exit."""
        self._running = False
        if current_thread() is self._thread:
            # The worker closes resources after this callback returns.
            return
        if self._thread.is_alive():
            self._thread.join(timeout=SHM_STOP_JOIN_TIMEOUT_SECONDS)
        if self._thread.is_alive():
            self._logwarn(SHM_WARNING_READER_STILL_STOPPING)
            return
        self._close_handles()
