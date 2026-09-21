"""Regression tests for the latest-value shared-memory transport.

The tests exercise cross-process locking, FIFO wake-up coalescing, mapping
resizes, channel isolation, and reader lifecycle behavior.
"""

from __future__ import annotations

import multiprocessing
import os
import stat
import struct
import tempfile
import threading
import unittest
from contextlib import suppress
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import dtps_http.shm as shm_module
from dtps_http.shm import ShmReader, ShmWriter

fcntl: Any = None
with suppress(ImportError):
    import fcntl

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


def _has_required_capabilities(
    module: object,
    capabilities: tuple[str, ...],
) -> bool:
    """Return whether *module* provides every named capability."""
    for capability in capabilities:
        if getattr(module, capability, None) is None:
            return False
    return True


_SHM_TEST_SUPPORTED = (
    os.name == "posix"
    and fcntl is not None
    and _has_required_capabilities(
        fcntl,
        _REQUIRED_FCNTL_CAPABILITIES,
    )
    and _has_required_capabilities(os, _REQUIRED_OS_CAPABILITIES)
)


def _hold_shared_lock(lock_path, lock_acquired, release_lock) -> None:
    """Hold a process-shared read lock until the parent releases this helper."""
    lock_file_descriptor = os.open(lock_path, os.O_RDWR)
    try:
        fcntl.flock(lock_file_descriptor, fcntl.LOCK_SH)
        lock_acquired.set()
        release_lock.wait()
    finally:
        fcntl.flock(lock_file_descriptor, fcntl.LOCK_UN)
        os.close(lock_file_descriptor)


@unittest.skipUnless(
    _SHM_TEST_SUPPORTED,
    "Shared-memory transport tests require POSIX capabilities.",
)
class SharedMemoryTransportTests(unittest.TestCase):
    """Exercise the public safety and delivery contract of shared memory."""

    def test_close_handles_clears_state_after_close_failures(self):
        """Detach every handle when its close operation fails."""
        errors: list[str] = []
        failing_map = Mock()
        failing_map.close.side_effect = BufferError("exported buffer")
        endpoint = object.__new__(shm_module._ShmEndpoint)
        endpoint._memory_map = failing_map
        endpoint._signal_file_descriptor = 10
        endpoint._lock_file_descriptor = 11
        endpoint._logerr = errors.append

        with patch.object(
            shm_module.os,
            "close",
            side_effect=[
                OSError("signal close failed"),
                OSError("lock close failed"),
            ],
        ):
            endpoint._close_handles()

        self.assertIsNone(endpoint._memory_map)
        self.assertIsNone(endpoint._signal_file_descriptor)
        self.assertIsNone(endpoint._lock_file_descriptor)
        self.assertEqual(
            errors,
            [
                f"{shm_module.SHM_ERROR_CLOSE_MAP}: exported buffer",
                f"{shm_module.SHM_ERROR_CLOSE_SIGNAL_FIFO}: "
                "signal close failed",
                f"{shm_module.SHM_ERROR_CLOSE_LOCK_FILE}: "
                "lock close failed",
            ],
        )

    def test_writer_rejects_missing_fcntl_capability(self):
        """Fail clearly before creating files when advisory locks are absent."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "missing-fcntl"
            with patch.object(shm_module, "fcntl", None), self.assertRaisesRegex(OSError, "fcntl"):
                ShmWriter(
                    channel_path,
                    warnings.append,
                    errors.append,
                )
            channel_exists = channel_path.exists()

        self.assertFalse(channel_exists)
        self.assertFalse(warnings)
        self.assertFalse(errors)

    def test_writer_rejects_missing_no_follow_capability(self):
        """Fail clearly before creating files when no-follow opens are absent."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "missing-no-follow"
            with patch.object(shm_module.os, "O_NOFOLLOW", None), self.assertRaisesRegex(OSError, "os.O_NOFOLLOW"):
                ShmWriter(
                    channel_path,
                    warnings.append,
                    errors.append,
                )
            channel_exists = channel_path.exists()

        self.assertFalse(channel_exists)
        self.assertFalse(warnings)
        self.assertFalse(errors)

    def test_writer_rejects_missing_close_on_exec_capability(self):
        """Fail clearly before creating files without close-on-exec opens."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "missing-close-on-exec"
            with patch.object(
                shm_module.os,
                "O_CLOEXEC",
                None,
            ), self.assertRaisesRegex(OSError, "os.O_CLOEXEC"):
                ShmWriter(
                    channel_path,
                    warnings.append,
                    errors.append,
                )
            channel_exists = channel_path.exists()

        self.assertFalse(channel_exists)
        self.assertFalse(warnings)
        self.assertFalse(errors)

    def test_writer_opens_channel_artifacts_close_on_exec(self):
        """Set close-on-exec on every channel artifact descriptor."""
        warnings: list[str] = []
        errors: list[str] = []
        original_open = os.open

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "close-on-exec"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                with patch.object(
                    shm_module.os,
                    "open",
                    wraps=original_open,
                ) as open_method:
                    writer.publish(b"payload")
                open_calls = open_method.call_args_list
            finally:
                writer.close()

        open_flags = [open_call.args[1] for open_call in open_calls]
        self.assertTrue(open_flags)
        for open_flags_value in open_flags:
            close_on_exec = open_flags_value & os.O_CLOEXEC
            self.assertEqual(close_on_exec, os.O_CLOEXEC)
        self.assertFalse(warnings)
        self.assertFalse(errors)

    def test_new_channel_files_are_owner_only(self):
        """Create data, lock, and signal entries with owner-only access."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "private-channel"
            lock_path = channel_path.parent / f"{channel_path.name}.lock"
            signal_path = channel_path.parent / f"{channel_path.name}.p2c"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                writer.publish(b"payload")
                for path in (
                    channel_path,
                    lock_path,
                    signal_path,
                ):
                    path_status = path.stat()
                    mode = stat.S_IMODE(path_status.st_mode)
                    self.assertEqual(mode, 0o600)
            finally:
                writer.close()

        self.assertFalse(errors)

    def test_writer_resets_oversized_corrupt_channel(self):
        """Discard an untrusted backing-file size with an invalid header."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "oversized-corrupt-channel"
            oversized_capacity = shm_module.SHM_DEFAULT_CAPACITY * 8
            oversized_file_size = (
                shm_module.SHM_HEADER_SIZE + oversized_capacity
            )
            channel_path.write_bytes(b"invalid header")
            os.truncate(channel_path, oversized_file_size)
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                writer.publish(b"payload")
                channel_size = channel_path.stat().st_size
            finally:
                writer.close()

        expected_size = (
            shm_module.SHM_HEADER_SIZE + shm_module.SHM_DEFAULT_CAPACITY
        )
        self.assertEqual(channel_size, expected_size)
        self.assertTrue(warnings)
        self.assertFalse(errors)

    def test_writer_resets_channel_with_excessive_capacity(self):
        """Discard a valid header that exceeds the capacity ceiling."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "excessive-capacity-channel"
            excessive_capacity = shm_module.SHM_MAX_CAPACITY + 1
            header = struct.pack(
                shm_module.SHM_HEADER_FORMAT,
                shm_module.SHM_MAGIC,
                shm_module.SHM_VERSION,
                excessive_capacity,
                0,
            )
            channel_path.write_bytes(header)
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                writer.publish(b"payload")
                channel_size = channel_path.stat().st_size
            finally:
                writer.close()

        expected_size = (
            shm_module.SHM_HEADER_SIZE + shm_module.SHM_DEFAULT_CAPACITY
        )
        self.assertEqual(channel_size, expected_size)
        self.assertTrue(warnings)
        self.assertFalse(errors)

    def test_header_rejects_negative_capacity(self):
        """Reject a negative capacity before writing a channel header."""
        with self.assertRaises(ValueError):
            shm_module._pack_header(-1, 0)

    def test_writer_rejects_payload_exceeding_capacity_ceiling(self):
        """Reject a write that would grow the channel beyond its ceiling."""
        warnings: list[str] = []
        errors: list[str] = []

        with patch.multiple(
            shm_module,
            SHM_DEFAULT_CAPACITY=8,
            SHM_MAX_CAPACITY=16,
        ), tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "oversized-payload"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                with self.assertRaisesRegex(
                    ValueError,
                    "exceeds configured maximum",
                ):
                    writer.publish(b"x" * 17)
                channel_size = channel_path.stat().st_size
            finally:
                writer.close()

        expected_size = shm_module.SHM_HEADER_SIZE + 8
        self.assertEqual(channel_size, expected_size)
        self.assertFalse(warnings)
        self.assertFalse(errors)

    def test_writer_recovers_without_leaking_handles_after_write_failure(
        self,
    ):
        """Close partial state before reopening after a mapped write fails."""
        warnings: list[str] = []
        errors: list[str] = []
        opened_signal_file_descriptors: list[int] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "failed-write"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )

            def fail_after_map_close(
                _payload: bytes,
                _payload_length: int,
            ) -> None:
                signal_file_descriptor = writer._signal_file_descriptor
                assert signal_file_descriptor is not None
                opened_signal_file_descriptors.append(signal_file_descriptor)
                memory_map = writer._memory_map
                assert memory_map is not None
                memory_map.close()
                writer._memory_map = None
                raise OSError("simulated mapped write failure")

            try:
                with patch.object(
                    writer,
                    "_write_payload_locked",
                    side_effect=fail_after_map_close,
                ), self.assertRaisesRegex(OSError, "simulated mapped write"):
                    writer.publish(b"payload")

                self.assertIsNone(writer._memory_map)
                self.assertIsNone(writer._signal_file_descriptor)
                self.assertIsNone(writer._lock_file_descriptor)
                with self.assertRaises(OSError):
                    os.fstat(opened_signal_file_descriptors[0])

                writer.publish(b"recovered")
            finally:
                writer.close()

        self.assertFalse(warnings)
        self.assertFalse(errors)

    def test_new_channel_files_ignore_restrictive_umask(self):
        """Correct new channel directory and entry modes after a strict umask."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_directory = Path(directory) / "umask-directory"
            channel_path = channel_directory / "umask-channel"
            lock_path = channel_path.parent / f"{channel_path.name}.lock"
            signal_path = channel_path.parent / f"{channel_path.name}.p2c"
            previous_umask = os.umask(0o777)
            writer: ShmWriter | None = None
            try:
                writer = ShmWriter(
                    channel_path,
                    warnings.append,
                    errors.append,
                )
                writer.publish(b"payload")
            finally:
                os.umask(previous_umask)
                if writer is not None:
                    writer.close()

            for path in (
                channel_path,
                lock_path,
                signal_path,
            ):
                path_status = path.stat()
                mode = stat.S_IMODE(path_status.st_mode)
                self.assertEqual(mode, 0o600)
            directory_status = channel_directory.stat()
            directory_mode = stat.S_IMODE(directory_status.st_mode)
            self.assertEqual(directory_mode, 0o700)

        self.assertFalse(errors)

    def test_nested_channel_directories_ignore_restrictive_umask(self):
        """Repair each new parent before creating the next channel directory."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            first_directory = Path(directory) / "first-directory"
            second_directory = first_directory / "second-directory"
            channel_path = second_directory / "channel"
            previous_umask = os.umask(0o777)
            writer: ShmWriter | None = None
            try:
                writer = ShmWriter(
                    channel_path,
                    warnings.append,
                    errors.append,
                )
                writer.publish(b"payload")
            finally:
                os.umask(previous_umask)
                if writer is not None:
                    writer.close()

            for channel_directory in (first_directory, second_directory):
                directory_status = channel_directory.stat()
                directory_mode = stat.S_IMODE(directory_status.st_mode)
                self.assertEqual(directory_mode, 0o700)

        self.assertFalse(errors)

    def test_writer_rejects_world_writable_channel_directory(self):
        """Reject channel artifacts located directly in a public directory."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_directory = Path(directory) / "public-directory"
            channel_directory.mkdir(mode=0o700)
            channel_directory.chmod(0o707)
            channel_path = channel_directory / "channel"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                with self.assertRaisesRegex(
                    ValueError, "writable by other users"
                ):
                    writer.publish(b"payload")
                self.assertFalse(channel_path.exists())
                self.assertIsNone(writer._lock_file_descriptor)
            finally:
                writer.close()

        self.assertFalse(errors)

    def test_writer_rejects_group_writable_channel_directory(self):
        """Reject channel artifacts located directly in a shared directory."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_directory = Path(directory) / "shared-directory"
            channel_directory.mkdir(mode=0o700)
            channel_directory.chmod(0o770)
            channel_path = channel_directory / "channel"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                with self.assertRaisesRegex(
                    ValueError, "writable by other users"
                ):
                    writer.publish(b"payload")
                self.assertFalse(channel_path.exists())
                self.assertIsNone(writer._lock_file_descriptor)
            finally:
                writer.close()

        self.assertFalse(errors)

    def test_writer_rejects_writable_channel_directory_ancestor(self):
        """Reject a channel below an ancestor an untrusted user can replace."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            shared_parent = Path(directory) / "shared-parent"
            shared_parent.mkdir(mode=0o700)
            shared_parent.chmod(0o777)
            channel_directory = shared_parent / "private-directory"
            channel_path = channel_directory / "channel"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                with self.assertRaisesRegex(
                    ValueError, "writable by other users"
                ):
                    writer.publish(b"payload")
                self.assertFalse(channel_directory.exists())
                self.assertIsNone(writer._lock_file_descriptor)
            finally:
                writer.close()

        self.assertFalse(errors)

    def test_writer_allows_sticky_channel_directory_ancestor(self):
        """Allow a private channel nested below a sticky shared parent."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            sticky_parent = Path(directory) / "sticky-parent"
            sticky_parent.mkdir(mode=0o700)
            sticky_parent.chmod(0o1777)
            channel_path = sticky_parent / "private-directory" / "channel"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                writer.publish(b"payload")
                self.assertTrue(channel_path.exists())
            finally:
                writer.close()

        self.assertFalse(errors)

    def test_writer_rejects_symlinked_channel_directory(self):
        """Reject a channel directory that resolves through a symlink."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            target_directory = Path(directory) / "target-directory"
            target_directory.mkdir(mode=0o700)
            channel_directory = Path(directory) / "symlinked-directory"
            channel_directory.symlink_to(
                target_directory, target_is_directory=True
            )
            channel_path = channel_directory / "channel"
            target_channel_path = target_directory / "channel"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                with self.assertRaisesRegex(ValueError, "is a symlink"):
                    writer.publish(b"payload")
                self.assertFalse(target_channel_path.exists())
                self.assertIsNone(writer._lock_file_descriptor)
            finally:
                writer.close()

        self.assertFalse(errors)

    def test_writer_rejects_channel_directory_with_symlinked_parent(self):
        """Reject a new channel directory below a symlinked parent."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            target_directory = Path(directory) / "target-directory"
            target_directory.mkdir(mode=0o700)
            symlinked_parent = Path(directory) / "symlinked-parent"
            symlinked_parent.symlink_to(
                target_directory, target_is_directory=True
            )
            channel_directory = symlinked_parent / "new-directory"
            channel_path = channel_directory / "channel"
            target_channel_directory = target_directory / "new-directory"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                with self.assertRaisesRegex(ValueError, "is a symlink"):
                    writer.publish(b"payload")
                self.assertFalse(target_channel_directory.exists())
                self.assertIsNone(writer._lock_file_descriptor)
            finally:
                writer.close()

        self.assertFalse(errors)

    def test_channel_file_fallback_does_not_create_after_race(self):
        """Do not recreate an artifact that vanishes after an EEXIST race."""
        open_flags = os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW
        open_calls: list[tuple[int, int | None]] = []

        def open_file(
            _path: str,
            flags: int,
            mode: int | None = None,
        ) -> int:
            open_calls.append((flags, mode))
            if len(open_calls) == 1:
                raise FileExistsError
            raise FileNotFoundError

        with patch.object(
            shm_module.os,
            "open",
            side_effect=open_file,
        ), self.assertRaises(FileNotFoundError):
            shm_module._open_or_create_channel_file(
                "channel",
                open_flags,
                0o600,
            )

        fallback_flags, fallback_mode = open_calls[1]
        self.assertFalse(fallback_flags & os.O_CREAT)
        self.assertFalse(fallback_flags & os.O_EXCL)
        self.assertIsNone(fallback_mode)

    def test_independent_channels_deliver_only_their_payloads(self):
        """Keep payloads isolated by their independent channel paths."""
        warnings: list[str] = []
        errors: list[str] = []
        payloads = {
            "alpha": b"alpha-payload",
            "beta": b"beta-payload",
            "gamma": b"gamma-payload",
        }
        received = {channel_name: [] for channel_name in payloads}
        writers = []
        readers = []

        with tempfile.TemporaryDirectory() as directory:
            for channel_name, payload in payloads.items():
                channel_path = Path(directory) / channel_name
                writer = ShmWriter(
                    channel_path,
                    warnings.append,
                    errors.append,
                )
                writer.publish(payload)
                reader = ShmReader(
                    channel_path,
                    received[channel_name].append,
                    warnings.append,
                    errors.append,
                )
                writers.append(writer)
                readers.append(reader)

            try:
                for reader in readers:
                    reader.start()
            finally:
                for reader in readers:
                    reader.stop()
                for writer in writers:
                    writer.close()

        for channel_name, payload in payloads.items():
            channel_payloads = received[channel_name]
            self.assertTrue(channel_payloads)
            all_payloads_match = all(
                received_payload == payload
                for received_payload in channel_payloads
            )
            self.assertTrue(all_payloads_match)
        self.assertFalse(errors)

    def test_reader_receives_resized_payload(self):
        """Deliver a complete payload after the writer grows its backing map."""
        warnings: list[str] = []
        errors: list[str] = []
        received: list[bytes] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "resized-payload"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            reader = ShmReader(
                channel_path,
                received.append,
                warnings.append,
                errors.append,
            )
            payload = b"x" * ((1024 * 1024) + 1)
            try:
                writer.publish(payload)
                reader.start()
            finally:
                reader.stop()
                writer.close()

        self.assertTrue(received)
        all_payloads_match = all(
            received_payload == payload for received_payload in received
        )
        self.assertTrue(all_payloads_match)
        self.assertFalse(errors)

    def test_reader_retries_remap_after_transient_map_failure(self):
        """Keep the prior map usable when one resize remap cannot open."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "retry-remap"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            reader = ShmReader(
                channel_path,
                lambda _payload: None,
                warnings.append,
                errors.append,
            )
            payload = b"x" * (shm_module.SHM_DEFAULT_CAPACITY + 1)
            original_open_memory_map = reader._open_memory_map
            map_attempts = 0

            def fail_once(map_size: int):
                nonlocal map_attempts
                map_attempts += 1
                if map_attempts == 1:
                    raise OSError("transient mapping failure")
                return original_open_memory_map(map_size)

            try:
                writer.publish(b"initial")
                reader._ensure_open()
                writer.publish(payload)
                with patch.object(
                    reader,
                    "_open_memory_map",
                    side_effect=fail_once,
                ):
                    with self.assertRaisesRegex(
                        OSError,
                        "transient mapping failure",
                    ):
                        reader._read_payload()
                    self.assertEqual(reader._read_payload(), payload)
            finally:
                reader.stop()
                writer.close()

        self.assertEqual(map_attempts, 2)
        self.assertFalse(errors)

    def test_writer_waits_for_shared_channel_lock(self):
        """Block an exclusive writer update until another process releases LOCK_SH."""
        warnings: list[str] = []
        errors: list[str] = []
        publish_started = threading.Event()
        publish_completed = threading.Event()
        publish_errors: list[Exception] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "locked-channel"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            writer.publish(b"before")

            lock_path = str(channel_path) + ".lock"
            # Use a separate process so this verifies the kernel-level flock
            # contract rather than thread-local behavior in one interpreter.
            lock_acquired = multiprocessing.Event()
            release_lock = multiprocessing.Event()
            lock_process = multiprocessing.Process(
                target=_hold_shared_lock,
                args=(lock_path, lock_acquired, release_lock),
            )
            lock_process.start()

            def publish() -> None:
                publish_started.set()
                try:
                    writer.publish(b"after")
                except Exception as error:  # pragma: no cover - asserted below
                    publish_errors.append(error)
                finally:
                    publish_completed.set()

            publish_thread = threading.Thread(target=publish)
            try:
                self.assertTrue(lock_acquired.wait(1))
                publish_thread.start()
                self.assertTrue(publish_started.wait(1))
                self.assertFalse(publish_completed.wait(0.1))

                release_lock.set()
                self.assertTrue(publish_completed.wait(1))
                publish_thread.join(timeout=1)
            finally:
                # Always unblock and reap the helper, even after an assertion.
                release_lock.set()
                lock_process.join(timeout=1)
                if lock_process.is_alive():
                    lock_process.terminate()
                    lock_process.join(timeout=1)
                writer.close()

        self.assertFalse(publish_thread.is_alive())
        self.assertEqual(lock_process.exitcode, 0)
        self.assertFalse(publish_errors)
        self.assertFalse(errors)

    def test_writer_rejects_non_fifo_signal_path(self):
        """Reject a regular file at the signal path and release setup resources."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "invalid-signal"
            signal_name = f"{channel_path.name}.p2c"
            signal_path = channel_path.parent / signal_name
            signal_path.touch()
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                with self.assertRaisesRegex(ValueError, "not a FIFO"):
                    writer.publish(b"payload")
                self.assertIsNone(writer._lock_file_descriptor)
                self.assertIsNone(writer._memory_map)
                self.assertIsNone(writer._signal_file_descriptor)
            finally:
                writer.close()

        self.assertFalse(errors)

    def test_writer_rejects_symlinked_signal_path(self):
        """Reject a symlink even when it resolves to a valid FIFO."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "symlinked-signal"
            signal_name = f"{channel_path.name}.p2c"
            signal_path = channel_path.parent / signal_name
            target_path = Path(directory) / "signal-target"
            os.mkfifo(target_path, 0o600)
            signal_path.symlink_to(target_path)
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                with self.assertRaisesRegex(ValueError, "not a FIFO"):
                    writer.publish(b"payload")
                self.assertIsNone(writer._lock_file_descriptor)
                self.assertIsNone(writer._memory_map)
                self.assertIsNone(writer._signal_file_descriptor)
            finally:
                writer.close()

        self.assertFalse(errors)

    def test_writer_rejects_existing_public_signal_fifo(self):
        """Reject a pre-existing FIFO that other users can access."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "public-signal"
            signal_name = f"{channel_path.name}.p2c"
            signal_path = channel_path.parent / signal_name
            os.mkfifo(signal_path, 0o600)
            signal_path.chmod(0o606)
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                with self.assertRaisesRegex(
                    ValueError, "accessible by other users"
                ):
                    writer.publish(b"payload")
                signal_status = signal_path.stat()
                signal_mode = stat.S_IMODE(signal_status.st_mode)
                self.assertEqual(signal_mode, 0o606)
                self.assertIsNone(writer._lock_file_descriptor)
                self.assertIsNone(writer._memory_map)
                self.assertIsNone(writer._signal_file_descriptor)
            finally:
                writer.close()

        self.assertFalse(errors)

    def test_writer_rejects_symlinked_backing_and_lock_paths(self):
        """Do not follow symlinks for regular channel artifacts."""
        warnings: list[str] = []
        errors: list[str] = []

        for suffix in ("", ".lock"):
            with self.subTest(suffix=suffix), tempfile.TemporaryDirectory() as directory:
                channel_path = Path(directory) / "symlinked-channel"
                protected_path = Path(directory) / "protected"
                protected_payload = b"do not overwrite"
                protected_path.write_bytes(protected_payload)
                artifact_name = f"{channel_path.name}{suffix}"
                artifact_path = channel_path.parent / artifact_name
                artifact_path.symlink_to(protected_path)
                writer = ShmWriter(
                    channel_path,
                    warnings.append,
                    errors.append,
                )
                try:
                    with self.assertRaisesRegex(
                        ValueError, "regular file"
                    ):
                        writer.publish(b"payload")
                    self.assertEqual(
                        protected_path.read_bytes(),
                        protected_payload,
                    )
                    self.assertIsNone(writer._lock_file_descriptor)
                    self.assertIsNone(writer._memory_map)
                    self.assertIsNone(writer._signal_file_descriptor)
                finally:
                    writer.close()

        self.assertFalse(errors)

    def test_writer_rejects_invalid_backing_before_creating_signal(self):
        """Avoid a partial FIFO when the backing channel path is invalid."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "invalid-backing"
            protected_path = Path(directory) / "protected"
            protected_path.write_bytes(b"do not overwrite")
            channel_path.symlink_to(protected_path)
            signal_name = f"{channel_path.name}.p2c"
            signal_path = channel_path.parent / signal_name
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                with self.assertRaisesRegex(ValueError, "regular file"):
                    writer.publish(b"payload")
                signal_exists = signal_path.exists()
                self.assertFalse(signal_exists)
            finally:
                writer.close()

        self.assertFalse(errors)

    def test_writer_removes_new_signal_after_map_failure(self):
        """Remove a FIFO created during an unsuccessful channel open."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "map-failure"
            signal_name = f"{channel_path.name}.p2c"
            signal_path = channel_path.parent / signal_name
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                with patch.object(writer, "_open_memory_map", side_effect=OSError("mapping failed")), self.assertRaisesRegex(OSError, "mapping failed"):
                    writer.publish(b"payload")
                self.assertFalse(signal_path.exists())
                self.assertIsNone(writer._lock_file_descriptor)
                self.assertIsNone(writer._memory_map)
                self.assertIsNone(writer._signal_file_descriptor)
            finally:
                writer.close()

        self.assertFalse(errors)

    def test_ensure_signal_fifo_corrects_restrictive_umask(self):
        """Repair a new FIFO's mode before opening its descriptor."""
        with tempfile.TemporaryDirectory() as directory:
            signal_path = Path(directory) / "signal.p2c"
            signal_file_descriptor: int | None = None
            signal_mode: int | None = None
            previous_umask = os.umask(0o777)
            try:
                signal_created, _signal_identity = (
                    shm_module._ensure_signal_fifo(str(signal_path))
                )
            finally:
                os.umask(previous_umask)
            try:
                self.assertTrue(signal_created)
                signal_file_descriptor = shm_module._open_signal_fifo(
                    str(signal_path),
                    created=signal_created,
                )
                signal_mode = stat.S_IMODE(
                    os.fstat(signal_file_descriptor).st_mode
                )
            finally:
                if signal_file_descriptor is not None:
                    os.close(signal_file_descriptor)
                if signal_path.exists():
                    signal_path.unlink()

        self.assertEqual(signal_mode, 0o600)

    def test_ensure_signal_fifo_removes_path_after_validation_failure(self):
        """Remove a FIFO created before its setup validation fails."""
        with tempfile.TemporaryDirectory() as directory:
            signal_path = Path(directory) / "signal.p2c"
            with patch.object(
                shm_module,
                "_validate_signal_fifo_status",
                side_effect=ValueError("validation failed"),
            ), self.assertRaisesRegex(ValueError, "validation failed"):
                shm_module._ensure_signal_fifo(str(signal_path))
            self.assertFalse(signal_path.exists())

    def test_writer_removes_new_signal_after_fifo_setup_failure(self):
        """Remove a FIFO when its initial permission setup fails."""
        warnings: list[str] = []
        errors: list[str] = []
        original_fchmod = os.fchmod

        def fail_signal_fchmod(file_descriptor: int, mode: int) -> None:
            file_status = os.fstat(file_descriptor)
            if stat.S_ISFIFO(file_status.st_mode):
                raise OSError("chmod failed")
            original_fchmod(file_descriptor, mode)

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "fifo-setup-failure"
            signal_name = f"{channel_path.name}.p2c"
            signal_path = channel_path.parent / signal_name
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                with patch.object(
                    shm_module.os,
                    "fchmod",
                    side_effect=fail_signal_fchmod,
                ), self.assertRaisesRegex(OSError, "chmod failed"):
                    writer.publish(b"payload")
                self.assertFalse(signal_path.exists())
                self.assertIsNone(writer._lock_file_descriptor)
                self.assertIsNone(writer._memory_map)
                self.assertIsNone(writer._signal_file_descriptor)
            finally:
                writer.close()

        self.assertFalse(errors)

    def test_writer_preserves_replaced_signal_after_fifo_setup_failure(self):
        """Do not remove a path substituted during FIFO setup cleanup."""
        warnings: list[str] = []
        errors: list[str] = []
        replacement_payload = b"replacement"
        original_fchmod = os.fchmod

        def replace_signal_then_fail(
            signal_file_descriptor: int,
            mode: int,
        ) -> None:
            signal_status = os.fstat(signal_file_descriptor)
            if not stat.S_ISFIFO(signal_status.st_mode):
                original_fchmod(signal_file_descriptor, mode)
                return
            signal_path.unlink()
            signal_path.write_bytes(replacement_payload)
            raise OSError("chmod failed")

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "fifo-setup-replacement"
            signal_name = f"{channel_path.name}.p2c"
            signal_path = channel_path.parent / signal_name
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                with patch.object(
                    shm_module.os,
                    "fchmod",
                    side_effect=replace_signal_then_fail,
                ), self.assertRaisesRegex(OSError, "chmod failed"):
                    writer.publish(b"payload")
                self.assertEqual(signal_path.read_bytes(), replacement_payload)
                self.assertIsNone(writer._lock_file_descriptor)
                self.assertIsNone(writer._memory_map)
                self.assertIsNone(writer._signal_file_descriptor)
            finally:
                writer.close()

        self.assertFalse(errors)

    def test_writer_rejects_hardlinked_backing_path(self):
        """Do not truncate an existing file linked into a channel path."""
        warnings: list[str] = []
        errors: list[str] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "hardlinked-channel"
            protected_path = Path(directory) / "protected"
            protected_payload = b"do not overwrite"
            protected_path.write_bytes(protected_payload)
            os.link(protected_path, channel_path)
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            try:
                with self.assertRaisesRegex(ValueError, "multiple hard links"):
                    writer.publish(b"payload")
                self.assertEqual(
                    protected_path.read_bytes(), protected_payload
                )
                self.assertIsNone(writer._lock_file_descriptor)
                self.assertIsNone(writer._memory_map)
                self.assertIsNone(writer._signal_file_descriptor)
            finally:
                writer.close()

        self.assertFalse(errors)

    def test_writer_coalesces_notifications_when_fifo_is_full(self):
        """Keep publishing when the FIFO is full and warn only once per episode."""
        warnings: list[str] = []
        errors: list[str] = []
        publish_completed = threading.Event()
        publish_errors: list[Exception] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "full-fifo"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            writer.publish(b"initial")

            signal_path = str(channel_path) + ".p2c"
            fill_flags = os.O_RDWR | os.O_NONBLOCK
            fill_file_descriptor = os.open(signal_path, fill_flags)
            fill_chunk = b"x" * 8192
            fifo_is_full = False
            try:
                # No reader drains this descriptor, so non-blocking writes
                # eventually prove that the FIFO has reached backpressure.
                for _ in range(1024):
                    try:
                        os.write(fill_file_descriptor, fill_chunk)
                    except BlockingIOError:
                        fifo_is_full = True
                        break
                self.assertTrue(fifo_is_full)

                def publish() -> None:
                    try:
                        writer.publish(b"latest")
                    except (
                        Exception
                    ) as error:  # pragma: no cover - asserted below
                        publish_errors.append(error)
                    finally:
                        publish_completed.set()

                publish_thread = threading.Thread(target=publish, daemon=True)
                publish_thread.start()
                self.assertTrue(publish_completed.wait(1))
                publish_thread.join(timeout=1)
                self.assertFalse(publish_thread.is_alive())
                # A second full-FIFO publish must not duplicate the warning.
                writer.publish(b"newest")
            finally:
                os.close(fill_file_descriptor)
                writer.close()

        self.assertFalse(publish_errors)
        backpressure_warnings = [
            warning for warning in warnings if "signal FIFO is full" in warning
        ]
        self.assertEqual(len(backpressure_warnings), 1)
        self.assertFalse(errors)

    def test_reader_coalesces_queued_wakeups(self):
        """Turn a queued FIFO batch into one latest-payload callback."""
        warnings: list[str] = []
        errors: list[str] = []
        received: list[bytes] = []
        received_lock = threading.Lock()
        queued_delivery_received = threading.Event()

        def on_payload(payload: bytes) -> None:
            """Record startup priming and the following coalesced delivery."""
            with received_lock:
                received.append(payload)
                received_count = len(received)
            if received_count >= 2:
                queued_delivery_received.set()

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "queued-wakeups"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            payload = b"latest"
            # Each token is eight bytes. 512 tokens make one exact 4096-byte
            # reader drain, covering the boundary between drain iterations.
            for _ in range(512):
                writer.publish(payload)

            reader = ShmReader(
                channel_path,
                on_payload,
                warnings.append,
                errors.append,
            )
            try:
                reader.start()
                self.assertTrue(queued_delivery_received.wait(1))
            finally:
                reader.stop()
                writer.close()

        with received_lock:
            self.assertEqual(len(received), 2)
        all_payloads_match = all(
            received_payload == payload for received_payload in received
        )
        # The first callback primes the existing payload; the second represents
        # the complete queued wake-up batch.
        self.assertTrue(all_payloads_match)
        self.assertFalse(errors)

    def test_reader_can_restart_after_stop(self):
        """Create a fresh worker thread and deliver payloads after a restart."""
        warnings: list[str] = []
        errors: list[str] = []
        received: list[bytes] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "restartable-reader"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            reader = ShmReader(
                channel_path,
                received.append,
                warnings.append,
                errors.append,
            )
            try:
                first_payload = b"first"
                writer.publish(first_payload)
                reader.start()
                reader.stop()

                second_payload = b"second"
                writer.publish(second_payload)
                reader.start()
                reader.stop()
            finally:
                reader.stop()
                writer.close()

        self.assertIn(first_payload, received)
        self.assertIn(second_payload, received)
        self.assertFalse(errors)

    def test_reader_continues_after_startup_callback_error(self):
        """Log a retained-payload callback failure without aborting startup."""
        warnings: list[str] = []
        errors: list[str] = []
        received: list[bytes] = []
        received_event = threading.Event()
        retained_payload = b"retained"
        next_payload = b"next"

        def on_payload(payload: bytes) -> None:
            if payload == retained_payload:
                raise RuntimeError("retained callback failure")
            received.append(payload)
            received_event.set()

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "startup-callback-error"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            reader = ShmReader(
                channel_path,
                on_payload,
                warnings.append,
                errors.append,
            )
            try:
                writer.publish(retained_payload)
                reader.start()
                writer.publish(next_payload)
                next_payload_received = received_event.wait(1)
                self.assertTrue(next_payload_received)
            finally:
                reader.stop()
                writer.close()

        self.assertIn(next_payload, received)
        callback_errors = [
            error
            for error in errors
            if "Failed to handle shared-memory payload" in error
        ]
        self.assertTrue(callback_errors)

    def test_fresh_reader_skips_previous_session_payload(self):
        """Discard retained payloads and wake-ups before a fresh session."""
        warnings: list[str] = []
        errors: list[str] = []
        received: list[bytes] = []
        received_event = threading.Event()

        def on_payload(payload: bytes) -> None:
            received.append(payload)
            received_event.set()

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "fresh-reader"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            reader = ShmReader(
                channel_path,
                on_payload,
                warnings.append,
                errors.append,
            )
            try:
                writer.publish(b"previous-session")
                with patch.multiple(
                    "dtps_http.shm",
                    SHM_SIGNAL_DRAIN_READ_SIZE=1,
                    SHM_SIGNAL_DRAIN_READS=1,
                ):
                    reader.start(deliver_current=False)
                self.assertFalse(received_event.wait(0.1))

                writer.publish(b"current-session")
                self.assertTrue(received_event.wait(1))
            finally:
                reader.stop()
                writer.close()

        self.assertEqual(received, [b"current-session"])
        self.assertFalse(errors)

    def test_writer_can_clear_a_retained_payload(self):
        """A new producer can prevent its stale payload from being primed."""
        warnings: list[str] = []
        errors: list[str] = []
        received: list[bytes] = []

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "clear-retained"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            reader = ShmReader(
                channel_path,
                received.append,
                warnings.append,
                errors.append,
            )
            try:
                writer.publish(b"previous-session")
                writer.clear()
                reader.start()
            finally:
                reader.stop()
                writer.close()

        self.assertEqual(received, [])
        self.assertFalse(errors)

    def test_stop_waits_for_in_flight_callback_before_closing_handles(self):
        """Defer cleanup until a callback that outlives stop() has returned."""
        warnings: list[str] = []
        errors: list[str] = []
        callback_started = threading.Event()
        release_callback = threading.Event()
        received: list[bytes] = []

        def on_payload(payload: bytes) -> None:
            """Keep the worker active while the test requests shutdown."""
            callback_started.set()
            release_callback.wait()
            received.append(payload)

        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "in-flight-callback"
            writer = ShmWriter(
                channel_path,
                warnings.append,
                errors.append,
            )
            reader = ShmReader(
                channel_path,
                on_payload,
                warnings.append,
                errors.append,
            )
            payload = b"payload"
            try:
                reader.start()
                writer.publish(payload)
                self.assertTrue(callback_started.wait(1))

                # stop() must not close resources that its worker still needs.
                reader.stop()
                self.assertTrue(reader._thread.is_alive())
                self.assertIsNotNone(reader._memory_map)
                self.assertIsNotNone(reader._lock_file_descriptor)
                with self.assertRaisesRegex(RuntimeError, "still stopping"):
                    reader.start()

                # Cleanup is owned by the worker after the callback unblocks.
                release_callback.set()
                reader._thread.join(timeout=1)
                self.assertFalse(reader._thread.is_alive())
                self.assertIsNone(reader._memory_map)
                self.assertIsNone(reader._lock_file_descriptor)
            finally:
                release_callback.set()
                reader.stop()
                writer.close()

        self.assertIn(payload, received)
        self.assertFalse(errors)


if __name__ == "__main__":
    unittest.main()
