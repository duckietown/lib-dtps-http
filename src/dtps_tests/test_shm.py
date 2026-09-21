"""Integration tests for DTPS shared-memory publish and subscribe options."""

from __future__ import annotations

import asyncio
import os
import tempfile
import threading
from contextlib import suppress
from pathlib import Path
from typing import Any
from unittest import IsolatedAsyncioTestCase, skipUnless
from unittest.mock import AsyncMock, Mock, patch

from dtps import ContextConfig
from dtps.ergo_create import (
    ContextManagerCreate,
    ContextManagerCreateContext,
    ContextManagerCreateContextSubscriber,
)
from dtps.ergo_use import (
    ContextManagerUse,
    ContextManagerUseContext,
    ContextManagerUseContextPublisher,
    ContextManagerUseSubscription,
    FakeSubscriptionInterface,
)
from dtps.shm import (
    ShmWriterPool,
    _ShmSubscription,
    _ShmWriterEntry,
    create_shm_subscription,
    should_publish_http,
)
from dtps_http import MIME_TEXT, RawData, SUB_ID
from dtps_http_tests.utils import test_timeout

from .utils import create_use, create_use_pair

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
_THREAD_TIMEOUT_SECONDS = 5


class _BlockingWriter:
    """Test double that records whether shutdown overlaps publication."""

    def __init__(self) -> None:
        """Initialize synchronization state for one artificial publication."""
        self.close_called = threading.Event()
        self.publish_started = threading.Event()
        self.release_publish = threading.Event()
        self._publishing = False
        self._state_lock = threading.Lock()
        self.close_called_during_publish = False

    def publish(self, _payload: bytes) -> None:
        """Block publication until the test permits it to finish."""
        with self._state_lock:
            self._publishing = True
        self.publish_started.set()
        self.release_publish.wait()
        with self._state_lock:
            self._publishing = False

    def close(self) -> None:
        """Record whether close began while publication was active."""
        with self._state_lock:
            self.close_called_during_publish = self._publishing
        self.close_called.set()


class _FailingWriter:
    """Test double that checks whether failure cleanup holds its entry lock."""

    def __init__(self) -> None:
        """Initialize state for one failed publication."""
        self.entry_lock: Any = None
        self.entry_lock_held_on_close: bool | None = None
        self.close_called = False

    def publish(self, _payload: bytes) -> None:
        """Fail publication to exercise cleanup."""
        error_message = "publish failed"
        raise RuntimeError(error_message)

    def close(self) -> None:
        """Record whether the pool holds the entry lock while closing."""
        entry_lock = self.entry_lock
        if entry_lock is None:
            error_message = "Entry lock was not configured."
            raise AssertionError(error_message)
        self.entry_lock_held_on_close = entry_lock.locked()
        self.close_called = True


@skipUnless(
    _SHM_TEST_SUPPORTED,
    "Shared-memory transport tests require POSIX capabilities.",
)
class TestHttpSharedMemoryTransport(IsolatedAsyncioTestCase):
    """Verify HTTP delivery can mirror and exclusively use local SHM channels."""

    def test_writer_pool_closes_failed_writer_under_entry_lock(
        self,
    ) -> None:
        """Close a failed writer while holding its lifecycle lock."""
        writer_pool = ShmWriterPool()
        failing_writer = _FailingWriter()
        raw_data = RawData(content=b"payload", content_type=MIME_TEXT)
        shm_path = "channel"

        with patch(
            "dtps.shm.ShmWriter",
            return_value=failing_writer,
        ):
            writer_entry = writer_pool._get_writer_entry(shm_path)
            if writer_entry is None:
                self.fail("Writer pool did not create an entry.")
            failing_writer.entry_lock = writer_entry.lock
            published = writer_pool.publish(raw_data, shm_path)

        self.assertFalse(published)
        self.assertTrue(failing_writer.close_called)
        self.assertTrue(failing_writer.entry_lock_held_on_close)
        self.assertNotIn(shm_path, writer_pool._writers)

    def test_writer_pool_logs_publish_and_cleanup_failures(self) -> None:
        """Retain the publish failure when writer cleanup also fails."""
        writer_pool = ShmWriterPool()
        failing_writer = Mock()
        publish_error = RuntimeError("publish failed")
        cleanup_error = RuntimeError("cleanup failed")
        raw_data = RawData(content=b"payload", content_type=MIME_TEXT)
        shm_path = "channel"
        failing_writer.publish.side_effect = publish_error
        failing_writer.close.side_effect = cleanup_error

        with patch(
            "dtps.shm.ShmWriter",
            return_value=failing_writer,
        ), patch("dtps.shm.logger.warning") as log_warning:
            published = writer_pool.publish(raw_data, shm_path)

        self.assertFalse(published)
        warning_message, *warning_arguments = log_warning.call_args.args
        self.assertIn("writer cleanup also failed", warning_message)
        self.assertIn("falling back to normal DTPS delivery", warning_message)
        self.assertEqual(
            warning_arguments,
            [shm_path, publish_error, cleanup_error],
        )

    def test_writer_pool_serializes_publish_and_close(self) -> None:
        """Wait for an in-flight SHM publish before closing its writer."""
        writer_pool = ShmWriterPool()
        blocking_writer = _BlockingWriter()
        raw_data = RawData(content=b"payload", content_type=MIME_TEXT)
        shm_path = "channel"
        close_entry_attempted = threading.Event()
        original_close_writer_entry = writer_pool._close_writer_entry

        def record_close_entry_attempt(
            writer_entry: _ShmWriterEntry,
        ) -> None:
            close_entry_attempted.set()
            original_close_writer_entry(writer_entry)

        with patch(
            "dtps.shm.ShmWriter",
            return_value=blocking_writer,
        ), patch.object(
            writer_pool,
            "_close_writer_entry",
            side_effect=record_close_entry_attempt,
        ):
            publish_thread = threading.Thread(
                target=writer_pool.publish,
                args=(raw_data, shm_path),
            )
            close_thread = threading.Thread(target=writer_pool.close)
            publish_thread.start()
            try:
                publish_started = blocking_writer.publish_started.wait(
                    _THREAD_TIMEOUT_SECONDS,
                )
                self.assertTrue(publish_started)
                close_thread.start()
                close_attempt_started = close_entry_attempted.wait(
                    _THREAD_TIMEOUT_SECONDS,
                )
                self.assertTrue(close_attempt_started)
                self.assertFalse(blocking_writer.close_called.is_set())
            finally:
                blocking_writer.release_publish.set()
                publish_thread.join(timeout=_THREAD_TIMEOUT_SECONDS)
                close_thread.join(timeout=_THREAD_TIMEOUT_SECONDS)

        publish_thread_alive = publish_thread.is_alive()
        close_thread_alive = close_thread.is_alive()
        close_called = blocking_writer.close_called.is_set()
        self.assertFalse(blocking_writer.close_called_during_publish)
        self.assertTrue(close_called)
        self.assertFalse(publish_thread_alive)
        self.assertFalse(close_thread_alive)

    def test_writer_pool_waits_for_failed_publish_cleanup(self) -> None:
        """Keep a failed writer registered until its close has completed."""
        writer_pool = ShmWriterPool()
        failing_writer = Mock()
        raw_data = RawData(content=b"payload", content_type=MIME_TEXT)
        close_started = threading.Event()
        allow_close = threading.Event()
        close_attempted = threading.Event()
        close_completed = threading.Event()
        original_close_writer_entry = writer_pool._close_writer_entry

        def fail_publish(_payload: bytes) -> None:
            raise RuntimeError("publish failed")

        def block_close() -> None:
            close_started.set()
            allow_close.wait()

        def record_close_attempt(writer_entry: _ShmWriterEntry) -> None:
            close_attempted.set()
            original_close_writer_entry(writer_entry)

        def close_pool() -> None:
            writer_pool.close()
            close_completed.set()

        failing_writer.publish.side_effect = fail_publish
        failing_writer.close.side_effect = block_close
        with patch(
            "dtps.shm.ShmWriter",
            return_value=failing_writer,
        ), patch.object(
            writer_pool,
            "_close_writer_entry",
            side_effect=record_close_attempt,
        ):
            publish_thread = threading.Thread(
                target=writer_pool.publish,
                args=(raw_data, "channel"),
            )
            close_thread = threading.Thread(target=close_pool)
            publish_thread.start()
            try:
                self.assertTrue(
                    close_started.wait(_THREAD_TIMEOUT_SECONDS),
                )
                close_thread.start()
                self.assertTrue(
                    close_attempted.wait(_THREAD_TIMEOUT_SECONDS),
                )
                self.assertFalse(close_completed.is_set())
            finally:
                allow_close.set()
                publish_thread.join(timeout=_THREAD_TIMEOUT_SECONDS)
                close_thread.join(timeout=_THREAD_TIMEOUT_SECONDS)

        self.assertFalse(publish_thread.is_alive())
        self.assertFalse(close_thread.is_alive())
        self.assertTrue(close_completed.is_set())
        failing_writer.close.assert_called_once()

    def test_writer_pool_allows_distinct_channel_publishes(self) -> None:
        """Do not block one channel's publication behind another channel."""
        writer_pool = ShmWriterPool()
        blocking_writer = _BlockingWriter()
        second_writer = Mock()
        raw_data = RawData(content=b"payload", content_type=MIME_TEXT)
        second_publish_completed = threading.Event()

        def publish_second_channel() -> None:
            writer_pool.publish(raw_data, "second-channel")
            second_publish_completed.set()

        second_writer.publish.side_effect = lambda _payload: None
        with patch(
            "dtps.shm.ShmWriter",
            side_effect=[blocking_writer, second_writer],
        ):
            first_publish_thread = threading.Thread(
                target=writer_pool.publish,
                args=(raw_data, "first-channel"),
            )
            second_publish_thread = threading.Thread(
                target=publish_second_channel,
            )
            first_publish_thread.start()
            try:
                first_publish_started = blocking_writer.publish_started.wait(
                    _THREAD_TIMEOUT_SECONDS,
                )
                self.assertTrue(first_publish_started)
                second_publish_thread.start()
                second_publish_completed_early = (
                    second_publish_completed.wait(_THREAD_TIMEOUT_SECONDS)
                )
            finally:
                blocking_writer.release_publish.set()
                first_publish_thread.join(timeout=_THREAD_TIMEOUT_SECONDS)
                second_publish_thread.join(timeout=_THREAD_TIMEOUT_SECONDS)
                writer_pool.close()

        self.assertTrue(second_publish_completed_early)
        self.assertFalse(first_publish_thread.is_alive())
        self.assertFalse(second_publish_thread.is_alive())
        second_writer.publish.assert_called_once()

    def test_writer_pool_does_not_reopen_after_close(self) -> None:
        """Do not create a replacement writer after terminal shutdown."""
        writer_pool = ShmWriterPool()
        raw_data = RawData(content=b"payload", content_type=MIME_TEXT)

        writer_pool.close()
        with patch("dtps.shm.ShmWriter") as writer_type:
            published = writer_pool.publish(raw_data, "channel")

        self.assertFalse(published)
        writer_type.assert_not_called()

    async def test_create_close_stops_server_after_writer_close_failure(
        self,
    ) -> None:
        """Close the server even when SHM writer cleanup raises."""
        writer_pool = Mock()
        server_wrap = Mock()
        writer_pool.close.side_effect = RuntimeError("writer close failed")
        server_wrap.aclose = AsyncMock()
        context_manager = object.__new__(ContextManagerCreate)
        context_manager._shm_writers = writer_pool
        context_manager.dtps_server_wrap = server_wrap
        context_manager._subscriptions = set()
        context_manager._closing = False

        with self.assertRaisesRegex(RuntimeError, "writer close failed"):
            await context_manager.aclose()

        server_wrap.aclose.assert_awaited_once()

    async def test_create_close_stops_registered_subscriptions(self) -> None:
        """Stop active subscriptions before closing create-side resources."""
        close_events: list[str] = []
        subscription = Mock()
        writer_pool = Mock()
        server_wrap = Mock()

        async def stop_subscription() -> None:
            close_events.append("subscription")

        def close_writers() -> None:
            close_events.append("writers")

        async def close_server() -> None:
            close_events.append("server")

        subscription.unsubscribe = AsyncMock(side_effect=stop_subscription)
        writer_pool.close.side_effect = close_writers
        server_wrap.aclose = AsyncMock(side_effect=close_server)
        context_manager = object.__new__(ContextManagerCreate)
        context_manager._shm_writers = writer_pool
        context_manager.dtps_server_wrap = server_wrap
        context_manager._subscriptions = {subscription}
        context_manager._closing = False

        await context_manager.aclose()

        subscription.unsubscribe.assert_awaited_once()
        self.assertEqual(close_events, ["subscription", "writers", "server"])

    async def test_create_subscription_stops_its_processor(self) -> None:
        """Cancel and await a create-side callback worker on unsubscribe."""
        processor_started = asyncio.Event()
        processor_cancelled = asyncio.Event()
        object_queue = Mock()
        object_queue.unsubscribe = AsyncMock()
        manager = Mock()

        async def process() -> None:
            processor_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                processor_cancelled.set()
                raise

        processor_task = asyncio.create_task(process())
        subscription_id = SUB_ID(1)
        subscription = ContextManagerCreateContextSubscriber(
            manager,
            subscription_id,
            object_queue,
            processor_task,
            asyncio.Event(),
        )
        try:
            await processor_started.wait()
            await subscription.unsubscribe()
        finally:
            processor_task.cancel()
            await asyncio.gather(processor_task, return_exceptions=True)

        object_queue.unsubscribe.assert_awaited_once_with(subscription_id)
        manager.forget_subscription.assert_called_once_with(subscription)
        self.assertTrue(processor_cancelled.is_set())
        self.assertTrue(processor_task.done())

    async def test_create_callback_can_unsubscribe_itself(self) -> None:
        """Finish a create-side callback that stops its own subscription."""
        callback_finished = asyncio.Event()
        object_queue = Mock()
        object_queue.stored = False
        object_queue.unsubscribe = AsyncMock()
        captured_callbacks: list[Any] = []

        def capture_callback(
            callback: object,
            **_kwargs: object,
        ) -> SUB_ID:
            captured_callbacks.append(callback)
            return SUB_ID(1)

        object_queue.subscribe.side_effect = capture_callback
        server = Mock()
        server.get_oq.return_value = object_queue
        manager = Mock()
        manager.remember_subscription = AsyncMock()
        context = object.__new__(ContextManagerCreateContext)
        context.master = manager
        context._topic = Mock()
        subscription_holder: list[ContextManagerCreateContextSubscriber] = []

        async def on_data(_raw_data: RawData) -> None:
            subscription = subscription_holder[0]
            await subscription.unsubscribe()
            callback_finished.set()

        with patch.object(
            ContextManagerCreateContext,
            "_get_server",
            return_value=server,
        ):
            subscription = await context.subscribe(on_data)
        assert isinstance(
            subscription,
            ContextManagerCreateContextSubscriber,
        )
        subscription_holder.append(subscription)
        wrapped_callback = captured_callbacks[0]
        notification = Mock()
        notification.raw_data = RawData(
            content=b"payload",
            content_type=MIME_TEXT,
        )
        await wrapped_callback(object_queue, notification)

        await asyncio.wait_for(callback_finished.wait(), timeout=1)
        await asyncio.wait_for(subscription._processor_task, timeout=1)

        object_queue.unsubscribe.assert_awaited_once_with(subscription.sub_id)
        manager.forget_subscription.assert_called_once_with(subscription)
        self.assertFalse(subscription._processor_task.cancelled())

    async def test_create_subscribe_cleans_listener_on_processor_failure(
        self,
    ) -> None:
        """Do not leave an object-queue listener without a processor task."""
        object_queue = Mock()
        object_queue.stored = False
        object_queue.subscribe.return_value = SUB_ID(1)
        object_queue.unsubscribe = AsyncMock()
        server = Mock()
        server.get_oq.return_value = object_queue
        context = object.__new__(ContextManagerCreateContext)
        context.master = Mock()
        context._topic = Mock()
        processors: list[Any] = []

        async def on_data(_raw_data: RawData) -> None:
            return

        def fail_task_creation(processor: Any) -> None:
            processors.append(processor)
            raise RuntimeError("processor startup failed")

        with patch.object(
            ContextManagerCreateContext,
            "_get_server",
            return_value=server,
        ), patch(
            "dtps.ergo_create.asyncio.create_task",
            side_effect=fail_task_creation,
        ), self.assertRaisesRegex(
            RuntimeError,
            "processor startup failed",
        ):
            await context.subscribe(on_data)

        object_queue.unsubscribe.assert_awaited_once_with(SUB_ID(1))
        self.assertTrue(processors[0].cr_frame is None)

    async def test_create_shm_subscription_is_registered_for_shutdown(
        self,
    ) -> None:
        """Register an SHM-only subscription with the create manager."""
        manager = Mock()
        manager.remember_subscription = AsyncMock()
        subscription = Mock()
        context = object.__new__(ContextManagerCreateContext)
        context.master = manager

        async def on_data(_raw_data: RawData) -> None:
            pass

        with patch(
            "dtps.ergo_create.create_shm_subscription",
            return_value=subscription,
        ) as create_subscription:
            result = await context.subscribe(
                on_data,
                max_frequency=13.0,
                shm_path="channel",
                shm_only=True,
            )

        self.assertIs(result, subscription)
        manager.remember_subscription.assert_awaited_once_with(subscription)
        self.assertIs(
            create_subscription.call_args.kwargs["on_unsubscribe"],
            manager.forget_subscription,
        )
        self.assertEqual(
            create_subscription.call_args.kwargs["max_frequency"],
            13.0,
        )

    async def test_use_close_waits_for_tasks_before_writers(self) -> None:
        """Let canceled publisher tasks finish before closing SHM writers."""
        close_events: list[str] = []
        task_started = asyncio.Event()
        task_wait_event = asyncio.Event()
        writer_pool = Mock()
        client = Mock()

        def close_writers() -> None:
            close_events.append("writers")

        async def publishing_task() -> None:
            task_started.set()
            try:
                await task_wait_event.wait()
            except asyncio.CancelledError:
                close_events.append("task")
                raise

        writer_pool.close.side_effect = close_writers
        client.aclose = AsyncMock()
        task = asyncio.create_task(publishing_task())
        context_manager = object.__new__(ContextManagerUse)
        context_manager._shm_writers = writer_pool
        context_manager.client = client
        context_manager.tasks = [task]
        context_manager._tasks_lock = threading.Lock()
        context_manager._subscriptions = set()
        context_manager._shm_subscriptions = set()

        try:
            await task_started.wait()
            await context_manager.aclose()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        self.assertEqual(close_events, ["task", "writers"])
        client.aclose.assert_awaited_once()

    async def test_use_close_waits_for_tasks_added_from_other_thread(
        self,
    ) -> None:
        """Await tasks registered from another thread during shutdown."""
        close_events: list[str] = []
        late_task_started = asyncio.Event()
        late_tasks: list[asyncio.Task[None]] = []
        registration_threads: list[threading.Thread] = []
        writer_pool = Mock()
        client = Mock()

        def close_writers() -> None:
            close_events.append("writers")

        async def late_task() -> None:
            late_task_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                close_events.append("task")
                raise

        context_manager = object.__new__(ContextManagerUse)
        context_manager._shm_writers = writer_pool
        context_manager.client = client
        context_manager.tasks = []
        context_manager._tasks_lock = threading.Lock()
        context_manager._subscriptions = set()
        context_manager._shm_subscriptions = set()

        async def close_client() -> None:
            task = asyncio.create_task(late_task())
            late_tasks.append(task)
            await late_task_started.wait()
            registration_thread = threading.Thread(
                target=context_manager.remember_task,
                args=(task,),
            )
            registration_threads.append(registration_thread)
            registration_thread.start()
            registration_thread.join(timeout=_THREAD_TIMEOUT_SECONDS)

        writer_pool.close.side_effect = close_writers
        client.aclose = AsyncMock(side_effect=close_client)
        try:
            await context_manager.aclose()
        finally:
            for task in late_tasks:
                task.cancel()
            if late_tasks:
                await asyncio.gather(*late_tasks, return_exceptions=True)

        self.assertFalse(registration_threads[0].is_alive())
        self.assertEqual(close_events, ["task", "writers"])
        client.aclose.assert_awaited_once()

    async def test_use_close_stops_registered_shm_subscriptions(self) -> None:
        """Stop SHM readers before the use-side client and writers close."""
        close_events: list[str] = []
        subscription = Mock()
        writer_pool = Mock()
        client = Mock()
        context_manager = object.__new__(ContextManagerUse)

        async def stop_subscription() -> None:
            close_events.append("subscription")

        async def close_client() -> None:
            close_events.append("client")

        def close_writers() -> None:
            close_events.append("writers")

        subscription.unsubscribe = AsyncMock(side_effect=stop_subscription)
        client.aclose = AsyncMock(side_effect=close_client)
        writer_pool.close.side_effect = close_writers
        context_manager._closing = False
        context_manager._shm_writers = writer_pool
        context_manager.client = client
        context_manager.tasks = []
        context_manager._tasks_lock = threading.Lock()
        context_manager._subscriptions = set()
        context_manager._shm_subscriptions = {subscription}

        await context_manager.aclose()

        subscription.unsubscribe.assert_awaited_once()
        self.assertEqual(close_events, ["subscription", "client", "writers"])

    async def test_use_close_stops_registered_normal_subscriptions(
        self,
    ) -> None:
        """Stop normal listeners before closing use-side client resources."""
        close_events: list[str] = []
        subscription = Mock()
        writer_pool = Mock()
        client = Mock()
        context_manager = object.__new__(ContextManagerUse)

        async def stop_subscription() -> None:
            close_events.append("subscription")

        async def close_client() -> None:
            close_events.append("client")

        def close_writers() -> None:
            close_events.append("writers")

        subscription.unsubscribe = AsyncMock(side_effect=stop_subscription)
        client.aclose = AsyncMock(side_effect=close_client)
        writer_pool.close.side_effect = close_writers
        context_manager._closing = False
        context_manager._shm_writers = writer_pool
        context_manager.client = client
        context_manager.tasks = []
        context_manager._tasks_lock = threading.Lock()
        context_manager._subscriptions = {subscription}
        context_manager._shm_subscriptions = set()

        await context_manager.aclose()

        subscription.unsubscribe.assert_awaited_once()
        self.assertEqual(close_events, ["subscription", "client", "writers"])

    def test_remember_task_ignores_closed_task_loop(self) -> None:
        """Keep late shutdown registration harmless after a loop closes."""
        task_loop = Mock()
        cancel_task = Mock()
        task = Mock()
        task.get_loop.return_value = task_loop
        task.cancel = cancel_task
        task_loop.call_soon_threadsafe.side_effect = RuntimeError(
            "Event loop is closed",
        )
        context_manager = object.__new__(ContextManagerUse)
        context_manager._closing = True
        context_manager.tasks = []
        context_manager._tasks_lock = threading.Lock()

        context_manager.remember_task(task)

        self.assertEqual(context_manager.tasks, [task])
        task_loop.call_soon_threadsafe.assert_any_call(
            task.add_done_callback,
            context_manager._handle_task_completion,
        )
        task_loop.call_soon_threadsafe.assert_any_call(cancel_task)
        self.assertEqual(task_loop.call_soon_threadsafe.call_count, 2)
        cancel_task.assert_not_called()

    async def test_remember_task_forgets_completed_task(self) -> None:
        """Release completed publisher and callback tasks before shutdown."""
        context_manager = object.__new__(ContextManagerUse)
        context_manager._closing = False
        context_manager.tasks = []
        context_manager._tasks_lock = threading.Lock()

        async def complete() -> None:
            return

        task = asyncio.create_task(complete())
        context_manager.remember_task(task)
        await task
        for _ in range(3):
            if not context_manager.tasks:
                break
            await asyncio.sleep(0)

        self.assertEqual(context_manager.tasks, [])

    async def test_remember_task_consumes_failed_task_exception(self) -> None:
        """Avoid an unhandled-task report when a background task fails."""
        context_manager = object.__new__(ContextManagerUse)
        context_manager._closing = False
        context_manager.tasks = []
        context_manager._tasks_lock = threading.Lock()
        loop = asyncio.get_running_loop()
        unhandled_contexts: list[dict[str, object]] = []
        previous_exception_handler = loop.get_exception_handler()

        async def fail() -> None:
            raise RuntimeError("background task failed")

        loop.set_exception_handler(
            lambda _loop, context: unhandled_contexts.append(context),
        )
        try:
            with patch("dtps.ergo_use.logger.error") as log_error:
                task = asyncio.create_task(fail())
                context_manager.remember_task(task)
                await asyncio.sleep(0)
                await asyncio.sleep(0)
                del task
                await asyncio.sleep(0)

            self.assertEqual(context_manager.tasks, [])
            self.assertEqual(unhandled_contexts, [])
            log_error.assert_called_once()
            self.assertIn(
                "DTPS background task failed",
                log_error.call_args.args[0],
            )
        finally:
            loop.set_exception_handler(previous_exception_handler)

    async def test_remember_late_task_consumes_cancellation_failure(
        self,
    ) -> None:
        """Observe a task registered after the use manager starts closing."""
        context_manager = object.__new__(ContextManagerUse)
        context_manager._closing = True
        context_manager.tasks = []
        context_manager._tasks_lock = threading.Lock()
        loop = asyncio.get_running_loop()
        unhandled_contexts: list[dict[str, object]] = []
        previous_exception_handler = loop.get_exception_handler()

        async def fail_after_cancellation() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise RuntimeError("late task failed during cancellation")

        loop.set_exception_handler(
            lambda _loop, context: unhandled_contexts.append(context),
        )
        try:
            with patch("dtps.ergo_use.logger.error") as log_error:
                task = asyncio.create_task(fail_after_cancellation())
                context_manager.remember_task(task)
                for _ in range(3):
                    if not context_manager.tasks:
                        break
                    await asyncio.sleep(0)

            self.assertTrue(task.done())
            self.assertEqual(context_manager.tasks, [])
            self.assertEqual(unhandled_contexts, [])
            log_error.assert_called_once()
            self.assertIn(
                "DTPS background task failed",
                log_error.call_args.args[0],
            )
        finally:
            loop.set_exception_handler(previous_exception_handler)

    async def test_task_shutdown_schedules_foreign_loop_cancellation(
        self,
    ) -> None:
        """Cancel a foreign-loop task without awaiting it on this loop."""
        foreign_loop = Mock()
        cancel_task = Mock()
        foreign_task = Mock()
        foreign_task.cancel = cancel_task
        foreign_task.get_loop.return_value = foreign_loop
        context_manager = object.__new__(ContextManagerUse)
        context_manager.tasks = [foreign_task]
        context_manager._tasks_lock = threading.Lock()

        await context_manager._cancel_and_wait_for_tasks()

        foreign_loop.call_soon_threadsafe.assert_called_once_with(cancel_task)
        cancel_task.assert_not_called()
        self.assertEqual(context_manager.tasks, [])

    async def test_use_publisher_task_is_awaited_during_shutdown(
        self,
    ) -> None:
        """Cancel and await a continuous publisher before closing its client."""
        task_started = asyncio.Event()
        task_cancelled = asyncio.Event()
        writer_pool = Mock()
        client = Mock()
        context_manager = object.__new__(ContextManagerUse)
        context_manager._closing = False
        context_manager._shm_writers = writer_pool
        context_manager.client = client
        context_manager.tasks = []
        context_manager._tasks_lock = threading.Lock()
        context_manager._subscriptions = set()
        context_manager._shm_subscriptions = set()

        async def pushing_task() -> None:
            task_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                task_cancelled.set()
                raise

        task = asyncio.create_task(pushing_task())
        client.aclose = AsyncMock()
        client.push_continuous = AsyncMock(return_value=task)
        context = Mock()
        context._get_best_url = AsyncMock(return_value="url")
        publisher = ContextManagerUseContextPublisher(
            context,
            context_manager,
        )

        try:
            await publisher.init()
            await task_started.wait()
            await context_manager.aclose()
            self.assertTrue(task_cancelled.is_set())
            self.assertTrue(task.done())
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        client.aclose.assert_awaited_once()

    async def test_use_publisher_terminate_waits_for_task(self) -> None:
        """Complete continuous publisher cancellation before returning."""
        task_started = asyncio.Event()

        async def pushing_task() -> None:
            task_started.set()
            await asyncio.Event().wait()

        publisher = object.__new__(ContextManagerUseContextPublisher)
        task = asyncio.create_task(pushing_task())
        publisher.task_push = task
        try:
            await task_started.wait()
            await publisher.terminate()
            self.assertTrue(task.done())
            self.assertTrue(task.cancelled())
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_use_subscription_tracks_processor_task(self) -> None:
        """Register the callback processor for manager shutdown."""
        recorded_tasks: list[asyncio.Task[None]] = []
        listener = Mock()
        client = Mock()
        client.listen_url = AsyncMock(return_value=listener)
        manager = Mock()
        manager.client = client
        manager.remember_task.side_effect = recorded_tasks.append
        context = object.__new__(ContextManagerUseContext)
        context.master = manager

        async def on_data(_raw_data: RawData) -> None:
            pass

        try:
            with patch.object(
                ContextManagerUseContext,
                "_get_best_url",
                new=AsyncMock(return_value="url"),
            ):
                await context.subscribe_once(on_data)
            self.assertEqual(len(recorded_tasks), 1)
            processor_task = recorded_tasks[0]
            self.assertIsInstance(processor_task, asyncio.Task)
        finally:
            for processor_task in recorded_tasks:
                processor_task.cancel()
            if recorded_tasks:
                await asyncio.gather(
                    *recorded_tasks,
                    return_exceptions=True,
                )

    async def test_use_subscription_closes_processor_on_task_failure(
        self,
    ) -> None:
        """Do not leak a processor coroutine when task creation raises."""
        client = Mock()
        manager = Mock()
        manager.client = client
        context = object.__new__(ContextManagerUseContext)
        context.master = manager
        processors: list[Any] = []

        async def on_data(_raw_data: RawData) -> None:
            return

        def fail_task_creation(processor: Any) -> None:
            processors.append(processor)
            raise RuntimeError("processor startup failed")

        with patch.object(
            ContextManagerUseContext,
            "_get_best_url",
            new=AsyncMock(return_value="url"),
        ), patch(
            "dtps.ergo_use.asyncio.create_task",
            side_effect=fail_task_creation,
        ), self.assertRaisesRegex(
            RuntimeError,
            "processor startup failed",
        ):
            await context.subscribe_once(on_data)

        manager.remember_task.assert_not_called()
        client.listen_url.assert_not_called()
        self.assertTrue(processors[0].cr_frame is None)

    async def test_use_subscription_stops_processor_on_unsubscribe(
        self,
    ) -> None:
        """Cancel the normal delivery callback worker on unsubscribe."""
        processor_started = asyncio.Event()
        processor_cancelled = asyncio.Event()
        listener = Mock()
        listener.stop = AsyncMock()

        async def process() -> None:
            processor_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                processor_cancelled.set()
                raise

        processor_task = asyncio.create_task(process())
        subscription = ContextManagerUseSubscription(
            listener,
            processor_task,
            asyncio.Event(),
        )
        try:
            await processor_started.wait()
            await subscription.unsubscribe()
        finally:
            processor_task.cancel()
            await asyncio.gather(processor_task, return_exceptions=True)

        listener.stop.assert_awaited_once()
        self.assertTrue(processor_cancelled.is_set())
        self.assertTrue(processor_task.done())

    async def test_use_callback_can_unsubscribe_itself(self) -> None:
        """Finish a remote callback that stops its own subscription."""
        callback_finished = asyncio.Event()
        listener = Mock()
        listener.stop = AsyncMock()
        client = Mock()
        client.listen_url = AsyncMock(return_value=listener)
        manager = Mock()
        manager.client = client
        manager.remember_task = Mock()
        context = object.__new__(ContextManagerUseContext)
        context.master = manager
        subscription_holder: list[ContextManagerUseSubscription] = []

        async def on_data(_raw_data: RawData) -> None:
            subscription = subscription_holder[0]
            await subscription.unsubscribe()
            callback_finished.set()

        with patch.object(
            ContextManagerUseContext,
            "_get_best_url",
            new=AsyncMock(return_value="url"),
        ):
            subscription = await context.subscribe_once(on_data)
        assert isinstance(subscription, ContextManagerUseSubscription)
        subscription_holder.append(subscription)
        wrapped_callback = client.listen_url.call_args.args[1]
        await wrapped_callback(
            RawData(content=b"payload", content_type=MIME_TEXT),
        )

        await asyncio.wait_for(callback_finished.wait(), timeout=1)
        await asyncio.wait_for(subscription._processor_task, timeout=1)

        listener.stop.assert_awaited_once()
        self.assertFalse(subscription._processor_task.cancelled())

    async def test_use_subscription_setup_failure_stops_processor(
        self,
    ) -> None:
        """Do not retain a callback worker when listener setup raises."""
        recorded_tasks: list[asyncio.Task[None]] = []
        client = Mock()
        client.listen_url = AsyncMock(side_effect=RuntimeError("listen failed"))
        manager = Mock()
        manager.client = client
        manager.remember_task.side_effect = recorded_tasks.append
        context = object.__new__(ContextManagerUseContext)
        context.master = manager

        async def on_data(_raw_data: RawData) -> None:
            pass

        with patch.object(
            ContextManagerUseContext,
            "_get_best_url",
            new=AsyncMock(return_value="url"),
        ), self.assertRaisesRegex(RuntimeError, "listen failed"):
            await context.subscribe_once(on_data)

        self.assertEqual(len(recorded_tasks), 1)
        processor_task = recorded_tasks[0]
        self.assertTrue(processor_task.done())
        self.assertTrue(processor_task.cancelled())

    async def test_use_subscription_rejects_closing_manager(self) -> None:
        """Do not return a listener created after manager shutdown starts."""
        listener = Mock()
        listener.stop = AsyncMock()
        client = Mock()
        client.listen_url = AsyncMock(return_value=listener)
        manager = object.__new__(ContextManagerUse)
        manager._closing = True
        manager._tasks_lock = threading.Lock()
        manager.tasks = []
        manager.client = client
        manager._subscriptions = set()
        context = object.__new__(ContextManagerUseContext)
        context.master = manager
        context.config = ContextConfig.default()

        async def on_data(_raw_data: RawData) -> None:
            return

        with patch.object(
            ContextManagerUseContext,
            "_get_best_url",
            new=AsyncMock(return_value="url"),
        ), self.assertRaisesRegex(
            RuntimeError,
            "Context manager is closing",
        ):
            await context.subscribe(on_data)

        listener.stop.assert_awaited_once()

    async def test_use_shm_subscription_is_registered_for_shutdown(
        self,
    ) -> None:
        """Register an SHM-only subscription with the use manager."""
        manager = Mock()
        manager.remember_shm_subscription = AsyncMock()
        subscription = Mock()
        context = object.__new__(ContextManagerUseContext)
        context.master = manager

        async def on_data(_raw_data: RawData) -> None:
            pass

        with patch(
            "dtps.ergo_use.create_shm_subscription",
            return_value=subscription,
        ) as create_subscription:
            result = await context.subscribe(
                on_data,
                max_frequency=13.0,
                shm_path="channel",
                shm_only=True,
            )

        self.assertIs(result, subscription)
        manager.remember_shm_subscription.assert_awaited_once_with(
            subscription,
        )
        self.assertIs(
            create_subscription.call_args.kwargs["on_unsubscribe"],
            manager.forget_shm_subscription,
        )
        self.assertEqual(
            create_subscription.call_args.kwargs["max_frequency"],
            13.0,
        )

    async def test_patient_unsubscribe_closes_late_subscription(self) -> None:
        """Close a real subscription assigned after the caller stops waiting."""
        subscribe_started = asyncio.Event()
        release_subscription = asyncio.Event()
        real_subscription = Mock()
        real_subscription.unsubscribe = AsyncMock()
        fldi = FakeSubscriptionInterface(asyncio.Event())
        context = object.__new__(ContextManagerUseContext)

        async def subscribe_once(
            *_args: object,
            **_kwargs: object,
        ) -> object:
            subscribe_started.set()
            await release_subscription.wait()
            return real_subscription

        setattr(context, "subscribe_once", subscribe_once)

        async def on_data(_raw_data: RawData) -> None:
            pass

        patient_task = asyncio.create_task(
            context._subscribe_patient_task(fldi, on_data),
        )
        try:
            await subscribe_started.wait()
            await fldi.unsubscribe()
            release_subscription.set()
            await asyncio.wait_for(patient_task, timeout=1)
        finally:
            patient_task.cancel()
            await asyncio.gather(patient_task, return_exceptions=True)

        real_subscription.unsubscribe.assert_awaited_once()

    async def test_patient_task_cancellation_closes_subscription(self) -> None:
        """Release the active real subscription during manager shutdown."""
        subscription_assigned = asyncio.Event()
        real_subscription = Mock()
        real_subscription.unsubscribe = AsyncMock()
        fldi = FakeSubscriptionInterface(asyncio.Event())
        context = object.__new__(ContextManagerUseContext)

        async def subscribe_once(
            *_args: object,
            **_kwargs: object,
        ) -> object:
            subscription_assigned.set()
            return real_subscription

        setattr(context, "subscribe_once", subscribe_once)

        async def on_data(_raw_data: RawData) -> None:
            pass

        patient_task = asyncio.create_task(
            context._subscribe_patient_task(fldi, on_data),
        )
        try:
            await subscription_assigned.wait()
            await asyncio.sleep(0)
            patient_task.cancel()
            await asyncio.gather(patient_task, return_exceptions=True)
        finally:
            patient_task.cancel()
            await asyncio.gather(patient_task, return_exceptions=True)

        real_subscription.unsubscribe.assert_awaited_once()

    async def test_patient_unsubscribe_wakes_active_subscription(self) -> None:
        """Wake a patient retry worker when its active subscription stops."""
        subscription_assigned = asyncio.Event()
        real_subscription = Mock()
        real_subscription.unsubscribe = AsyncMock()
        fldi = FakeSubscriptionInterface(asyncio.Event())
        context = object.__new__(ContextManagerUseContext)

        async def subscribe_once(
            *_args: object,
            **_kwargs: object,
        ) -> object:
            subscription_assigned.set()
            return real_subscription

        setattr(context, "subscribe_once", subscribe_once)

        async def on_data(_raw_data: RawData) -> None:
            pass

        patient_task = asyncio.create_task(
            context._subscribe_patient_task(fldi, on_data),
        )
        try:
            await subscription_assigned.wait()
            await asyncio.sleep(0)
            self.assertIs(fldi.real, real_subscription)
            await fldi.unsubscribe()
            await asyncio.wait_for(patient_task, timeout=1)
        finally:
            patient_task.cancel()
            await asyncio.gather(patient_task, return_exceptions=True)

        real_subscription.unsubscribe.assert_awaited_once()

    def test_shm_publish_http_decision(self) -> None:
        """Select HTTP delivery consistently for every SHM publish outcome."""
        writer_pool = ShmWriterPool()
        raw_data = RawData(content=b"payload", content_type=MIME_TEXT)

        with tempfile.TemporaryDirectory() as directory:
            mirrored_path = Path(directory) / "mirrored"
            invalid_path = Path(directory) / "invalid"
            invalid_signal_name = f"{invalid_path.name}.p2c"
            invalid_signal_path = invalid_path.parent / invalid_signal_name
            invalid_signal_path.write_bytes(b"not a fifo")
            try:
                publish_http = should_publish_http(
                    writer_pool,
                    raw_data,
                    shm_path=None,
                    shm_only=False,
                )
                self.assertTrue(publish_http)
                with self.assertRaises(ValueError):
                    should_publish_http(
                        writer_pool,
                        raw_data,
                        shm_path=None,
                        shm_only=True,
                    )
                mirrored_path_string = str(mirrored_path)
                publish_http = should_publish_http(
                    writer_pool,
                    raw_data,
                    shm_path=mirrored_path_string,
                    shm_only=False,
                )
                self.assertTrue(publish_http)
                publish_http = should_publish_http(
                    writer_pool,
                    raw_data,
                    shm_path=mirrored_path_string,
                    shm_only=True,
                )
                self.assertFalse(publish_http)
                invalid_path_string = str(invalid_path)
                publish_http = should_publish_http(
                    writer_pool,
                    raw_data,
                    shm_path=invalid_path_string,
                    shm_only=True,
                )
                self.assertTrue(publish_http)
            finally:
                writer_pool.close()

    async def test_subscriptions_reject_invalid_max_frequency(self) -> None:
        """Reject invalid rate limits before opening a subscription."""

        async def on_data(_raw_data: RawData) -> None:
            return

        invalid_frequencies = (0.0, -1.0, float("inf"), float("nan"))
        with tempfile.TemporaryDirectory() as directory:
            channel_path = str(Path(directory) / "invalid-frequency")
            for max_frequency in invalid_frequencies:
                with self.subTest(max_frequency=max_frequency):
                    try:
                        subscription = create_shm_subscription(
                            on_data,
                            channel_path,
                            1,
                            max_frequency=max_frequency,
                        )
                    except ValueError as error:
                        self.assertRegex(
                            str(error),
                            "finite positive number",
                        )
                    else:
                        await subscription.unsubscribe()
                        self.fail("Invalid max_frequency was accepted.")

        for context_class in (
            ContextManagerCreateContext,
            ContextManagerUseContext,
        ):
            context = object.__new__(context_class)
            with self.subTest(context_class=context_class.__name__):
                with self.assertRaisesRegex(
                    ValueError,
                    "finite positive number",
                ):
                    await context.subscribe(on_data, max_frequency=0.0)

    async def test_enqueue_retries_when_queue_refills(self) -> None:
        """Keep the newest payload when insertion encounters another full queue."""
        subscription = object.__new__(_ShmSubscription)
        subscription._closed = False
        subscription._when = Mock()
        subscription._when.now.return_value = True
        queue: asyncio.Queue[RawData] = asyncio.Queue(maxsize=1)
        stale_payload = RawData(content=b"stale", content_type=MIME_TEXT)
        competing_payload = RawData(
            content=b"competing", content_type=MIME_TEXT
        )
        expected_payload = RawData(content=b"expected", content_type=MIME_TEXT)
        queue.put_nowait(stale_payload)

        original_put_nowait = queue.put_nowait
        put_attempts = 0

        def put_nowait(item: RawData) -> None:
            nonlocal put_attempts
            put_attempts += 1
            if put_attempts == 2:
                original_put_nowait(competing_payload)
            original_put_nowait(item)

        queue.put_nowait = put_nowait  # type: ignore[method-assign]
        subscription._queue = queue

        subscription._enqueue(expected_payload)

        queued_payload = queue.get_nowait()
        self.assertEqual(queued_payload, expected_payload)
        self.assertEqual(put_attempts, 3)
        queue.task_done()
        await asyncio.wait_for(queue.join(), timeout=1)

    async def test_enqueue_respects_max_frequency(self) -> None:
        """Discard SHM payloads that arrive before the next local interval."""
        subscription = object.__new__(_ShmSubscription)
        subscription._closed = False
        subscription._when = Mock()
        subscription._when.now.side_effect = [True, False, True]
        queue: asyncio.Queue[RawData] = asyncio.Queue(maxsize=3)
        first_payload = RawData(content=b"first", content_type=MIME_TEXT)
        skipped_payload = RawData(
            content=b"skipped",
            content_type=MIME_TEXT,
        )
        latest_payload = RawData(content=b"latest", content_type=MIME_TEXT)
        subscription._queue = queue

        subscription._enqueue(first_payload)
        subscription._enqueue(skipped_payload)
        subscription._enqueue(latest_payload)

        self.assertEqual(queue.get_nowait(), first_payload)
        queue.task_done()
        self.assertEqual(queue.get_nowait(), latest_payload)
        queue.task_done()
        await asyncio.wait_for(queue.join(), timeout=1)

    async def test_processor_marks_queue_item_done(self) -> None:
        """Keep queue joins usable after a callback completes."""
        subscription = object.__new__(_ShmSubscription)
        queue: asyncio.Queue[RawData] = asyncio.Queue()
        raw_data = RawData(content=b"payload", content_type=MIME_TEXT)
        on_data = AsyncMock()
        subscription._queue = queue
        subscription._on_data = on_data
        processor_task = asyncio.create_task(subscription._process())
        try:
            queue.put_nowait(raw_data)
            await asyncio.wait_for(queue.join(), timeout=1)
        finally:
            processor_task.cancel()
            await asyncio.gather(processor_task, return_exceptions=True)

        on_data.assert_awaited_once_with(raw_data)

    async def test_unsubscribe_cancels_processor_after_reader_stop_failure(
        self,
    ) -> None:
        """Do not orphan a callback worker when reader shutdown fails."""
        processor_started = asyncio.Event()
        processor_cancelled = asyncio.Event()
        reader = Mock()
        reader.stop.side_effect = RuntimeError("reader stop failed")
        unsubscribe_callback = Mock()
        subscription = object.__new__(_ShmSubscription)
        subscription._closed = False
        subscription._reader = reader
        subscription._on_unsubscribe = unsubscribe_callback

        async def process() -> None:
            processor_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                processor_cancelled.set()
                raise

        processor_task = asyncio.create_task(process())
        subscription._processor_task = processor_task
        try:
            await processor_started.wait()
            with self.assertRaisesRegex(RuntimeError, "reader stop failed"):
                await subscription.unsubscribe()
        finally:
            processor_task.cancel()
            await asyncio.gather(processor_task, return_exceptions=True)

        self.assertTrue(processor_cancelled.is_set())
        unsubscribe_callback.assert_called_once_with(subscription)

    async def test_callback_can_unsubscribe_its_own_shm_subscription(
        self,
    ) -> None:
        """Finish the active callback without cancelling its own task."""
        callback_finished = asyncio.Event()
        reader = Mock()
        subscription = object.__new__(_ShmSubscription)
        subscription._closed = False
        subscription._reader = reader
        subscription._on_unsubscribe = Mock()
        subscription._queue = asyncio.Queue()

        async def on_data(_raw_data: RawData) -> None:
            await subscription.unsubscribe()
            callback_finished.set()

        subscription._on_data = on_data
        processor_task = asyncio.create_task(subscription._process())
        subscription._processor_task = processor_task
        subscription._queue.put_nowait(
            RawData(content=b"payload", content_type=MIME_TEXT),
        )

        await asyncio.wait_for(callback_finished.wait(), timeout=1)
        await asyncio.wait_for(subscription._queue.join(), timeout=1)
        await asyncio.wait_for(processor_task, timeout=1)

        reader.stop.assert_called_once()
        subscription._on_unsubscribe.assert_called_once_with(subscription)
        self.assertFalse(processor_task.cancelled())

    async def test_reader_start_failure_does_not_create_processor_task(
        self,
    ) -> None:
        """Avoid leaving a cancelled processor task when reader startup fails."""
        reader = Mock()
        reader.start.side_effect = RuntimeError("reader startup failed")
        loop = Mock()

        async def on_data(_raw_data: RawData) -> None:
            pass

        with patch(
            "dtps.shm.asyncio.get_running_loop",
            return_value=loop,
        ), patch(
            "dtps.shm.ShmReader",
            return_value=reader,
        ), self.assertRaisesRegex(
            RuntimeError,
            "reader startup failed",
        ):
            _ShmSubscription(on_data, "channel", queue_size=1)

        loop.create_task.assert_not_called()

    async def test_processor_start_failure_stops_reader(self) -> None:
        """Release reader resources when processor task creation fails."""
        reader = Mock()
        processor = Mock()
        process = Mock(return_value=processor)
        loop = Mock()
        loop.create_task.side_effect = RuntimeError("processor startup failed")

        async def on_data(_raw_data: RawData) -> None:
            pass

        with patch(
            "dtps.shm.asyncio.get_running_loop",
            return_value=loop,
        ), patch(
            "dtps.shm.ShmReader",
            return_value=reader,
        ), patch.object(
            _ShmSubscription,
            "_process",
            new=process,
        ), self.assertRaisesRegex(
            RuntimeError,
            "processor startup failed",
        ):
            _ShmSubscription(on_data, "channel", queue_size=1)

        reader.start.assert_called_once()
        reader.stop.assert_called_once()
        processor.close.assert_called_once()

    def test_frequency_ignores_only_stale_publications(self) -> None:
        """Avoid indexing an emptied publication history after a clock jump."""
        context = object.__new__(ContextManagerUseContext)
        context.last_published = [0.0]

        with patch("dtps.ergo_use.time.time", return_value=20.0):
            frequency = context._get_frequency_publishing()

        self.assertEqual(frequency, 0.0)
        self.assertEqual(context.last_published, [])

    @test_timeout(5)
    async def test_context_publish_mirrors_http_and_shm(self) -> None:
        """Mirror one RawData value to both selected transport paths."""
        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "mirrored"
            shm_path = str(channel_path)
            async with create_use_pair("shmmirror") as (
                context_create,
                context_use,
            ):
                create_topic_context = context_create / "topic"
                create_topic = await create_topic_context.queue_create()
                use_topic = context_use / "topic"
                expected = RawData(
                    content=b"mirrored payload",
                    content_type=MIME_TEXT,
                )
                http_received: list[RawData] = []
                shm_received: list[RawData] = []
                dtps_event = asyncio.Event()
                shm_event = asyncio.Event()

                async def on_http(raw_data: RawData) -> None:
                    http_received.append(raw_data)
                    dtps_event.set()

                async def on_shm(raw_data: RawData) -> None:
                    shm_received.append(raw_data)
                    shm_event.set()

                http_subscription = await use_topic.subscribe(on_http)
                shm_subscription = await use_topic.subscribe(
                    on_shm,
                    shm_path=shm_path,
                    shm_only=True,
                )
                try:
                    await create_topic.publish(expected, shm_path=shm_path)
                    await asyncio.wait_for(dtps_event.wait(), timeout=1)
                    await asyncio.wait_for(shm_event.wait(), timeout=1)
                finally:
                    await shm_subscription.unsubscribe()
                    await http_subscription.unsubscribe()

        self.assertEqual(http_received, [expected])
        self.assertEqual(shm_received, [expected])

    @test_timeout(5)
    async def test_publisher_can_use_shm_only(self) -> None:
        """Deliver a RawData value through a remote SHM-only publisher."""
        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "publisher-only"
            shm_path = str(channel_path)
            async with create_use_pair("shmpublisher") as (
                context_create,
                context_use,
            ):
                create_topic_context = context_create / "topic"
                create_topic = await create_topic_context.queue_create()
                use_topic = context_use / "topic"
                expected = RawData(
                    content=b"shared-memory only",
                    content_type=MIME_TEXT,
                )
                received: list[RawData] = []
                received_event = asyncio.Event()

                async def on_data(raw_data: RawData) -> None:
                    received.append(raw_data)
                    received_event.set()

                subscription = await create_topic.subscribe(
                    on_data,
                    shm_path=shm_path,
                    shm_only=True,
                )
                publisher = await use_topic.publisher()
                try:
                    await publisher.publish(
                        expected,
                        shm_path=shm_path,
                        shm_only=True,
                    )
                    await asyncio.wait_for(received_event.wait(), timeout=1)
                finally:
                    await publisher.terminate()
                    await subscription.unsubscribe()

        self.assertEqual(received, [expected])

    @test_timeout(5)
    async def test_remote_context_publish_can_use_shm_only(self) -> None:
        """Deliver a RawData value through direct remote context publishing."""
        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "remote-context-only"
            shm_path = str(channel_path)
            async with create_use_pair("shmremotecontext") as (
                context_create,
                context_use,
            ):
                create_topic_context = context_create / "topic"
                create_topic = await create_topic_context.queue_create()
                use_topic = context_use / "topic"
                expected = RawData(
                    content=b"direct shared-memory only",
                    content_type=MIME_TEXT,
                )
                received: list[RawData] = []
                received_event = asyncio.Event()

                async def on_data(raw_data: RawData) -> None:
                    received.append(raw_data)
                    received_event.set()

                subscription = await create_topic.subscribe(
                    on_data,
                    shm_path=shm_path,
                    shm_only=True,
                )
                try:
                    await use_topic.publish(
                        expected,
                        shm_path=shm_path,
                        shm_only=True,
                    )
                    await asyncio.wait_for(received_event.wait(), timeout=1)
                finally:
                    await subscription.unsubscribe()

        self.assertEqual(received, [expected])

    @test_timeout(5)
    async def test_mirrored_subscription_does_not_duplicate_callbacks(
        self,
    ) -> None:
        """A non-SHM-only subscriber keeps its single HTTP delivery path."""
        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "non-duplicated"
            shm_path = str(channel_path)
            async with create_use_pair("shmduplicate") as (
                context_create,
                context_use,
            ):
                create_topic_context = context_create / "topic"
                create_topic = await create_topic_context.queue_create()
                use_topic = context_use / "topic"
                expected = RawData(
                    content=b"one callback",
                    content_type=MIME_TEXT,
                )
                received: list[RawData] = []
                received_event = asyncio.Event()
                unexpected_callback_event = asyncio.Event()

                async def on_data(raw_data: RawData) -> None:
                    received.append(raw_data)
                    if len(received) == 1:
                        received_event.set()
                    else:
                        unexpected_callback_event.set()

                subscription = await use_topic.subscribe(
                    on_data,
                    shm_path=shm_path,
                )
                try:
                    await create_topic.publish(expected, shm_path=shm_path)
                    await asyncio.wait_for(received_event.wait(), timeout=1)
                    with self.assertRaises(asyncio.TimeoutError):
                        await asyncio.wait_for(
                            unexpected_callback_event.wait(),
                            timeout=0.1,
                        )
                finally:
                    await subscription.unsubscribe()

        self.assertEqual(received, [expected])

    @test_timeout(5)
    async def test_unsubscribe_stops_shm_delivery(self) -> None:
        """Stopping a SHM subscription stops its reader and processor."""
        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "unsubscribe"
            shm_path = str(channel_path)
            async with create_use_pair("shmunsubscribe") as (
                context_create,
                context_use,
            ):
                create_topic_context = context_create / "topic"
                create_topic = await create_topic_context.queue_create()
                use_topic = context_use / "topic"
                first_value = RawData(
                    content=b"first",
                    content_type=MIME_TEXT,
                )
                second_value = RawData(
                    content=b"second",
                    content_type=MIME_TEXT,
                )
                received: list[RawData] = []
                received_event = asyncio.Event()
                unexpected_callback_event = asyncio.Event()

                async def on_data(raw_data: RawData) -> None:
                    received.append(raw_data)
                    if len(received) == 1:
                        received_event.set()
                    else:
                        unexpected_callback_event.set()

                subscription = await use_topic.subscribe(
                    on_data,
                    shm_path=shm_path,
                    shm_only=True,
                )
                await create_topic.publish(
                    first_value,
                    shm_path=shm_path,
                    shm_only=True,
                )
                await asyncio.wait_for(received_event.wait(), timeout=1)
                await subscription.unsubscribe()
                await create_topic.publish(
                    second_value,
                    shm_path=shm_path,
                    shm_only=True,
                )
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        unexpected_callback_event.wait(),
                        timeout=0.1,
                    )

        self.assertEqual(received, [first_value])

    @test_timeout(5)
    async def test_shm_only_requires_a_path(self) -> None:
        """Reject a transport request that cannot identify an SHM channel."""
        async with create_use_pair("shmrequirespath") as (
            context_create,
            _context_use,
        ):
            create_topic_context = context_create / "topic"
            create_topic = await create_topic_context.queue_create()
            payload = RawData(
                content=b"missing path",
                content_type=MIME_TEXT,
            )

            with self.assertRaises(ValueError):
                await create_topic.publish(payload, shm_only=True)

    @test_timeout(5)
    async def test_shm_only_subscription_requires_a_path(self) -> None:
        """Reject an SHM-only subscription without a channel path."""

        async def on_data(_raw_data: RawData) -> None:
            pass

        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "missing.sock"
            async with create_use(
                name="shmsubscriberequirespath",
                socket_node=str(socket_path),
            ) as context_use:
                direct_config = ContextConfig(patient=False)
                direct_context = context_use.configure(
                    direct_config,
                )
                direct_topic = direct_context / "topic"
                with self.assertRaises(ValueError):
                    await direct_topic.subscribe(
                        on_data,
                        shm_only=True,
                    )

    @test_timeout(5)
    async def test_shm_only_subscription_does_not_require_http_availability(
        self,
    ) -> None:
        """Create a local SHM subscription while its HTTP endpoint is absent."""

        async def on_data(_raw_data: RawData) -> None:
            pass

        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "missing.sock"
            shm_path = Path(directory) / "subscription"
            async with create_use(
                name="shmsubscribeunreachable",
                socket_node=str(socket_path),
            ) as context_use:
                direct_config = ContextConfig(patient=False)
                direct_context = context_use.configure(
                    direct_config,
                )
                direct_topic = direct_context / "topic"
                subscription = await direct_topic.subscribe(
                    on_data,
                    shm_path=str(shm_path),
                    shm_only=True,
                )
                try:
                    socket_exists = socket_path.exists()
                    self.assertFalse(socket_exists)
                finally:
                    await subscription.unsubscribe()

    @test_timeout(5)
    async def test_failed_shm_only_publish_falls_back_to_http(self) -> None:
        """Preserve HTTP delivery when the configured SHM channel is invalid."""
        with tempfile.TemporaryDirectory() as directory:
            channel_path = Path(directory) / "invalid"
            shm_path = str(channel_path)
            signal_path = Path(shm_path + ".p2c")
            signal_path.write_bytes(b"not a fifo")
            async with create_use_pair("shmfallback") as (
                context_create,
                context_use,
            ):
                create_topic_context = context_create / "topic"
                create_topic = await create_topic_context.queue_create()
                use_topic = context_use / "topic"
                expected = RawData(
                    content=b"fallback payload",
                    content_type=MIME_TEXT,
                )
                received: list[RawData] = []
                received_event = asyncio.Event()

                async def on_data(raw_data: RawData) -> None:
                    received.append(raw_data)
                    received_event.set()

                subscription = await use_topic.subscribe(on_data)
                try:
                    await create_topic.publish(
                        expected,
                        shm_path=shm_path,
                        shm_only=True,
                    )
                    await asyncio.wait_for(received_event.wait(), timeout=1)
                finally:
                    await subscription.unsubscribe()

        self.assertEqual(received, [expected])
