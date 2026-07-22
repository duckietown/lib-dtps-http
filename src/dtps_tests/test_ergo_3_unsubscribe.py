import asyncio
import threading
from typing import Any, List
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

from dtps import ContextConfig, DTPSContext
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
)
from dtps_http import async_error_catcher, MIME_TEXT, RawData, SUB_ID
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger
from dtps_tests.utils import create_use_pair


async def check_ergo_unsub(base: DTPSContext, inline: bool) -> None:
    node_input = await (base / "dtps" / "node" / "in").queue_create()

    rd = RawData(content=b"hello", content_type=MIME_TEXT)

    received: List[RawData] = []

    @async_error_catcher
    async def on_input(data: RawData, /) -> None:
        n = len(received)
        logger.info(f"received #{n}")
        received.append(data)

    sub1 = await node_input.subscribe(on_input, inline=inline)

    await asyncio.sleep(1)

    await node_input.publish(rd)
    await node_input.publish(rd)

    await asyncio.sleep(2)

    if len(received) != 2:
        raise AssertionError("expected 2")

    await sub1.unsubscribe()

    await node_input.publish(rd)
    await node_input.publish(rd)

    await asyncio.sleep(2)

    if len(received) != 2:
        raise AssertionError("expected 2")


class TestErgoUnsub(IsolatedAsyncioTestCase):
    @test_timeout(15)
    async def test_ergo_simple__create__inline__before(self):
        async with create_use_pair("testcreate") as (context_create, context_use):
            await check_ergo_unsub(
                context_create,
                inline=True,
            )

    @test_timeout(15)
    async def test_ergo_simple__create__offline_before(self):
        async with create_use_pair("testcreate") as (context_create, context_use):
            await check_ergo_unsub(
                context_create,
                inline=False,
            )

    @test_timeout(15)
    async def test_ergo_simple__use__inline_before(self):
        # create a server
        async with create_use_pair("testuse") as (context_create, context_use):
            await check_ergo_unsub(
                context_use,
                inline=True,
            )

    @test_timeout(15)
    async def test_ergo_simple__use__offline_before(self):
        # create a server
        async with create_use_pair("testuse") as (context_create, context_use):
            await check_ergo_unsub(
                context_use,
                inline=False,
            )


class TestCreateSubscriptionLifecycle(IsolatedAsyncioTestCase):
    async def test_create_manager_unsubscribes_tracked_subscriptions(
        self,
    ) -> None:
        """Stop tracked create-side subscriptions during manager shutdown."""
        subscription = Mock()
        subscription.unsubscribe = AsyncMock()
        manager = object.__new__(ContextManagerCreate)
        manager._subscriptions = {subscription}

        await manager._unsubscribe_all()

        subscription.unsubscribe.assert_awaited_once()
        self.assertEqual(manager._subscriptions, set())

    async def test_create_subscription_stops_processor(self) -> None:
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
        captured_callbacks: List[Any] = []

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
        subscription_holder: List[ContextManagerCreateContextSubscriber] = []

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
        processors: List[Any] = []

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


class TestUseTaskLifecycle(IsolatedAsyncioTestCase):
    async def test_remember_task_forgets_completed_task(self) -> None:
        """Release completed background tasks before manager shutdown."""
        manager = object.__new__(ContextManagerUse)
        manager._closing = False
        manager.tasks = []
        manager._tasks_lock = threading.Lock()

        async def complete() -> None:
            return

        task = asyncio.create_task(complete())
        manager.remember_task(task)
        await task
        for _ in range(3):
            if not manager.tasks:
                break
            await asyncio.sleep(0)

        self.assertEqual(manager.tasks, [])

    async def test_remember_task_observes_failures(self) -> None:
        """Consume a background failure before its task is discarded."""
        manager = object.__new__(ContextManagerUse)
        manager._closing = False
        manager.tasks = []
        manager._tasks_lock = threading.Lock()

        async def fail() -> None:
            raise RuntimeError("background task failed")

        with patch("dtps.ergo_use.logger.error") as log_error:
            task = asyncio.create_task(fail())
            manager.remember_task(task)
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        self.assertEqual(manager.tasks, [])
        log_error.assert_called_once()
        self.assertIn(
            "DTPS background task failed",
            log_error.call_args.args[0],
        )

    async def test_task_shutdown_cancels_foreign_loop_task(self) -> None:
        """Schedule cancellation without awaiting a foreign-loop task."""
        foreign_loop = Mock()
        cancel_task = Mock()
        foreign_task = Mock()
        foreign_task.cancel = cancel_task
        foreign_task.get_loop.return_value = foreign_loop
        manager = object.__new__(ContextManagerUse)
        manager.tasks = [foreign_task]
        manager._tasks_lock = threading.Lock()

        await manager._cancel_and_wait_for_tasks()

        foreign_loop.call_soon_threadsafe.assert_called_once_with(cancel_task)
        cancel_task.assert_not_called()
        self.assertEqual(manager.tasks, [])

    async def test_remember_task_cancels_late_registration(self) -> None:
        """Cancel a task registered after use-side shutdown begins."""
        task_started = asyncio.Event()
        task_cancelled = asyncio.Event()
        manager = object.__new__(ContextManagerUse)
        manager._closing = True
        manager.tasks = []
        manager._tasks_lock = threading.Lock()

        async def pending() -> None:
            task_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                task_cancelled.set()
                raise

        task = asyncio.create_task(pending())
        try:
            await task_started.wait()
            manager.remember_task(task)
            await asyncio.wait_for(task_cancelled.wait(), timeout=1)
            await asyncio.gather(task, return_exceptions=True)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        self.assertTrue(task.done())
        for _ in range(3):
            if not manager.tasks:
                break
            await asyncio.sleep(0)
        self.assertEqual(manager.tasks, [])

    async def test_use_manager_unsubscribes_tracked_subscriptions(
        self,
    ) -> None:
        """Stop normal subscriptions owned by the use manager."""
        subscription = Mock()
        subscription.unsubscribe = AsyncMock()
        manager = object.__new__(ContextManagerUse)
        manager._tasks_lock = threading.Lock()
        manager._subscriptions = {subscription}

        await manager._unsubscribe_subscriptions()

        subscription.unsubscribe.assert_awaited_once()
        self.assertEqual(manager._subscriptions, set())

    async def test_use_subscription_tracks_processor_task(self) -> None:
        """Register the callback processor for manager shutdown."""
        recorded_tasks: List[asyncio.Task[None]] = []
        listener = Mock()
        listener.stop = AsyncMock()
        client = Mock()
        client.listen_url = AsyncMock(return_value=listener)
        manager = Mock()
        manager.client = client
        manager.remember_task.side_effect = recorded_tasks.append
        context = object.__new__(ContextManagerUseContext)
        context.master = manager

        async def on_data(_raw_data: RawData) -> None:
            return

        try:
            with patch.object(
                ContextManagerUseContext,
                "_get_best_url",
                new=AsyncMock(return_value="url"),
            ):
                await context.subscribe_once(on_data)
            self.assertEqual(len(recorded_tasks), 1)
            self.assertIsInstance(recorded_tasks[0], asyncio.Task)
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
        """Close a processor coroutine when its task cannot start."""
        client = Mock()
        manager = Mock()
        manager.client = client
        context = object.__new__(ContextManagerUseContext)
        context.master = manager
        processors: List[Any] = []

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
        """Cancel the normal callback worker on unsubscribe."""
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
        subscription_holder: List[ContextManagerUseSubscription] = []

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
        """Do not retain a worker when listener setup raises."""
        recorded_tasks: List[asyncio.Task[None]] = []
        client = Mock()
        client.listen_url = AsyncMock(side_effect=RuntimeError("listen failed"))
        manager = Mock()
        manager.client = client
        manager.remember_task.side_effect = recorded_tasks.append
        context = object.__new__(ContextManagerUseContext)
        context.master = manager

        async def on_data(_raw_data: RawData) -> None:
            return

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
        """Do not return a listener created after shutdown starts."""
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

    async def test_publisher_registers_push_task(self) -> None:
        """Make the use manager own the persistent publisher task."""
        client = Mock()
        manager = Mock()
        manager.client = client
        context = Mock()
        context._get_best_url = AsyncMock(return_value="url")

        async def push() -> None:
            await asyncio.Event().wait()

        task = asyncio.create_task(push())
        client.push_continuous = AsyncMock(return_value=task)
        publisher = ContextManagerUseContextPublisher(context, manager)
        try:
            await publisher.init()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        manager.remember_task.assert_called_once_with(task)

    async def test_publisher_terminate_waits_for_task(self) -> None:
        """Complete persistent-publisher cancellation before returning."""
        task_started = asyncio.Event()

        async def push() -> None:
            task_started.set()
            await asyncio.Event().wait()

        publisher = object.__new__(ContextManagerUseContextPublisher)
        task = asyncio.create_task(push())
        publisher.task_push = task
        try:
            await task_started.wait()
            await publisher.terminate()
            self.assertTrue(task.done())
            self.assertTrue(task.cancelled())
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
