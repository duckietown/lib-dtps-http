import asyncio
from typing import Any, List
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

from dtps import DTPSContext
from dtps.ergo_create import (
    ContextManagerCreate,
    ContextManagerCreateContext,
    ContextManagerCreateContextSubscriber,
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
