"""TPT test."""

from asyncio import Event
from contextlib import AsyncExitStack
from typing import Any
from unittest import IsolatedAsyncioTestCase

from dtps_http import MIME_TEXT, RawData, async_error_catcher
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger
from dtps_tests.utils import create_rust_server, create_use_pair


class TestTPT(IsolatedAsyncioTestCase):
    """TPT test."""

    @staticmethod
    def _get_on_topic2(raw_data_expected: RawData, event: Event) -> Any:
        async def on_topic2(raw_data: RawData, /) -> None:
            logger.info("topic2 received %s", raw_data)
            if raw_data != raw_data_expected:
                message = f"{raw_data_expected} != {raw_data}"
                raise AssertionError(message)
            event.set()

        return on_topic2

    @test_timeout(120)
    @async_error_catcher
    async def test_connect1(self) -> None:
        """Run first connect test."""
        async_exit_stack = AsyncExitStack()
        async with async_exit_stack:
            context_manager = create_rust_server("switchboard")
            switchboard = await async_exit_stack.enter_async_context(
                context_manager,
            )
            context_manager = create_use_pair("node1")
            _, node1_remote = await async_exit_stack.enter_async_context(
                context_manager,
            )
            context_manager = create_use_pair("node2")
            _, node2_remote = await async_exit_stack.enter_async_context(
                context_manager,
            )
            topic1 = node1_remote / "topic1"
            await topic1.queue_create()
            raw_data = RawData(
                content=b"first on topic1",
                content_type=MIME_TEXT,
            )
            await topic1.publish(raw_data)
            topic2 = node2_remote / "topic2"
            await topic2.queue_create()
            mounted1 = switchboard / "mounted1"
            await mounted1.expose(topic1)
            mounted2 = switchboard / "mounted2"
            await mounted2.expose(topic2)
            await mounted1.connect_to(mounted2)
            raw_data_expected = RawData(
                content=b"this should go to topic2",
                content_type=MIME_TEXT,
            )
            event = Event()
            on_topic2 = self._get_on_topic2(raw_data_expected, event)
            await topic2.subscribe(on_topic2)
            await topic1.publish(raw_data_expected)
            await event.wait()

    @test_timeout(120)
    @async_error_catcher
    async def test_connect2(self) -> None:
        """Run second connect test."""
        async_exit_stack = AsyncExitStack()
        async with async_exit_stack:
            context_manager = create_rust_server("switchboard")
            switchboard = await async_exit_stack.enter_async_context(
                context_manager,
            )
            context_manager = create_use_pair("node1")
            _, node1_remote = await async_exit_stack.enter_async_context(
                context_manager,
            )
            context_manager = create_use_pair("node2")
            _, node2_remote = await async_exit_stack.enter_async_context(
                context_manager,
            )
            topic1 = node1_remote / "topic1"
            await topic1.queue_create()
            raw_data = RawData(
                content=b"first on topic1",
                content_type=MIME_TEXT,
            )
            await topic1.publish(raw_data)
            topic2 = node2_remote / "topic2"
            await topic2.queue_create()
            mounted1 = switchboard / "mounted1"
            await mounted1.expose(topic1)
            mounted2 = switchboard / "mounted2"
            await mounted2.expose(topic2)
            await mounted1.connect_to(mounted2)
            raw_data_expected = RawData(
                content=b"this should go to topic2",
                content_type=MIME_TEXT,
            )
            event = Event()
            on_topic2 = self._get_on_topic2(raw_data_expected, event)
            await topic2.subscribe(on_topic2)
            await topic1.publish(raw_data_expected)
            await event.wait()
