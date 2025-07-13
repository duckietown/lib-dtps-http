"""Simple ergo test."""

import asyncio
from asyncio import Event
from typing import Any
from unittest import IsolatedAsyncioTestCase

from dtps import AbstractDTPSContext
from dtps_http import MIME_TEXT, RawData, async_error_catcher
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger
from dtps_tests.utils import TEST_APP_DATA, create_use_pair


def get_on_input(raw_data: RawData, event: Event) -> Any:
    """Return `on_input`."""

    @async_error_catcher
    async def on_input(data: RawData, /) -> None:
        if data != raw_data:
            raise AssertionError
        event.set()

    return on_input


async def check_ergo_simple(
    base: AbstractDTPSContext,
    *,
    inline: bool,
    send_before: bool,
) -> None:
    """Run simple ergo test."""
    node_input = base / "dtps" / "node" / "in"
    await node_input.queue_create(app_data=TEST_APP_DATA)
    node_input = base.navigate("dtps/node/in")
    raw_data = RawData(content=b"hello", content_type=MIME_TEXT)
    if send_before:
        # Send before subscribing
        await node_input.publish(raw_data)
    event = Event()
    on_input = get_on_input(raw_data, event)
    await node_input.subscribe(on_input, inline=inline)
    await asyncio.sleep(1)
    if not send_before:
        # Send after subscribing
        await node_input.publish(raw_data)
    logger.debug("Waiting for the data to pass through...\n\n\n\n")
    await event.wait()
    raw_data_2 = await node_input.data_get()
    if raw_data_2 != raw_data:
        raise AssertionError
    meta = node_input.meta()
    data = await meta.data_get()
    logger.info("Got %s.", data)


class TestErgoSimple(IsolatedAsyncioTestCase):
    """Simple ergo test."""

    @staticmethod
    @test_timeout(5)
    async def test_ergo_simple_create_inline_before() -> None:
        """Run create-inline-before simple ergo test."""
        async with create_use_pair("testcreate") as (context_create, _):
            await check_ergo_simple(
                context_create,
                inline=True,
                send_before=True,
            )

    @staticmethod
    @test_timeout(5)
    async def test_ergo_simple_create_offline_before() -> None:
        """Run create-offline-before simple ergo test."""
        async with create_use_pair("testcreate") as (context_create, _):
            await check_ergo_simple(
                context_create,
                inline=False,
                send_before=True,
            )

    @staticmethod
    @test_timeout(5)
    async def test_ergo_simple_use_inline_before() -> None:
        """Run use-inline-before simple ergo test."""
        # create a server
        async with create_use_pair("testuse") as (_, context_use):
            await check_ergo_simple(context_use, inline=True, send_before=True)

    @staticmethod
    @test_timeout(5)
    async def test_ergo_simple_use_offline_before() -> None:
        """Run use-offline-before simple ergo test."""
        # create a server
        async with create_use_pair("testuse") as (_, context_use):
            await check_ergo_simple(
                context_use,
                inline=False,
                send_before=True,
            )

    @staticmethod
    @test_timeout(5)
    async def test_ergo_simple_create_inline_after() -> None:
        """Run create-inline-after simple ergo test."""
        async with create_use_pair("testcreate") as (context_create, _):
            await check_ergo_simple(
                context_create,
                inline=True,
                send_before=False,
            )

    @staticmethod
    @test_timeout(5)
    async def test_ergo_simple_create_offline_after() -> None:
        """Run create-offline-after simple ergo test."""
        async with create_use_pair("testcreate") as (context_create, _):
            await check_ergo_simple(
                context_create,
                inline=False,
                send_before=False,
            )

    @staticmethod
    @test_timeout(5)
    async def test_ergo_simple_use_inline_after() -> None:
        """Run use-inline-after simple ergo test."""
        # create a server
        async with create_use_pair("testuse") as (_, context_use):
            await check_ergo_simple(
                context_use,
                inline=True,
                send_before=False,
            )

    @staticmethod
    @test_timeout(5)
    async def test_ergo_simple_use_offline_after() -> None:
        """Run use-offline-after simple ergo test."""
        # create a server
        async with create_use_pair("testuse") as (_, context_use):
            await check_ergo_simple(
                context_use,
                inline=False,
                send_before=False,
            )
