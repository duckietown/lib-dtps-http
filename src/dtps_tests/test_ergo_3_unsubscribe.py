"""Unsubscribe ergo test."""

import asyncio
from typing import Any
from unittest import IsolatedAsyncioTestCase

from dtps import AbstractDTPSContext
from dtps_http import MIME_TEXT, RawData, async_error_catcher
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger
from dtps_tests.utils import create_use_pair

EXPECTED_RECEIVED_LENGTH = 2


def get_on_input(received: list[RawData]) -> Any:
    """Return `on_input`."""

    @async_error_catcher
    async def on_input(data: RawData, /) -> None:
        received_length = len(received)
        logger.info("received #%s", received_length)
        received.append(data)

    return on_input


async def check_ergo_unsub(base: AbstractDTPSContext, *, inline: bool) -> None:
    """Run ergo unsubscribe check."""
    node_input = base / "dtps" / "node" / "in"
    await node_input.queue_create()
    raw_data = RawData(content=b"hello", content_type=MIME_TEXT)
    received: list[RawData] = []
    on_input = get_on_input(received)
    sub1 = await node_input.subscribe(on_input, inline=inline)
    await asyncio.sleep(1)
    await node_input.publish(raw_data)
    await node_input.publish(raw_data)
    await asyncio.sleep(2)
    if len(received) != EXPECTED_RECEIVED_LENGTH:
        message = f"Expected {EXPECTED_RECEIVED_LENGTH}."
        raise AssertionError(message)
    await sub1.unsubscribe()
    await node_input.publish(raw_data)
    await node_input.publish(raw_data)
    await asyncio.sleep(2)
    if len(received) != EXPECTED_RECEIVED_LENGTH:
        message = f"Expected {EXPECTED_RECEIVED_LENGTH}."
        raise AssertionError(message)


class TestErgoUnsub(IsolatedAsyncioTestCase):
    """Ergo unsubscribe test."""

    @staticmethod
    @test_timeout(15)
    async def test_ergo_simple_create_inline_before() -> None:
        """Run create-inline-before simple ergo test."""
        async with create_use_pair("testcreate") as (context_create, _):
            await check_ergo_unsub(context_create, inline=True)

    @staticmethod
    @test_timeout(15)
    async def test_ergo_simple_create_offline_before() -> None:
        """Run create-offline-before simple ergo test."""
        async with create_use_pair("testcreate") as (context_create, _):
            await check_ergo_unsub(context_create, inline=False)

    @staticmethod
    @test_timeout(15)
    async def test_ergo_simple_use_inline_before() -> None:
        """Run use-inline-before simple ergo test."""
        # create a server
        async with create_use_pair("testuse") as (context_create, context_use):
            await check_ergo_unsub(context_use, inline=True)

    @staticmethod
    @test_timeout(15)
    async def test_ergo_simple_use_offline_before() -> None:
        """Run use-offline-before simple ergo test."""
        # create a server
        async with create_use_pair("testuse") as (context_create, context_use):
            await check_ergo_unsub(context_use, inline=False)
