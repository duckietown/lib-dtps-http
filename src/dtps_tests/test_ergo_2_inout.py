"""In-out ergo test."""

from asyncio import Event
from typing import Any
from unittest import IsolatedAsyncioTestCase

from dtps import AbstractDTPSContext, AbstractPublisherInterface
from dtps_http import MIME_TEXT, RawData
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger
from dtps_tests.utils import create_use_pair


def get_on_input(publisher: AbstractPublisherInterface) -> Any:
    """Return `on_input`."""

    async def on_input(data: RawData, /) -> None:
        logger.debug("Got data: %s", data)
        await publisher.publish(data)

    return on_input


def get_on_output(event: Event) -> Any:
    """Return `on_output`."""

    async def on_output(_: RawData, /) -> None:
        event.set()

    return on_output


async def go(base: AbstractDTPSContext, *, inline: bool) -> None:
    """Go."""
    node_input = base / "dtps" / "node" / "in"
    node_output = base / "dtps" / "node" / "out"
    await node_input.queue_create()
    await node_output.queue_create()
    publisher = await node_output.publisher()
    event = Event()
    on_input = get_on_input(publisher)
    on_output = get_on_output(event)
    sub1 = await node_input.subscribe(on_input, inline=inline)
    sub2 = await node_output.subscribe(on_output, inline=inline)
    raw_data = RawData(content=b"hello", content_type=MIME_TEXT)
    await node_input.publish(raw_data)
    raw_data_2 = await node_input.data_get()
    if raw_data_2 != raw_data:
        raise AssertionError
    logger.debug("Waiting for the data to pass through...\n\n\n\n")
    await event.wait()
    found = await node_output.data_get()
    if found.content != b"hello":
        message = "Unexpected content."
        raise Exception(message)
    await publisher.terminate()
    await sub1.unsubscribe()
    await sub2.unsubscribe()


class TestCreate(IsolatedAsyncioTestCase):
    """Create test."""

    @staticmethod
    @test_timeout(5)
    async def test_create_inline() -> None:
        """Run create inline test."""
        async with create_use_pair("testcreate") as (context_create, _):
            await go(context_create, inline=True)

    @staticmethod
    @test_timeout(5)
    async def test_create_offline() -> None:
        """Run create offline test."""
        async with create_use_pair("testcreate") as (context_create, _):
            await go(context_create, inline=False)


class TestUse(IsolatedAsyncioTestCase):
    """Use test."""

    @staticmethod
    @test_timeout(5)
    async def test_use_inline() -> None:
        """Run use inline test."""
        # create a server
        async with create_use_pair("testuse") as (context_create, context_use):
            await go(context_use, inline=True)

    @staticmethod
    @test_timeout(10)
    async def test_use_offline() -> None:
        """Run use offline test."""
        # create a server
        async with create_use_pair("testuse") as (context_create, context_use):
            await go(context_use, inline=False)
