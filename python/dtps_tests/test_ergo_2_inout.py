import asyncio
from typing import Optional
from unittest import IsolatedAsyncioTestCase

from dtps import DTPSContext
from dtps_http import MIME_TEXT, RawData
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger
from dtps_tests.utils import create_use_pair


async def go(base: DTPSContext, max_frequency: Optional[float]) -> None:
    node_input = await (base / "dtps" / "node" / "in").queue_create()
    node_output = await (base / "dtps" / "node" / "out").queue_create()

    publisher = await node_output.publisher()

    async def on_input(data: RawData, /) -> None:
        logger.debug("Got data: " + str(data))
        await publisher.publish(data)

    event = asyncio.Event()

    async def on_output(_: RawData, /) -> None:
        event.set()

    sub1 = await node_input.subscribe(on_input, max_frequency=max_frequency)
    sub2 = await node_output.subscribe(on_output, max_frequency=max_frequency)

    rd = RawData(content=b"hello", content_type=MIME_TEXT)

    await node_input.publish(rd)
    rd2 = await node_input.data_get()

    assert rd2 == rd

    logger.debug("Now waiting for the data to pass through" + "\n" * 4)

    await event.wait()
    found = await node_output.data_get()
    if found.content != b"hello":
        raise Exception("unexpected content")

    await publisher.terminate()
    await sub1.unsubscribe()
    await sub2.unsubscribe()


class TestCreate(IsolatedAsyncioTestCase):
    @test_timeout(5)
    async def test_create_inline(self):
        async with create_use_pair("testcreate") as (context_create, context_use):
            await go(context_create, max_frequency=None)

    @test_timeout(5)
    async def test_create_offline(self):
        async with create_use_pair("testcreate") as (context_create, context_use):
            await go(context_create, max_frequency=100)


class TestUse(IsolatedAsyncioTestCase):
    @test_timeout(5)
    async def test_use_inline(self):
        # create a server
        async with create_use_pair("testuse") as (context_create, context_use):
            await go(context_use, max_frequency=None)

    @test_timeout(10)
    async def test_use_offline(self):
        # create a server
        async with create_use_pair("testuse") as (context_create, context_use):
            await go(context_use, max_frequency=100)
