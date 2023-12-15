import asyncio
from typing import Optional
from unittest import IsolatedAsyncioTestCase

from dtps import DTPSContext
from dtps_http import MIME_TEXT, RawData
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger
from dtps_tests.utils import create_use_pair


async def check_ergo_simple(base: DTPSContext, max_frequency: Optional[float]) -> None:
    node_input = await (base / "dtps" / "node" / "in").queue_create()

    rd = RawData(content=b"hello", content_type=MIME_TEXT)

    async def on_input(data: RawData, /) -> None:
        assert data == rd
        event.set()

    event = asyncio.Event()

    sub1 = await node_input.subscribe(on_input, max_frequency=max_frequency)

    await node_input.publish(rd)

    logger.debug("Now waiting for the data to pass through" + "\n" * 4)

    await event.wait()

    rd2 = await node_input.data_get()

    assert rd2 == rd


class TestErgoSimple(IsolatedAsyncioTestCase):
    @test_timeout(5)
    async def test_ergo_simple_create_inline(self):
        async with create_use_pair("testcreate") as (context_create, context_use):
            await check_ergo_simple(context_create, max_frequency=None)

    @test_timeout(5)
    async def test_ergo_simple__create_offline(self):
        async with create_use_pair("testcreate") as (context_create, context_use):
            await check_ergo_simple(context_create, max_frequency=100)

    @test_timeout(5)
    async def test_ergo_simple__use_inline(self):
        # create a server
        async with create_use_pair("testuse") as (context_create, context_use):
            await check_ergo_simple(context_use, max_frequency=None)

    @test_timeout(5)
    async def test_ergo_simple__use_offline(self):
        # create a server
        async with create_use_pair("testuse") as (context_create, context_use):
            await check_ergo_simple(context_use, max_frequency=100)
