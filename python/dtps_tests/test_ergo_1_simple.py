import asyncio
from unittest import IsolatedAsyncioTestCase

from dtps import DTPSContext
from dtps_http import async_error_catcher, MIME_TEXT, RawData
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger
from dtps_tests.utils import create_use_pair


async def check_ergo_simple(base: DTPSContext, inline: bool, send_before: bool) -> None:
    node_input = await (base / "dtps" / "node" / "in").queue_create()
    node_input = base.navigate("dtps/node/in")

    rd = RawData(content=b"hello", content_type=MIME_TEXT)

    if send_before:
        # Send before subscribing
        await node_input.publish(rd)

    @async_error_catcher
    async def on_input(data: RawData, /) -> None:
        assert data == rd
        event.set()

    event = asyncio.Event()

    sub1 = await node_input.subscribe(on_input, inline=inline)

    await asyncio.sleep(1)

    if not send_before:
        # Send after subscribing
        await node_input.publish(rd)

    logger.debug("Now waiting for the data to pass through" + "\n" * 4)

    await event.wait()

    rd2 = await node_input.data_get()

    assert rd2 == rd


class TestErgoSimple(IsolatedAsyncioTestCase):
    @test_timeout(5)
    async def test_ergo_simple__create__inline__before(self):
        async with create_use_pair("testcreate") as (context_create, context_use):
            await check_ergo_simple(context_create, inline=True, send_before=True)

    @test_timeout(5)
    async def test_ergo_simple__create__offline_before(self):
        async with create_use_pair("testcreate") as (context_create, context_use):
            await check_ergo_simple(context_create, inline=False, send_before=True)

    @test_timeout(5)
    async def test_ergo_simple__use__inline_before(self):
        # create a server
        async with create_use_pair("testuse") as (context_create, context_use):
            await check_ergo_simple(context_use, inline=True, send_before=True)

    @test_timeout(5)
    async def test_ergo_simple__use__offline_before(self):
        # create a server
        async with create_use_pair("testuse") as (context_create, context_use):
            await check_ergo_simple(context_use, inline=False, send_before=True)

    @test_timeout(5)
    async def test_ergo_simple__create__inline__after(self):
        async with create_use_pair("testcreate") as (context_create, context_use):
            await check_ergo_simple(context_create, inline=True, send_before=False)

    @test_timeout(5)
    async def test_ergo_simple__create__offline_after(self):
        async with create_use_pair("testcreate") as (context_create, context_use):
            await check_ergo_simple(context_create, inline=False, send_before=False)

    @test_timeout(5)
    async def test_ergo_simple__use__inline_after(self):
        # create a server
        async with create_use_pair("testuse") as (context_create, context_use):
            await check_ergo_simple(context_use, inline=True, send_before=False)

    @test_timeout(5)
    async def test_ergo_simple__use__offline_after(self):
        # create a server
        async with create_use_pair("testuse") as (context_create, context_use):
            await check_ergo_simple(context_use, inline=False, send_before=False)
