import asyncio
from typing import Optional
from unittest import IsolatedAsyncioTestCase

from dtps import DTPSContext
from dtps_http import async_error_catcher, MIME_TEXT, RawData
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger
from dtps_tests.utils import create_use_pair


async def check_ergo_unsub(base: DTPSContext, inline: bool) -> None:
    node_input = await (base / "dtps" / "node" / "in").queue_create()

    rd = RawData(content=b"hello", content_type=MIME_TEXT)

    received = []

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
