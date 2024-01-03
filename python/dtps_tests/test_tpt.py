import asyncio
import unittest
from contextlib import AsyncExitStack
from unittest import IsolatedAsyncioTestCase
from . import logger
from dtps_http import (
    async_error_catcher,
    MIME_TEXT,
    RawData,
)
from dtps_http_tests.utils import test_timeout
from .utils import create_rust_server, create_use_pair


class TestTPT(IsolatedAsyncioTestCase):
    @unittest.expectedFailure
    @test_timeout(120)
    @async_error_catcher
    async def test_connect1(self):
        S = AsyncExitStack()

        async with S:
            switchboard = await S.enter_async_context(create_rust_server("switchboard"))
            _, node1_remote = await S.enter_async_context(create_use_pair("node1"))
            _, node2_remote = await S.enter_async_context(create_use_pair("node2"))

            topic1 = node1_remote / "topic1"
            await topic1.queue_create()

            rd = RawData(content=b"hello", content_type=MIME_TEXT)
            await topic1.publish(rd)

            topic2 = node2_remote / "topic2"
            await topic2.queue_create()

            rd = RawData(content=b"hello", content_type=MIME_TEXT)
            await topic2.publish(rd)

            mounted1 = switchboard / "mounted1"
            await mounted1.expose(topic1)

            logger.info("sleeping 25s to wait for mount to propagate")
            await asyncio.sleep(10)

            # await mounted1.data_get()

            mounted2 = switchboard / "mounted2"
            await mounted2.expose(topic2)

            # await asyncio.sleep(5)
            # await mounted2.data_get()

            await mounted1.connect_to(mounted2)

            await asyncio.sleep(20)
