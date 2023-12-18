import asyncio
import unittest
from contextlib import AsyncExitStack
from unittest import IsolatedAsyncioTestCase

from dtps_http import (
    async_error_catcher,
)
from dtps_http_tests.utils import test_timeout
from .utils import create_rust_server, create_use_pair


class TestTPT(IsolatedAsyncioTestCase):
    @unittest.expectedFailure
    @test_timeout(20)
    @async_error_catcher
    async def test_connect1(self):
        S = AsyncExitStack()

        async with S:
            switchboard = await S.enter_async_context(create_rust_server("switchboard"))
            node1_local, node1_remote = await S.enter_async_context(create_use_pair("node1"))
            node2_local, node2_remote = await S.enter_async_context(create_use_pair("node2"))

            topic2 = node2_local / "topic2"
            await topic2.queue_create()

            topic1 = node1_local / "topic1"
            await topic1.queue_create()

            mounted1 = switchboard / "mounted1"
            await mounted1.expose(topic1)

            mounted2 = switchboard / "mounted2"
            await mounted2.expose(topic2)

            await mounted1.connect_to(mounted2)

            await asyncio.sleep(10)
