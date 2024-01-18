import asyncio
from contextlib import AsyncExitStack
from unittest import IsolatedAsyncioTestCase

from dtps_http import (
    async_error_catcher,
    MIME_TEXT,
    RawData,
)
from dtps_http_tests.utils import test_timeout
from . import logger
from .utils import create_rust_server, create_use_pair


class TestTPT(IsolatedAsyncioTestCase):
    # @unittest.expectedFailure
    @test_timeout(120)
    @async_error_catcher
    async def test_connect1(self):
        S = AsyncExitStack()

        async with S:
            switchboard = await S.enter_async_context(create_rust_server("switchboard"))
            _, node1_remote = await S.enter_async_context(create_use_pair("node1"))
            _, node2_remote = await S.enter_async_context(create_use_pair("node2"))

            # if use_remote:
            #     node1 = node1_remote
            #     node2 = node2_remote
            # else:
            #     node1 = node1_local
            #     node2 = node2_local
            #
            topic1 = node1_remote / "topic1"
            await topic1.queue_create()

            rd = RawData(content=b"first on topic1", content_type=MIME_TEXT)
            await topic1.publish(rd)

            topic2 = node2_remote / "topic2"
            await topic2.queue_create()

            mounted1 = switchboard / "mounted1"
            await mounted1.expose(topic1)

            mounted2 = switchboard / "mounted2"
            await mounted2.expose(topic2)

            await mounted1.connect_to(mounted2)

            event = asyncio.Event()

            rd_expected = RawData(content=b"this should go to topic2", content_type=MIME_TEXT)

            async def on_topic2(rd_received: RawData, /) -> None:
                logger.info(f"topic2 received {rd_received}")
                if rd_received != rd_expected:
                    raise AssertionError(f"{rd_expected} != {rd_received}")

                event.set()

            await topic2.subscribe(on_topic2)

            await topic1.publish(rd_expected)

            await event.wait()

    @test_timeout(120)
    @async_error_catcher
    async def test_connect2(self):
        S = AsyncExitStack()

        async with S:
            switchboard = await S.enter_async_context(create_rust_server("switchboard"))
            _, node1_remote = await S.enter_async_context(create_use_pair("node1"))
            _, node2_remote = await S.enter_async_context(create_use_pair("node2"))

            # if use_remote:
            #     node1 = node1_remote
            #     node2 = node2_remote
            # else:
            #     node1 = node1_local
            #     node2 = node2_local
            #
            topic1 = node1_remote / "topic1"
            await topic1.queue_create()

            rd = RawData(content=b"first on topic1", content_type=MIME_TEXT)
            await topic1.publish(rd)

            topic2 = node2_remote / "topic2"
            await topic2.queue_create()

            mounted1 = switchboard / "mounted1"
            await mounted1.expose(topic1)

            mounted2 = switchboard / "mounted2"
            await mounted2.expose(topic2)

            await mounted1.connect_to(mounted2)

            event = asyncio.Event()

            rd_expected = RawData(content=b"this should go to topic2", content_type=MIME_TEXT)

            async def on_topic2(rd_received: RawData, /) -> None:
                logger.info(f"topic2 received {rd_received}")
                if rd_received != rd_expected:
                    raise AssertionError(f"{rd_expected} != {rd_received}")

                event.set()

            await topic2.subscribe(on_topic2)

            await topic1.publish(rd_expected)

            await event.wait()
