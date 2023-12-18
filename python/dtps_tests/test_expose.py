import asyncio
import os
import tempfile
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncContextManager, AsyncIterator, Optional, TYPE_CHECKING
from unittest import IsolatedAsyncioTestCase

from dtps import context_cleanup, DTPSContext
from dtps_http import (
    app_start,
    async_error_catcher,
    check_is_unix_socket,
    DTPSServer,
    make_http_unix_url,
    MIME_TEXT,
    RawData,
    url_to_string,
)
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger
from .utils import create_use_pair


class TestExpose(IsolatedAsyncioTestCase):
    @test_timeout(20)
    @async_error_catcher
    async def test_expose(self):
        with tempfile.TemporaryDirectory() as td:
            socket_switchboard = os.path.join(td, "expose-switchboard")
            socket_node = os.path.join(td, "expose-node")

            url_switchboard = make_http_unix_url(socket_switchboard)
            url_node = make_http_unix_url(socket_node)
            url_node_s = url_to_string(url_node)
            url_switchboard_s = url_to_string(url_switchboard)
            logger.info(f"switchboard: {url_switchboard}")
            logger.info(f"node: {url_node}")

            switchboard = await app_start(
                DTPSServer.create(nickname="switchboard"),
                unix_paths=[socket_switchboard],
            )

            async with switchboard:
                environment = {
                    "DTPS_BASE_EXPOSENODE": f"create:{url_node_s}",
                    "DTPS_BASE_EXPOSESWITCHBOARD": f"{url_switchboard_s}",
                }
                logger.info(f"environment: {environment}")
                async with context_cleanup("exposenode", environment) as context_self:
                    check_is_unix_socket(socket_node)

                    async with context_cleanup("exposeswitchboard", environment) as context_switchboard:
                        out = context_self / "out"
                        await out.queue_create()
                        rd = RawData(content=b"hello", content_type=MIME_TEXT)
                        await out.publish(rd)
                        mountpoint = context_switchboard / "dtps" / "node" / "nodename"
                        await mountpoint.expose(context_self)
                        await asyncio.sleep(2)

                        out_mounted = mountpoint / "out"

                        found = await out_mounted.data_get()
                        if found.content != b"hello":
                            raise Exception("unexpected content")

                        logger.debug("ok, received")
                    logger.debug("switchboard use context cleaned")
                logger.debug("self contexst cleaned")

            logger.debug("switchboard server cleaned")

    @test_timeout(20)
    @async_error_catcher
    async def test_expose_correct_node_id(self):
        async with create_use_pair("expose2a") as (context_a_local, context_a_remote):
            async with create_use_pair("expose2b") as (context_b_local, context_b_remote):
                b_node_id = await context_b_remote.get_node_id()
                b_node_id2 = await context_b_local.get_node_id()
                self.assertEqual(b_node_id, b_node_id2)

                topic = "my/topic"
                b_topic = await (context_b_local / topic).queue_create()

                self.assertEqual(await b_topic.get_node_id(), b_node_id)

                mountpoint = "mnt/b"
                b_mounted = context_a_remote / mountpoint
                await b_mounted.expose(context_b_remote)
                await asyncio.sleep(2)

                b_node_id_of_mounted_topic = await (b_mounted / topic).get_node_id()

                self.assertEqual(b_node_id, b_node_id_of_mounted_topic)

                b_node_id_of_mounted_root = await b_mounted.get_node_id()

                self.assertEqual(b_node_id, b_node_id_of_mounted_root)

    @test_timeout(20)
    @async_error_catcher
    async def test_expose3_events(self):
        """Reads events from a topic mounted on a remote node."""
        async with create_use_pair("expose3b") as (context_b_local, context_b_remote):
            async with create_use_pair("expose3a") as (context_a_local, context_a_remote):
                topic = "my/topic"
                b_topic = await (context_b_remote / topic).queue_create()

                mountpoint = "mnt/b"
                b_mounted = context_a_remote / mountpoint
                await b_mounted.expose(context_b_remote)
                await asyncio.sleep(2)

                b_topic_mounted = b_mounted / topic
                received_proxy = []
                received_direct = []
                sent = []

                async def on_received_proxy(rec: RawData):
                    logger.info(f"proxy: {rec}")
                    received_proxy.append(rec)

                async def on_received_direct(rec: RawData):
                    logger.info(f"direct: {rec}")
                    received_direct.append(rec)

                sub1 = await b_topic_mounted.subscribe(on_received_proxy)
                sub2 = await b_topic.subscribe(on_received_direct)

                for i in range(10):
                    rd = RawData.json_from_native_object({"count": i})
                    await b_topic.publish(rd)
                    sent.append(rd)
                    await asyncio.sleep(0.1)

                await asyncio.sleep(5)
                self.assertEqual(received_direct, sent)
                self.assertEqual(received_proxy, sent)
                await sub1.unsubscribe()
                await sub2.unsubscribe()

    @test_timeout(20)
    @async_error_catcher
    async def test_forwarded_websocket_offline(self):
        await self.check_forwarded_websocket_(max_frequency=100)

    @test_timeout(20)
    @async_error_catcher
    async def test_forwarded_websocket_inline(self):
        await self.check_forwarded_websocket_(max_frequency=None)

    async def check_forwarded_websocket_(self, max_frequency: Optional[float] = None):
        async with get_exposed_topic("expose3") as exposed:
            # subscribe to the topic
            received = []
            sent = []

            async def on_received(rec: RawData):
                logger.info(f"direct: {rec}")
                received.append(rec)

            sub = await exposed.mounted.subscribe(on_received, max_frequency=max_frequency)
            await asyncio.sleep(2)
            logger.info(f"subscribed: {sub}")
            # publish to the topic
            for i in range(10):
                rd = RawData.json_from_native_object({"count": i})
                await exposed.local.publish(rd)
                logger.info(f"published: {rd}")
                sent.append(rd)

            await asyncio.sleep(3)

            # check that the messages were received
            self.assertEqual(received, sent)

    @test_timeout(20)
    @async_error_catcher
    async def test_forwarded_patch(self) -> None:
        async with get_exposed_topic("fpatch") as exposed:
            original = {"a": 1, "b": 2}
            original_rd = RawData.json_from_native_object(original)
            await exposed.mounted.publish(original_rd)

            expected = {"a": 1, "b": 3}
            expected_rd = RawData.json_from_native_object(expected)

            patch = [
                {"op": "replace", "path": "/b", "value": 3},
            ]

            await exposed.mounted.patch(patch)

            found = await exposed.local.data_get()

            self.assertEqual(found, expected_rd)

    # @test_timeout(20)
    # @async_error_catcher
    # async def test_forwarded_call(self):
    #     async with get_exposed_topic("fpatch") as exposed:
    #
    #         request = RawData(content=b"hi", content_type=MIME_TEXT)
    #         response = RawData(content=b"hello", content_type=MIME_TEXT)
    #
    #         async def rpc_handler(otc: RawData) -> Union[RawData, TransformError]:
    #             self.assertEqual(otc, request)
    #             return response
    #
    #         # self.assertEqual(await exposed.local.exists(), False)
    #         # await exposed.local.queue_create(transform=rpc_handler)
    #         # self.assertEqual(await exposed.local.exists(), True)
    #
    #         result1 = await exposed.local.call(request)
    #         if result1 != response:
    #             raise Exception("unexpected content")
    #
    #         logger.info(f"rpc_call: {exposed.mounted}")
    #         logger.info(f"rpc_listen: {exposed.local}")
    #         self.assertEqual(await exposed.mounted.exists(), True)
    #         result = await exposed.mounted.call(request)
    #
    #         if result != response:
    #             raise Exception("unexpected content")
    #


@dataclass
class ExposedSetup:
    local: DTPSContext
    mounted: DTPSContext


if TYPE_CHECKING:

    def get_exposed_topic(name: str) -> AsyncContextManager[ExposedSetup]:
        ...

else:

    @asynccontextmanager
    async def get_exposed_topic(name: str) -> AsyncIterator[ExposedSetup]:
        async with create_use_pair(f"{name}a") as (context_a_local, context_a_remote):
            async with create_use_pair(f"{name}b") as (context_b_local, context_b_remote):
                b_node_id = await context_b_remote.get_node_id()
                b_node_id2 = await context_b_local.get_node_id()
                assert b_node_id == b_node_id2

                topic = "my/topic"
                b_topic = await (context_b_local / topic).queue_create()

                mountpoint = "mnt/b"
                b_mounted = context_a_remote / mountpoint
                await b_mounted.expose(context_b_remote)
                await asyncio.sleep(2)

                b_mounted_topic = b_mounted / topic

                yield ExposedSetup(local=b_topic, mounted=b_mounted_topic)
