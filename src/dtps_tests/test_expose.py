"""Expose test."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING
from unittest import IsolatedAsyncioTestCase

from dtps import AbstractDTPSContext, context_cleanup
from dtps_http import (
    MIME_TEXT,
    DTPSServer,
    RawData,
    app_start,
    async_error_catcher,
    check_is_unix_socket,
    make_http_unix_url,
    url_to_string,
)
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger
from dtps_tests.utils import create_use_pair


class TestExpose(IsolatedAsyncioTestCase):
    """Expose test."""

    @test_timeout(20)
    @async_error_catcher
    async def test_expose(self) -> None:
        """Run expose test."""
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory)
            expose_switchboard_path = path / "expose-switchboard"
            socket_switchboard = expose_switchboard_path.as_posix()
            expose_node_path = path / "expose-node"
            socket_node = expose_node_path.as_posix()
            url_switchboard = make_http_unix_url(socket_switchboard)
            url_node = make_http_unix_url(socket_node)
            url_node_string = url_to_string(url_node)
            url_switchboard_string = url_to_string(url_switchboard)
            logger.info("switchboard: %s", url_switchboard)
            logger.info("node: %s", url_node)
            server = DTPSServer.create(nickname="switchboard")
            switchboard = await app_start(
                server,
                unix_paths=[socket_switchboard],
            )
            async with switchboard:
                environment = {
                    "DTPS_BASE_EXPOSENODE": f"create:{url_node_string}",
                    "DTPS_BASE_EXPOSESWITCHBOARD": f"{url_switchboard_string}",
                }
                logger.info("environment: %s", environment)
                async with context_cleanup(
                    "exposenode",
                    environment,
                ) as context_self:
                    check_is_unix_socket(socket_node)
                    async with context_cleanup(
                        "exposeswitchboard",
                        environment,
                    ) as context_switchboard:
                        out = context_self / "out"
                        await out.queue_create()
                        raw_data = RawData(
                            content=b"hello", content_type=MIME_TEXT
                        )
                        await out.publish(raw_data)
                        mountpoint = (
                            context_switchboard / "dtps" / "node" / "nodename"
                        )
                        await mountpoint.expose(context_self)
                        await asyncio.sleep(2)
                        out_mounted = mountpoint / "out"
                        found = await out_mounted.data_get()
                        if found.content != b"hello":
                            message = "Unexpected content."
                            raise Exception(message)
                        logger.debug("Okay, received.")
                    logger.debug("switchboard use context cleaned.")
                logger.debug("self context cleaned.")
            logger.debug("switchboard server cleaned.")

    @test_timeout(20)
    @async_error_catcher
    async def test_expose_correct_node_id(self) -> None:
        """Run expose correct node ID test."""
        async with (
            create_use_pair("expose2a") as (_, context_a_remote),
            create_use_pair("expose2b") as (context_b_local, context_b_remote),
        ):
            b_node_id = await context_b_remote.get_node_id()
            b_node_id2 = await context_b_local.get_node_id()
            if b_node_id != b_node_id2:
                raise AssertionError
            topic = "my/topic"
            b_topic = context_b_local / topic
            await b_topic.queue_create()
            if await b_topic.get_node_id() != b_node_id:
                raise AssertionError
            mountpoint = "mnt/b"
            b_mounted = context_a_remote / mountpoint
            await b_mounted.expose(context_b_remote)
            await asyncio.sleep(2)
            mounted_topic = b_mounted / topic
            node_id = await mounted_topic.get_node_id()
            if b_node_id != node_id:
                raise AssertionError
            node_id = await b_mounted.get_node_id()
            if b_node_id != node_id:
                raise AssertionError

    @test_timeout(20)
    @async_error_catcher
    async def test_expose3_events(self) -> None:
        """Run third expose3 events test.

        Reads events from a topic mounted on a remote node.
        """
        async with (
            create_use_pair("expose3b") as (_, context_b_remote),
            create_use_pair("expose3a") as (_, context_a_remote),
        ):
            topic = "my/topic"
            b_topic = context_b_remote / topic
            await b_topic.queue_create()

            mountpoint = "mnt/b"
            b_mounted = context_a_remote / mountpoint
            await b_mounted.expose(context_b_remote)
            await asyncio.sleep(2)

            b_topic_mounted = b_mounted / topic
            received_proxy: list[RawData] = []
            received_direct: list[RawData] = []
            sent: list[RawData] = []

            async def on_received_proxy(rec: RawData):
                logger.info(f"proxy: {rec}")
                received_proxy.append(rec)

            async def on_received_direct(rec: RawData):
                logger.info(f"direct: {rec}")
                received_direct.append(rec)

            sub1 = await b_topic_mounted.subscribe(on_received_proxy)
            sub2 = await b_topic.subscribe(on_received_direct)

            for i in range(10):
                raw_data = RawData.json_from_native_object({"count": i})
                await b_topic.publish(raw_data)
                sent.append(raw_data)
                await asyncio.sleep(0.1)

            await asyncio.sleep(5)
            if received_direct != sent:
                raise AssertionError
            if received_proxy != sent:
                raise AssertionError
            await sub1.unsubscribe()
            await sub2.unsubscribe()

    @test_timeout(20)
    @async_error_catcher
    async def test_forwarded_websocket_offline(self) -> None:
        """Run forwarded websocket offline test."""
        await self.check_forwarded_websocket_(inline=False)

    @test_timeout(20)
    @async_error_catcher
    async def test_forwarded_websocket_inline(self) -> None:
        """Run forwarded websocket inline test."""
        await self.check_forwarded_websocket_(inline=True)

    async def check_forwarded_websocket_(self, inline: bool) -> None:
        """Run forwarded websocket check."""
        async with get_exposed_topic("expose3") as exposed:
            # subscribe to the topic
            received: list[RawData] = []
            sent: list[RawData] = []

            async def on_received(rec: RawData) -> None:
                logger.info(f"direct: {rec}")
                received.append(rec)

            sub = await exposed.mounted.subscribe(on_received, inline=inline)
            await asyncio.sleep(2)
            logger.info(f"subscribed: {sub}")
            # publish to the topic
            for i in range(10):
                raw_data = RawData.json_from_native_object({"count": i})
                await exposed.local.publish(raw_data)
                logger.info(f"published: {raw_data}")
                sent.append(raw_data)

            await asyncio.sleep(3)

            logger.info(f"sent: {sent}")
            logger.info(f"received: {received}")
            # check that the messages were received
            if received != sent:
                raise AssertionError

    @test_timeout(20)
    @async_error_catcher
    async def test_forwarded_patch(self) -> None:
        """Run forwarded patch test."""
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

            if found != expected_rd:
                raise AssertionError


@dataclass
class ExposedSetup:
    """Exposed setup."""

    local: AbstractDTPSContext
    mounted: AbstractDTPSContext


if TYPE_CHECKING:

    def get_exposed_topic(
        name: str,
    ) -> AbstractAsyncContextManager[ExposedSetup]:
        """Return exposed topic."""

else:

    @asynccontextmanager
    async def get_exposed_topic(name: str) -> AsyncIterator["ExposedSetup"]:
        """Return exposed topic."""
        async with (
            create_use_pair(f"{name}a") as (_, context_a_remote),
            create_use_pair(f"{name}b") as (context_b_local, context_b_remote),
        ):
            b_node_id = await context_b_remote.get_node_id()
            b_node_id2 = await context_b_local.get_node_id()
            if b_node_id != b_node_id2:
                raise AssertionError
            topic = "my/topic"
            b_topic = context_b_local / topic
            await b_topic.queue_create()
            mountpoint = "mnt/b"
            b_mounted = context_a_remote / mountpoint
            await b_mounted.expose(context_b_remote)
            await asyncio.sleep(2)
            b_mounted_topic = b_mounted / topic
            yield ExposedSetup(local=b_topic, mounted=b_mounted_topic)
