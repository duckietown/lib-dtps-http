import asyncio
import os
import tempfile
from unittest import IsolatedAsyncioTestCase

from dtps import context_cleanup, DTPSContext
from dtps_http import app_start, DTPSServer, make_http_unix_url, MIME_TEXT, RawData
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger


async def go(base: DTPSContext) -> None:
    node_input = await (base / "dtps" / "node" / "in").queue_create()
    node_output = await (base / "dtps" / "node" / "out").queue_create()

    publisher = await node_output.publisher()

    async def on_input(data: RawData, /) -> None:
        await publisher.publish(data)

    event = asyncio.Event()

    async def on_output(_: RawData, /) -> None:
        event.set()

    sub1 = await node_input.subscribe(on_input)
    sub2 = await node_output.subscribe(on_output)

    rd = RawData(content=b"hello", content_type=MIME_TEXT)

    # await asyncio.sleep(1)
    await node_input.publish(rd)
    await node_input.data_get()

    logger.debug("Now waiting for the data to pass through")
    await event.wait()
    found = await node_output.data_get()
    if found.content != b"hello":
        raise Exception("unexpected content")

    await publisher.terminate()
    await sub1.unsubscribe()
    await sub2.unsubscribe()


class TestCreate(IsolatedAsyncioTestCase):
    @test_timeout(10)
    async def test_create(self):
        with tempfile.TemporaryDirectory() as td:
            socket = os.path.join(td, "socket")
            url_switchboard = make_http_unix_url(socket)

            environment = {"DTPS_BASE_SELF": f"create:{url_switchboard}"}
            async with context_cleanup("self", environment) as c:
                await go(c)


class TestUse(IsolatedAsyncioTestCase):
    @test_timeout(10)
    async def test_use(self):
        # create a server
        with tempfile.TemporaryDirectory() as td:
            socket = os.path.join(td, "socket")
            url = make_http_unix_url(socket)

            dtps_server = DTPSServer.create()

            a = await app_start(
                dtps_server,
                unix_paths=[socket],
            )
            async with a:
                # await asyncio.sleep(10)

                environment = {"DTPS_BASE_SELF": f"{url}"}
                async with context_cleanup("self", environment) as c:
                    await go(c)
                    # logger.debug("test down, cleaning up")
