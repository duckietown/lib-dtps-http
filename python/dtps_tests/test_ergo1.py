import asyncio
from unittest import IsolatedAsyncioTestCase

from dtps import context_cleanup, DTPSContext
from dtps_http import app_start, DTPSServer, MIME_TEXT, RawData
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
    async def test_create(self):
        environment = {"DTPS_BASE_SELF": "create:http://localhost:8001/"}
        async with context_cleanup("self", environment) as c:
            await go(c)


class TestUse(IsolatedAsyncioTestCase):
    async def test_use(self):
        # create a server
        port = 8432
        dtps_server = DTPSServer.create()

        a = await app_start(
            dtps_server,
            tcps=(("localhost", port),),
            unix_paths=[],
            tunnel=None,
        )
        async with a:
            # await asyncio.sleep(10)

            environment = {"DTPS_BASE_SELF": f"http://localhost:{port}/"}
            async with context_cleanup("self", environment) as c:
                await go(c)
                # logger.debug("test down, cleaning up")
