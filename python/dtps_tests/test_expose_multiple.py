import asyncio
from contextlib import AsyncExitStack
from unittest import IsolatedAsyncioTestCase

from dtps import DTPSContext
from dtps_http import (
    async_error_catcher,
)
from dtps_http_tests.utils import test_timeout
from . import logger
from .utils import create_rust_server, create_use_pair


class TestExposeMultiple(IsolatedAsyncioTestCase):
    @test_timeout(120)
    @async_error_catcher
    async def test_expose_multiple(self):
        S = AsyncExitStack()

        async with S:
            switchboard: DTPSContext = await S.enter_async_context(create_rust_server("switchboard"))
            _, my_context = await S.enter_async_context(create_use_pair("node1"))
            # my_context, _ = await S.enter_async_context(create_use_pair("node1"))

            jpeg = await (my_context / "out" / "jpeg").queue_create()
            parameters = await (my_context / "out" / "parameters").queue_create()

            # rd = RawData(content=b"first on topic1", content_type=MIME_TEXT)
            # await topic1.publish(rd)

            await (switchboard / "nodes" / "camera").expose(my_context)
            await (switchboard / "sensors" / "camera" / "jpeg").expose(jpeg)
            await (switchboard / "sensors" / "camera" / "parameters").expose(parameters)

            await asyncio.sleep(2)

            data = await switchboard.data_get()
            data = data.get_as_native_object()
            topics = list(data["topics"])  # type: ignore
            expected = [
                "nodes/camera/out/jpeg",
                "nodes/camera/out/parameters",
                "sensors/camera/jpeg",
                "sensors/camera/parameters",
            ]

            topics_s = "".join([f"{topic!r}\n" for topic in sorted(topics)])
            logger.info(f"topics:\n{topics_s}")

            for topic in expected:
                if topic not in topics:
                    raise AssertionError(f"{topic} not in {topics}")

            await asyncio.sleep(2)
