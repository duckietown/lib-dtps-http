import asyncio
import time
from unittest import IsolatedAsyncioTestCase

from dtps import DTPSContext, process_lowdatasize_last_recent
from dtps_http import (
    async_error_catcher,
    RawData,
)
from dtps_http_tests.utils import test_timeout
from .utils import create_use_pair


class TestExpensiveCallback(IsolatedAsyncioTestCase):
    @test_timeout(20)
    @async_error_catcher
    async def test_expensive_one(self) -> None:
        async with create_use_pair("call1") as (create, _use):
            topic: DTPSContext = await (create / "my_topic").queue_create()

            async def expensive_callback(_: RawData) -> None:
                # simulate expensive callback
                await asyncio.sleep(1)

                # note that if the IO is blocking then things should
                # be done in a different process/thread!
                # time.sleep(1)

            sub = await process_lowdatasize_last_recent(topic, expensive_callback)

            try:

                for i in range(20):
                    rdi = RawData.cbor_from_native_object({"a": i})
                    await topic.publish(rdi)
                    await asyncio.sleep(0.1)

            finally:
                await sub.unsubscribe()

            await asyncio.sleep(1)
