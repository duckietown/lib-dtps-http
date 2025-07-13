"""Expensive callback test."""

import asyncio
from unittest import IsolatedAsyncioTestCase

from dtps import process_low_data_size_last_recent
from dtps_http import RawData, async_error_catcher
from dtps_http_tests.utils import test_timeout
from dtps_tests.utils import create_use_pair


async def expensive_callback(_: RawData) -> None:
    """Run expensive callback."""
    # simulate expensive callback
    await asyncio.sleep(1)


class TestExpensiveCallback(IsolatedAsyncioTestCase):
    """Expensive callback test."""

    @staticmethod
    @test_timeout(20)
    @async_error_catcher
    async def test_expensive() -> None:
        """Run expensive test."""
        async with create_use_pair("call1") as (create, _):
            topic = create / "my_topic"
            await topic.queue_create()
            subscription = await process_low_data_size_last_recent(
                topic,
                expensive_callback,
            )
            try:
                for i in range(20):
                    raw_data = RawData.cbor_from_native_object(
                        {
                            "a": i,
                        },
                    )
                    await topic.publish(raw_data)
                    await asyncio.sleep(0.1)
            finally:
                await subscription.unsubscribe()
            await asyncio.sleep(1)
