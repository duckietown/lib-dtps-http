import asyncio
import time
from typing import List
from unittest import IsolatedAsyncioTestCase

from dtps import DTPSContext
from dtps_http import (
    async_error_catcher,
    RawData,
)
from dtps_http.utils_every_once_in_a_while import EveryOnceInAWhile
from dtps_http_tests.utils import test_timeout
from . import logger
from .utils import create_rust_server, create_use_pair


class TestMaxFrequency(IsolatedAsyncioTestCase):
    @test_timeout(20)
    @async_error_catcher
    async def test_max_freq_create(self):
        async with create_use_pair("call1") as (create, _):
            await self._testmax(create)

    @test_timeout(20)
    @async_error_catcher
    async def test_max_freq_use(self):
        async with create_use_pair("use") as (_, use):
            await self._testmax(use)

    @test_timeout(20)
    async def test_max_freq_rust(self):
        async with create_rust_server("maxfreq") as use:
            await self._testmax(use)

    async def _testmax(self, root: DTPSContext):
        topic: DTPSContext = await (root / "my_topic").queue_create()
        max_frequency = 3.0
        effective_frequency = 3 * max_frequency
        period_s = 4

        found: List[RawData] = []

        async def collect(rd: RawData) -> None:
            found.append(rd)

        await topic.subscribe(collect, max_frequency=max_frequency)

        publish_dt = 1.0 / effective_frequency
        when = EveryOnceInAWhile(publish_dt)
        t0 = time.time()
        i = 0
        logger.info(f"{publish_dt=}")
        nsent = 0
        while True:
            i += 1
            dt = time.time() - t0
            if dt > period_s:
                break

            if when.now():
                data = {"i": i, "dt": dt}
                await topic.publish(RawData.cbor_from_native_object(data))
                nsent += 1

            await asyncio.sleep(publish_dt)

        await asyncio.sleep(3)

        expected_n = int(period_s * max_frequency)

        too_many = len(found) > expected_n + 3  # allow for some slop
        too_few = len(found) < expected_n - 2
        logger.info(f"nsent: {nsent}")
        logger.info(f"expected: {expected_n}")
        logger.info(f"nfound: {len(found)}")
        if too_many:
            msg = f"Too many messages found: {len(found)}, expected around {expected_n}"
            raise Exception(msg)
        if too_few:
            msg = f"Too few messages found: {len(found)}, expected around {expected_n}"
            raise Exception(msg)

    @test_timeout(20)
    @async_error_catcher
    async def test_max_freq_publisher_local(self):
        async with create_use_pair("use") as (create, _):
            topic: DTPSContext = await (create / "my_topic").queue_create()
            max_frequency1 = 3.0
            max_frequency2 = 13.0

            async def collect(rd: RawData) -> None:
                pass

            async with topic.publisher_context() as publisher:
                listener_info = await publisher.get_listener_info()
                assert listener_info is not None
                logger.info(f"{listener_info=}")
                self.assertEqual(listener_info.num_listeners, 0)

                self.assertEqual(listener_info.max_frequency, None)

                sub1 = await topic.subscribe(collect, max_frequency=max_frequency1)
                sub2 = await topic.subscribe(collect, max_frequency=max_frequency2)

                listener_info = await publisher.get_listener_info()
                assert listener_info is not None
                logger.info(f"{listener_info=}")
                self.assertEqual(listener_info.num_listeners, 2)
                self.assertEqual(listener_info.max_frequency, max(max_frequency1, max_frequency2))

                await sub2.unsubscribe()

                listener_info = await publisher.get_listener_info()
                logger.info(f"{listener_info=}")
                assert listener_info is not None
                self.assertEqual(listener_info.num_listeners, 1)
                self.assertEqual(listener_info.max_frequency, max_frequency1)

                await sub1.unsubscribe()

                listener_info = await publisher.get_listener_info()
                assert listener_info is not None
                logger.info(f"{listener_info=}")
                self.assertEqual(listener_info.num_listeners, 0)

                self.assertEqual(listener_info.max_frequency, None)

    @test_timeout(20)
    @async_error_catcher
    async def test_max_freq_publisher_remote(self):
        async with create_use_pair("use") as (create, use):
            topic: DTPSContext = await (create / "my_topic").queue_create()
            topic_use: DTPSContext = use / "my_topic"
            max_frequency1 = 3.0
            max_frequency2 = 13.0
            delay = 0.2

            async def collect(rd: RawData) -> None:
                pass

            async with topic.publisher_context() as publisher:
                listener_info = await publisher.get_listener_info()
                assert listener_info is not None
                logger.info(f"{listener_info=}")
                self.assertEqual(listener_info.num_listeners, 0)

                self.assertEqual(listener_info.max_frequency, None)

                sub1 = await topic_use.subscribe(collect, max_frequency=max_frequency1)
                sub2 = await topic_use.subscribe(collect, max_frequency=max_frequency2)
                await asyncio.sleep(delay)
                listener_info = await publisher.get_listener_info()
                assert listener_info is not None
                logger.info(f"after sub1, sub2 subscribed: {listener_info=}")
                self.assertEqual(listener_info.num_listeners, 2)
                self.assertEqual(listener_info.max_frequency, max(max_frequency1, max_frequency2))

                await sub2.unsubscribe()
                await asyncio.sleep(delay)

                listener_info = await publisher.get_listener_info()
                assert listener_info is not None
                logger.info(f"after sub2 unsubscribed: {listener_info=}")
                self.assertEqual(listener_info.num_listeners, 1)
                self.assertEqual(listener_info.max_frequency, max_frequency1)

                await sub1.unsubscribe()
                await asyncio.sleep(delay)

                listener_info = await publisher.get_listener_info()
                assert listener_info is not None
                logger.info(f"after sub2 unsubscribed as well: {listener_info=}")
                self.assertEqual(listener_info.num_listeners, 0)

                self.assertEqual(listener_info.max_frequency, None)
