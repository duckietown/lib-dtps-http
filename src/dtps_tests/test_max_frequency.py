"""Maximum frequency test."""

import asyncio
import time
from typing import Any
from unittest import IsolatedAsyncioTestCase

from dtps import AbstractDTPSContext
from dtps_http import RawData, async_error_catcher
from dtps_http.utils_every_once_in_a_while import EveryOnceInAWhile
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger
from dtps_tests.utils import create_rust_server, create_use_pair


class TestMaxFrequency(IsolatedAsyncioTestCase):
    """Maximum frequency test."""

    @test_timeout(20)
    @async_error_catcher
    async def test_max_freq_create(self) -> None:
        """Run maximum frequency create test."""
        async with create_use_pair("call1") as (create, _):
            await self._testmax(create)

    @test_timeout(20)
    @async_error_catcher
    async def test_max_freq_use(self) -> None:
        """Run maximum frequency use test."""
        async with create_use_pair("use") as (_, use):
            await self._testmax(use)

    @test_timeout(20)
    async def test_max_freq_rust(self) -> None:
        """Run maximum frequency Rust test."""
        async with create_rust_server("maxfreq") as use:
            await self._testmax(use)

    @staticmethod
    def _get_collect_1(found: list[RawData]) -> Any:
        async def collect(raw_data: RawData) -> None:
            found.append(raw_data)

        return collect

    @staticmethod
    def _get_collect_2() -> Any:
        async def collect(_: RawData) -> None:
            pass

        return collect

    async def _testmax(self, root: AbstractDTPSContext) -> None:
        topic = root / "my_topic"
        await topic.queue_create()
        max_frequency = 3
        effective_frequency = 3 * max_frequency
        period = 4
        found = []
        collect = self._get_collect_1(found)
        await topic.subscribe(collect, max_frequency=max_frequency)
        publish_dt = 1 / effective_frequency
        when = EveryOnceInAWhile(publish_dt)
        i = 0
        logger.info("publish_dt=%s", publish_dt)
        nsent = 0
        start_time = time.time()
        while True:
            i += 1
            dt = time.time() - start_time
            if dt > period:
                break
            if when.now():
                data = {
                    "i": i,
                    "dt": dt,
                }
                raw_data = RawData.cbor_from_native_object(data)
                await topic.publish(raw_data)
                nsent += 1
            await asyncio.sleep(publish_dt)
        await asyncio.sleep(3)
        expected = period * max_frequency
        expected_n = int(expected)
        nfound = len(found)
        too_many = len(found) > expected_n + 3  # allow for some slop
        too_few = len(found) < expected_n - 2
        logger.info("nsent: %s", nsent)
        logger.info("expected: %s", expected_n)
        logger.info("nfound: %s", nfound)
        stats = (
            f" {max_frequency=} {effective_frequency=} {period} {nsent=} "
            f"{nfound=} {expected_n=} {expected=}"
        )
        if too_many:
            message = (
                f"Too many messages found: {nfound}, expected around "
                f"{expected_n}"
            )
            message += f"\n{stats}"
            raise Exception(message)
        if too_few:
            message = (
                f"Too few messages found: {nfound}, expected around "
                f"{expected_n}"
            )
            message += f"\n{stats}"
            raise Exception(message)

    @test_timeout(20)
    @async_error_catcher
    async def test_max_freq_publisher_local(self) -> None:
        """Run maximum frequency local publisher test."""
        async with create_use_pair("use") as (create, _):
            topic = create / "my_topic"
            await topic.queue_create()
            max_frequency1 = 3
            max_frequency2 = 13
            async with topic.publisher_context() as publisher:
                listener_info = await publisher.get_listener_info()
                if listener_info is None:
                    raise AssertionError
                logger.info("listener_info=%s", listener_info)
                expected_num_listeners = 0
                if listener_info.num_listeners != expected_num_listeners:
                    raise AssertionError
                if listener_info.max_frequency is not None:
                    raise AssertionError
                collect = self._get_collect_2()
                sub1 = await topic.subscribe(
                    collect,
                    max_frequency=max_frequency1,
                )
                sub2 = await topic.subscribe(
                    collect,
                    max_frequency=max_frequency2,
                )
                listener_info = await publisher.get_listener_info()
                if listener_info is None:
                    raise AssertionError
                logger.info("listener_info=%s", listener_info)
                expected_num_listeners = 2
                if listener_info.num_listeners != expected_num_listeners:
                    raise AssertionError
                if listener_info.max_frequency != max(
                    max_frequency1,
                    max_frequency2,
                ):
                    raise AssertionError
                await sub2.unsubscribe()
                listener_info = await publisher.get_listener_info()
                logger.info("listener_info=%s", listener_info)
                if listener_info is None:
                    raise AssertionError
                expected_num_listeners = 1
                if listener_info.num_listeners != expected_num_listeners:
                    raise AssertionError
                if listener_info.max_frequency != max_frequency1:
                    raise AssertionError
                await sub1.unsubscribe()
                listener_info = await publisher.get_listener_info()
                if listener_info is None:
                    raise AssertionError
                logger.info("listener_info=%s", listener_info)
                expected_num_listeners = 0
                if listener_info.num_listeners != expected_num_listeners:
                    raise AssertionError
                if listener_info.max_frequency is not None:
                    raise AssertionError

    @test_timeout(20)
    @async_error_catcher
    async def test_max_freq_publisher_remote(self) -> None:
        """Run maximum frequency remote publisher test."""
        async with create_use_pair("use") as (create, use):
            topic = create / "my_topic"
            await topic.queue_create()
            topic_use = use / "my_topic"
            max_frequency1 = 3
            max_frequency2 = 13
            delay = 0.2
            async with topic.publisher_context() as publisher:
                listener_info = await publisher.get_listener_info()
                if listener_info is None:
                    raise AssertionError
                logger.info("listener_info=%s", listener_info)
                expected_num_listeners = 0
                if listener_info.num_listeners != expected_num_listeners:
                    raise AssertionError
                if listener_info.max_frequency is not None:
                    raise AssertionError
                collect = self._get_collect_2()
                sub1 = await topic_use.subscribe(
                    collect,
                    max_frequency=max_frequency1,
                )
                sub2 = await topic_use.subscribe(
                    collect,
                    max_frequency=max_frequency2,
                )
                await asyncio.sleep(delay)
                listener_info = await publisher.get_listener_info()
                if listener_info is None:
                    raise AssertionError
                logger.info(
                    "After sub1, sub2 subscribed: listener_info=%s",
                    listener_info,
                )
                expected_num_listeners = 2
                if listener_info.num_listeners != expected_num_listeners:
                    raise AssertionError
                if listener_info.max_frequency != max(
                    max_frequency1,
                    max_frequency2,
                ):
                    raise AssertionError
                await sub2.unsubscribe()
                await asyncio.sleep(delay)
                listener_info = await publisher.get_listener_info()
                if listener_info is None:
                    raise AssertionError
                logger.info(
                    "After sub2 unsubscribed: listener_info=%s",
                    listener_info,
                )
                expected_num_listeners = 1
                if listener_info.num_listeners != expected_num_listeners:
                    raise AssertionError
                if listener_info.max_frequency != max_frequency1:
                    raise AssertionError
                await sub1.unsubscribe()
                await asyncio.sleep(delay)
                listener_info = await publisher.get_listener_info()
                if listener_info is None:
                    raise AssertionError
                logger.info(
                    "After sub2 unsubscribed as well: listener_info=%s",
                    listener_info,
                )
                expected_num_listeners = 0
                if listener_info.num_listeners != expected_num_listeners:
                    raise AssertionError
                if listener_info.max_frequency is not None:
                    raise AssertionError
