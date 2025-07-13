"""Patient test."""

import asyncio
from asyncio import Event
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest import IsolatedAsyncioTestCase

from dtps import AbstractDTPSContext, ContextConfig
from dtps_http import RawData, async_error_catcher
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger
from dtps_tests.utils import create_python_at_known_socket, create_use

MAXIMUM_FOUND_LENGTH = 2


class TestPatient(IsolatedAsyncioTestCase):
    """Patient test."""

    @staticmethod
    async def get_data(use_topic: AbstractDTPSContext) -> RawData:
        """Return data."""
        return await use_topic.data_get()

    @test_timeout(20)
    @async_error_catcher
    async def test_patient_get(self) -> None:
        """Run patient get test."""
        with TemporaryDirectory() as temporary_directory:
            testname = "patientget"
            path = Path(temporary_directory) / "patientget"
            socket = path.as_posix()
            async with create_use(
                name="using",
                socket_node=socket,
            ) as context_use:
                context_configuration = ContextConfig(patient=True)
                patient_context_use = context_use.configure(
                    context_configuration,
                )
                topic_name = "my_topic"
                use_topic = patient_context_use / topic_name
                coroutine = self.get_data(use_topic)
                task = asyncio.create_task(coroutine)
                await asyncio.sleep(1)
                async with create_python_at_known_socket(socket, testname) as (
                    create,
                    _,
                ):
                    topic = create / topic_name
                    await topic.queue_create()
                    raw_data = RawData(
                        content=b"hello",
                        content_type="text/plain",
                    )
                    await topic.publish(raw_data)
                    await asyncio.sleep(1)
                await task

    @staticmethod
    def _get_collect(
        found: list[RawData],
        events_arrived: tuple[Event, Event],
        finished: Event,
    ) -> Any:
        @async_error_catcher
        async def collect(raw_data: RawData) -> None:
            logger.info("client_task: Collected one %s", raw_data)
            i = len(found)
            events_arrived[i].set()
            found.append(raw_data)
            if len(found) == MAXIMUM_FOUND_LENGTH:
                logger.info("client_task: Finished collecting.")
                finished.set()

        return collect

    @async_error_catcher
    async def client_task(
        self,
        found: list[RawData],
        events_arrived: tuple[Event, Event],
        use_topic: AbstractDTPSContext,
    ) -> None:
        """Run client task."""
        finished = Event()
        collect = self._get_collect(found, events_arrived, finished)
        logger.info("client_task: Subscribing...")
        sub = await use_topic.subscribe(collect)
        try:
            logger.info("client_task: Wait for finish.")
            await finished.wait()
        finally:
            logger.info("client_task: Finishing...")
            await sub.unsubscribe()

    @test_timeout(120)
    @async_error_catcher
    async def test_patient_sub(self) -> None:
        """Run patient subscription test."""
        with TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "patientsub1"
            socket = path.as_posix()
            async with create_use(
                name="using",
                socket_node=socket,
            ) as context_use:
                context_configuration = ContextConfig(patient=True)
                configured_context_use = context_use.configure(
                    context_configuration,
                )
                topic_name = "my_topic"
                use_topic = configured_context_use / topic_name
                events_arrived = (Event(), Event())
                found: list[RawData] = []
                coroutine = self.client_task(found, events_arrived, use_topic)
                task = asyncio.create_task(coroutine)
                logger.info("Creating first instance...")
                async with create_python_at_known_socket(
                    socket,
                    "instance1",
                ) as (create, _):
                    topic = create / topic_name
                    await topic.queue_create()
                    raw_data = RawData(
                        content=b"hello1",
                        content_type="text/plain",
                    )
                    await topic.publish(raw_data)
                    logger.info("Waiting for client to get one...")
                    await events_arrived[0].wait()
                    logger.info("Terminating first instance...")
                if path.exists():
                    message = "Socket still exists."
                    raise Exception(message)
                logger.info("Terminated first instance.")
                logger.info("Creating second instance...")
                async with create_python_at_known_socket(
                    socket,
                    "instance2",
                ) as (create, _):
                    topic = create / topic_name
                    await topic.queue_create()
                    raw_data = RawData(
                        content=b"hello2",
                        content_type="text/plain",
                    )
                    await topic.publish(raw_data)
                    logger.info("Waiting for client to get second...")
                    await events_arrived[1].wait()
                    logger.info("Terminating second instance...")
                logger.info("Terminated second instance.")
                await task
                logger.info("found: %s", found)
