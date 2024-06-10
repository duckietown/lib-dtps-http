import asyncio
import os
import tempfile
from asyncio import Event
from typing import List
from unittest import IsolatedAsyncioTestCase

from dtps import ContextConfig, DTPSContext
from dtps_http import (
    async_error_catcher,
    RawData,
)
from dtps_http_tests.utils import test_timeout
from . import logger
from .utils import create_python_at_known_socket, create_use


class TestPatient1(IsolatedAsyncioTestCase):
    @test_timeout(20)
    @async_error_catcher
    async def test_patient_get(self):
        with tempfile.TemporaryDirectory() as td:
            testname = "patientget"
            socket = os.path.join(td, "patientget")

            context_use: DTPSContext
            async with create_use(name="using", socket_node=socket) as context_use:
                context_use = context_use.configure(ContextConfig(patient=True))
                topic_name = "my_topic"
                use_topic = context_use / topic_name

                async def get_it():
                    return await use_topic.data_get()

                task = asyncio.create_task(get_it())

                await asyncio.sleep(1)
                async with create_python_at_known_socket(socket, testname) as (create, use):
                    topic = await (create / topic_name).queue_create()
                    rd = RawData(content=b"hello", content_type="text/plain")
                    await topic.publish(rd)

                    await asyncio.sleep(1)

                await task

    @test_timeout(120)
    @async_error_catcher
    async def test_patient_sub1(self):
        with tempfile.TemporaryDirectory() as td:

            socket = os.path.join(td, "patientsub1")
            context_use: DTPSContext
            async with create_use(name="using", socket_node=socket) as context_use:
                context_use = context_use.configure(ContextConfig(patient=True))
                topic_name = "my_topic"
                use_topic = context_use / topic_name

                events_arrived = [Event(), Event()]
                found: List[RawData] = []

                @async_error_catcher
                async def client_task() -> None:
                    finished = Event()

                    @async_error_catcher
                    async def collect(rd_: RawData) -> None:
                        logger.info(f"client_task: collected one {rd_}")

                        i = len(found)
                        events_arrived[i].set()
                        found.append(rd_)

                        if len(found) == 2:
                            logger.info(f"client_task: finished collecting")
                            finished.set()

                    logger.info(f"client_task: subscribing")
                    sub = await use_topic.subscribe(collect)
                    try:
                        logger.info(f"client_task: wait for finish")
                        await finished.wait()
                    finally:
                        logger.info(f"client_task: finishing")
                        await sub.unsubscribe()

                t = asyncio.create_task(client_task())
                # await asyncio.sleep(1)
                logger.info(f"creating first instance")

                async with create_python_at_known_socket(socket, "instance1") as (create, use):
                    topic = await (create / topic_name).queue_create()
                    rd = RawData(content=b"hello1", content_type="text/plain")
                    await topic.publish(rd)

                    # await asyncio.sleep(10)
                    logger.info(f"waiting for client to get one")
                    await events_arrived[0].wait()
                    logger.info(f"terminating first instance")

                if os.path.exists(socket):
                    msg = "socket still exists"
                    raise Exception(msg)
                logger.info(f"terminated first instance")

                logger.info(f"creating second instance")
                async with create_python_at_known_socket(socket, "instance2") as (create, use):
                    topic = await (create / topic_name).queue_create()
                    rd = RawData(content=b"hello2", content_type="text/plain")
                    await topic.publish(rd)
                    logger.info(f"waiting for client to get second")
                    await events_arrived[1].wait()
                    logger.info(f"terminating second instance")
                logger.info(f"terminated second instance")
                await t
                logger.info(f"found: {found}")
                # self.assertEqual(len(found), 2)
