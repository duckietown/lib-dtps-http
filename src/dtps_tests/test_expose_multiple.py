"""Expose multiple test."""

import asyncio
from contextlib import AsyncExitStack
from unittest import IsolatedAsyncioTestCase

from dtps_http import async_error_catcher
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger
from dtps_tests.utils import create_rust_server, create_use_pair


class TestExposeMultiple(IsolatedAsyncioTestCase):
    """Expose multiple test."""

    @staticmethod
    @test_timeout(120)
    @async_error_catcher
    async def test_expose_multiple() -> None:
        """Run expose multiple test."""
        async_exit_stack = AsyncExitStack()
        async with async_exit_stack:
            switchboard_context_manager = create_rust_server("switchboard")
            switchboard = await async_exit_stack.enter_async_context(
                switchboard_context_manager,
            )
            node1_context_manager = create_use_pair("node1")
            _, my_context = await async_exit_stack.enter_async_context(
                node1_context_manager,
            )
            jpeg = my_context / "out" / "jpeg"
            await jpeg.queue_create()
            parameters = my_context / "out" / "parameters"
            await parameters.queue_create()
            await (switchboard / "nodes" / "camera").expose(my_context)
            await (switchboard / "sensors" / "camera" / "jpeg").expose(
                jpeg,
            )
            await (switchboard / "sensors" / "camera" / "parameters").expose(
                parameters,
            )
            await asyncio.sleep(2)
            data = await switchboard.data_get()
            object_ = data.get_as_native_object()
            topics = list(object_["topics"])
            topics.sort()
            expected = (
                "nodes/camera/out/jpeg",
                "nodes/camera/out/parameters",
                "sensors/camera/jpeg",
                "sensors/camera/parameters",
            )
            topic_string_list = [f"{topic!r}\n" for topic in topics]
            topics_string = "".join(topic_string_list)
            logger.info(f"topics:\n{topics_string}")
            for topic in expected:
                if topic not in topics:
                    message = f"{topic} not in {topics}."
                    raise AssertionError(message)
            await asyncio.sleep(2)
