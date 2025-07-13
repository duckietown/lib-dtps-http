"""Delete test."""

from unittest import IsolatedAsyncioTestCase

from dtps_http import async_error_catcher
from dtps_http_tests.utils import test_timeout
from dtps_tests.utils import create_use_pair


class TestDelete(IsolatedAsyncioTestCase):
    """Delete test."""

    @staticmethod
    @test_timeout(20)
    @async_error_catcher
    async def test_remove() -> None:
        """Run remove test."""
        async with create_use_pair("remove") as (
            contexts_local,
            context_remote,
        ):
            topic = "topic"
            topic_local = contexts_local / topic
            topic_remote = context_remote / topic
            if await topic_local.exists():
                raise AssertionError
            await topic_local.queue_create()
            if not await topic_local.exists():
                raise AssertionError
            await topic_local.remove()
            if await topic_local.exists():
                raise AssertionError
            if await topic_remote.exists():
                raise AssertionError
            await topic_remote.queue_create()
            if not await topic_remote.exists():
                raise AssertionError
            await topic_remote.remove()
            if await topic_remote.exists():
                raise AssertionError
