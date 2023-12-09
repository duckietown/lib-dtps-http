from unittest import IsolatedAsyncioTestCase

from dtps_http import (
    async_error_catcher,
)
from dtps_http_tests.utils import test_timeout
from .utils import create_use_pair


class TestCall(IsolatedAsyncioTestCase):
    @test_timeout(20)
    @async_error_catcher
    async def test_remove1(self):
        async with create_use_pair("remove") as (contexts_local, context_remote):
            topic = "topic"
            topic_local = contexts_local / topic
            topic_remote = context_remote / topic
            self.assertEqual(await topic_local.exists(), False)
            await topic_local.queue_create()
            self.assertEqual(await topic_local.exists(), True)
            await topic_local.remove()
            self.assertEqual(await topic_local.exists(), False)

            self.assertEqual(await topic_remote.exists(), False)
            await topic_remote.queue_create()
            self.assertEqual(await topic_remote.exists(), True)
            await topic_remote.remove()
            self.assertEqual(await topic_remote.exists(), False)
