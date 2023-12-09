from typing import Union
from unittest import IsolatedAsyncioTestCase

from dtps_http import (
    async_error_catcher,
    MIME_TEXT,
    RawData,
)
from dtps_http.object_queue import TransformError
from dtps_http_tests.utils import test_timeout
from . import logger
from .utils import create_use_pair


class TestCall(IsolatedAsyncioTestCase):
    @test_timeout(20)
    @async_error_catcher
    async def test_call1(self):
        async with create_use_pair("call1") as (context_rpcserver, context_rpcclient):
            rpc_listen = context_rpcserver / "rpc"
            rpc_call = context_rpcclient / "rpc"

            request = RawData(content=b"hi", content_type=MIME_TEXT)
            response = RawData(content=b"hello", content_type=MIME_TEXT)

            async def rpc_handler(otc: RawData) -> Union[RawData, TransformError]:
                self.assertEqual(otc, request)
                return response

            self.assertEqual(await rpc_listen.exists(), False)
            await rpc_listen.queue_create(transform=rpc_handler)
            self.assertEqual(await rpc_listen.exists(), True)

            result1 = await rpc_listen.call(request)
            if result1 != response:
                raise Exception("unexpected content")

            logger.info(f"rpc_call: {rpc_call}")
            logger.info(f"rpc_listen: {rpc_listen}")
            self.assertEqual(await rpc_call.exists(), True)
            result = await rpc_call.call(request)

            if result != response:
                raise Exception("unexpected content")

            node_id1 = await context_rpcserver.get_node_id()
            node_id2 = await context_rpcclient.get_node_id()
            self.assertEqual(node_id1, node_id2)

    @test_timeout(20)
    @async_error_catcher
    async def test_call_cannot_create(self):
        async with create_use_pair("callcannotcreate") as (context_rpcserver, context_rpcclient):

            async def rpc_handler(_: RawData) -> Union[RawData, TransformError]:
                raise AssertionError

            with self.assertRaises(ValueError):
                await context_rpcclient.queue_create(transform=rpc_handler)
