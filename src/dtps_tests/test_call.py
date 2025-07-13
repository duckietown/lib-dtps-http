"""Call test."""

from typing import Any
from unittest import IsolatedAsyncioTestCase

import pytest

from dtps_http import MIME_TEXT, RawData, async_error_catcher
from dtps_http.object_queue import TransformError
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger
from dtps_tests.utils import create_use_pair


class TestCall(IsolatedAsyncioTestCase):
    """Call test."""

    @staticmethod
    def _get_rpc_handler_1(request: RawData, response: RawData) -> Any:
        async def rpc_handler(otc: RawData) -> RawData | TransformError:
            if otc != request:
                raise AssertionError
            return response

        return rpc_handler

    @staticmethod
    def _get_rpc_handler_2() -> Any:
        async def rpc_handler(_: RawData) -> RawData | TransformError:
            raise AssertionError

        return rpc_handler

    @test_timeout(20)
    @async_error_catcher
    async def test_call(self) -> None:
        """Run call test."""
        async with create_use_pair("call1") as (
            context_rpc_server,
            context_rpc_client,
        ):
            rpc_listen = context_rpc_server / "rpc"
            rpc_call = context_rpc_client / "rpc"
            request = RawData(content=b"hi", content_type=MIME_TEXT)
            response = RawData(content=b"hello", content_type=MIME_TEXT)
            if await rpc_listen.exists():
                raise AssertionError
            rpc_handler = self._get_rpc_handler_1(request, response)
            await rpc_listen.queue_create(transform=rpc_handler)
            if not await rpc_listen.exists():
                raise AssertionError
            result1 = await rpc_listen.call(request)
            if result1 != response:
                message = "Unexpected content."
                raise Exception(message)
            logger.info("rpc_call: %s", rpc_call)
            logger.info("rpc_listen: %s", rpc_listen)
            if not await rpc_call.exists():
                raise AssertionError
            result = await rpc_call.call(request)
            if result != response:
                message = "Unexpected content."
                raise Exception(message)
            node_id1 = await context_rpc_server.get_node_id()
            node_id2 = await context_rpc_client.get_node_id()
            if node_id1 != node_id2:
                raise AssertionError

    @test_timeout(20)
    @async_error_catcher
    async def test_call_cannot_create(self) -> None:
        """Run call cannot create test."""
        async with create_use_pair("callcannotcreate") as (
            context_rpc_server,
            context_rpc_client,
        ):
            rpc_handler = self._get_rpc_handler_2()
            with pytest.raises(ValueError):
                await context_rpc_client.queue_create(transform=rpc_handler)
