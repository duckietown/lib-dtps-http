"""Server tests."""

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Literal, cast

import aiohttp
import cbor2
import yaml

from dtps_http import (
    CONTENT_TYPE_PATCH_CBOR,
    CONTENT_TYPE_PATCH_JSON,
    CONTENT_TYPE_PATCH_YAML,
    MIME_CBOR,
    MIME_JSON,
    MIME_YAML,
    ContentInfo,
    DTPSClient,
    DTPSServer,
    RawData,
    TopicNameV,
    TopicProperties,
    TopicRefAdd,
    URLIndexer,
    URLString,
    app_start,
    async_error_catcher,
    interpret_command_line_and_start,
    join,
    make_http_unix_url,
    parse_url_unescape,
)
from dtps_http.structures import Bounds
from dtps_http_tests import logger
from dtps_http_tests.utils import test_timeout


class TestAsyncServerFunction(unittest.IsolatedAsyncioTestCase):
    """Test async server function."""

    @test_timeout(10)
    @async_error_catcher
    async def test_push(self) -> None:
        """Run push test."""
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "node"
            socket_node = path.as_posix()
            dtps_server = DTPSServer.create(nickname="node")
            server = await app_start(dtps_server, unix_paths=[socket_node])
            async with server:
                url0 = make_http_unix_url(socket_node)
                async with DTPSClient.create() as client:
                    content_info = ContentInfo.simple(MIME_JSON)
                    properties = TopicProperties.rw_pushable()
                    bounds = Bounds.unbounded()
                    parameters = TopicRefAdd(
                        app_data={},
                        properties=properties,
                        content_info=content_info,
                        bounds=bounds,
                    )
                    topic = TopicNameV.from_dash_sep("a/b")
                    url_indexer = cast(URLIndexer, url0)
                    await client.add_topic(url_indexer, topic, parameters)
                    queue_in: asyncio.Queue[RawData] = asyncio.Queue()
                    queue_out: asyncio.Queue[bool] = asyncio.Queue()
                    relative_url = topic.as_relative_url()
                    url_topic = join(url0, relative_url)
                    received: list[RawData] = []
                    callback = self._get_callback(received)
                    listen_data_interface = await client.listen_url(
                        url_topic,
                        callback,
                        inline_data=True,
                        raise_on_error=True,
                        max_frequency=None,
                    )
                    task_push = await client.push_continuous(
                        url_topic,
                        queue_in=queue_in,
                        queue_out=queue_out,
                    )
                    sent: list[RawData] = []
                    for i in range(5):
                        raw_data = RawData.json_from_native_object(i)
                        sent.append(raw_data)
                        await queue_in.put(raw_data)
                        logger.info("Got %r.", raw_data)
                        success = await queue_out.get()
                        if not success:
                            message = f"Could not push {raw_data!r}."
                            raise Exception(message)
                    await asyncio.sleep(1)
                    logger.info("Received %r.", received)
                    if received != sent:
                        message = f"received={received!r} != sent={sent!r}."
                        raise Exception(message)
                    task_push.cancel()
                    await listen_data_interface.stop()
                    logger.info("Test complete.")

    def _get_callback(self, received: list[RawData]) -> Any:
        def callback(raw_data: RawData) -> None:
            received.append(raw_data)
            logger.info("Found %r.", raw_data)

        return callback

    @test_timeout(10)
    async def test_static(self) -> None:
        """Run static test."""
        port = "8432"
        args = ["--tcp-port", port]
        dtps_server = DTPSServer.create()
        coroutine = interpret_command_line_and_start(dtps_server, args)
        task = asyncio.create_task(coroutine)
        await dtps_server.started.wait()
        paths = ("/", "/static/style.css", "/static/send.js")
        urls = [f"http://localhost:{port}{path}" for path in paths]
        for url in urls:
            logger.info("GET %r", url)
            async with aiohttp.ClientSession() as session:
                logger.info("GET %r", url)
                async with session.get(url) as resp:
                    logger.info("GET %r status=%s", url, resp.status)
                    resp.raise_for_status()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            if task.done():
                pass
            else:
                raise

    @test_timeout(10)
    async def test_patch1_json_json(self) -> None:
        """JSON-JSON patch test."""
        await do_it("json", "json")

    @test_timeout(10)
    async def test_patch1_json_cbor(self) -> None:
        """JSON-CBOR patch test."""
        await do_it("json", "cbor")

    @test_timeout(10)
    async def test_patch1_cbor_json(self) -> None:
        """CBOR-JSON patch test."""
        await do_it("cbor", "json")

    @test_timeout(10)
    async def test_patch1_cbor_cbor(self) -> None:
        """CBOR-CBOR patch test."""
        await do_it("cbor", "cbor")

    @test_timeout(10)
    async def test_patch1_cbor_yaml(self) -> None:
        """CBOR-YAML patch test."""
        await do_it("cbor", "yaml")

    @test_timeout(10)
    async def test_patch1_yaml_cbor(self) -> None:
        """YAML-CBOR patch test."""
        await do_it("yaml", "cbor")

    @test_timeout(10)
    async def test_patch1_json_yaml(self) -> None:
        """JSON-YAML patch test."""
        await do_it("json", "yaml")

    @test_timeout(10)
    async def test_patch1_yaml_json(self) -> None:
        """YAML-JSON patch test."""
        await do_it("yaml", "json")

    @test_timeout(10)
    async def test_patch_creation(self) -> None:
        """Patch creation test."""
        port = "8435"
        args = ["--tcp-port", port]
        dtps_server = DTPSServer.create()
        coroutine = interpret_command_line_and_start(dtps_server, args)
        task = asyncio.create_task(coroutine)
        logger.info("waiting for server to start")
        await dtps_server.started.wait()
        logger.info("waiting for server to start: done")
        url_string = URLString(f"http://localhost:{port}/")
        url = parse_url_unescape(url_string)
        url_indexer = URLIndexer(url)
        content_info = ContentInfo.simple(MIME_JSON)
        properties = TopicProperties.rw_pushable()
        bounds = Bounds.unbounded()
        topic_reference_add = TopicRefAdd(
            content_info=content_info,
            properties=properties,
            app_data={},
            bounds=bounds,
        )
        async with DTPSClient.create() as client:
            topic_name = TopicNameV.from_dash_sep("a/b")
            await client.add_topic(
                url_indexer,
                topic_name,
                topic_reference_add,
            )
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            if task.done():
                pass
            else:
                raise


async def do_it(
    topic_mime: Literal["json", "cbor", "yaml"],
    patch_mime: Literal["json", "cbor", "yaml"],
) -> None:
    """Do it."""
    with tempfile.TemporaryDirectory() as temporary_directory:
        path = Path(temporary_directory) / "node"
        socket_node = path.as_posix()
        dtps_server = DTPSServer.create(nickname="node")
        server = await app_start(dtps_server, unix_paths=[socket_node])
        url_server = make_http_unix_url(socket_node)
        async with server:
            if not path.exists():
                raise AssertionError
            topic_name = TopicNameV.from_relative_url("config/")
            if topic_mime == "json":
                topic_content_type = MIME_JSON
            elif topic_mime == "cbor":
                topic_content_type = MIME_CBOR
            elif topic_mime == "yaml":
                topic_content_type = MIME_YAML
            else:
                message = f"Unknown topic_mime={topic_mime!r}"
                raise Exception(message)
            content_info = ContentInfo.simple(topic_content_type)
            bounds = Bounds.unbounded()
            object_queue = await dtps_server.create_object_queue(
                topic_name,
                content_info=content_info,
                topic_properties=None,
                bounds=bounds,
            )
            ob1 = {
                "A": {
                    "B": ["C", "D"],
                },
            }
            if topic_mime == "json":
                await object_queue.publish_json(ob1)
            elif topic_mime == "cbor":
                await object_queue.publish_cbor(ob1)
            elif topic_mime == "yaml":
                await object_queue.publish_yaml(ob1)
            else:
                message = f"Unknown topic_mime={patch_mime!r}"
                raise Exception(message)
            dtps_server_object_queues_keys = dtps_server.object_queues.keys()
            dtps_server_object_queues_keys_list = list(
                dtps_server_object_queues_keys,
            )
            logger.info(dtps_server_object_queues_keys_list)
            relative_url = topic_name.as_relative_url()
            url = join(url_server, relative_url)
            patch = [
                {
                    "op": "add",
                    "path": "/A/B/-",
                    "value": "E",
                },
            ]
            ob2_expected = {
                "A": {
                    "B": ["C", "D", "E"],
                },
            }
            data: bytes | str
            if patch_mime == "json":
                headers = {
                    "Content-type": CONTENT_TYPE_PATCH_JSON,
                }
                data = json.dumps(patch)
            elif patch_mime == "cbor":
                headers = {
                    "Content-type": CONTENT_TYPE_PATCH_CBOR,
                }
                data = cbor2.dumps(patch)
            elif patch_mime == "yaml":
                headers = {
                    "Content-type": CONTENT_TYPE_PATCH_YAML,
                }
                data = yaml.dump(patch)
            else:
                message = f"Unknown patch_mime={patch_mime!r}"
                raise Exception(message)
            async with (
                DTPSClient.create() as client,
                client.my_session(url) as (session, use_url),
            ):
                resp = await session.patch(use_url, headers=headers, data=data)
                resp.raise_for_status()
            raw_data_2 = object_queue.last_data()
            ob2 = raw_data_2.get_as_native_object()
            logger.info("ob1=%r", ob1)
            logger.info("ob2=%r", ob2)
            if ob2 != ob2_expected:
                message = f"ob2={ob2!r} != ob2_expected={ob2_expected!r}"
                raise Exception(message)


# This allows running the tests with `nose2` command.
if __name__ == "__main__":
    unittest.main()
