import asyncio
import json
import os
import tempfile
import unittest
from typing import cast, Literal

import cbor2
import yaml

from dtps_http import (
    app_start,
    async_error_catcher,
    CONTENT_TYPE_PATCH_CBOR,
    CONTENT_TYPE_PATCH_JSON,
    CONTENT_TYPE_PATCH_YAML,
    ContentInfo,
    DTPSClient,
    DTPSServer,
    interpret_command_line_and_start,
    join,
    make_http_unix_url,
    MIME_CBOR,
    MIME_JSON,
    MIME_YAML,
    parse_url_unescape,
    RawData,
    TopicNameV,
    TopicProperties,
    TopicRefAdd,
    URLIndexer,
    URLString,
)
from . import logger
from .utils import test_timeout


class TestAsyncServerFunction(unittest.IsolatedAsyncioTestCase):
    @test_timeout(10)
    @async_error_catcher
    async def test_push1(self):
        with tempfile.TemporaryDirectory() as td:
            socket_node = os.path.join(td, "node")
            dtps_server = DTPSServer.create(nickname="node")
            server = await app_start(
                dtps_server,
                unix_paths=[socket_node],
            )
            async with server:
                url0 = make_http_unix_url(socket_node)

                async with DTPSClient.create() as client:
                    parameters = TopicRefAdd(
                        content_info=ContentInfo.simple(MIME_JSON),
                        properties=TopicProperties.rw_pushable(),
                        app_data={},
                    )
                    topic = TopicNameV.from_dash_sep("a/b")

                    await client.add_topic(cast(URLIndexer, url0), topic, parameters)

                    queue_in: "asyncio.Queue[RawData]" = asyncio.Queue()
                    queue_out = asyncio.Queue()
                    url_topic = join(url0, topic.as_relative_url())

                    received = []

                    async def found(rd_: RawData) -> None:
                        logger.info(f"found {rd!r}")
                        received.append(rd_)

                    ldi = await client.listen_url(url_topic, found, inline_data=True, raise_on_error=True)
                    task_push = await client.push_continuous(
                        url_topic, queue_in=queue_in, queue_out=queue_out
                    )
                    N = 5
                    sent = []
                    for i in range(N):
                        rd = RawData.json_from_native_object(i)
                        sent.append(rd)
                        await queue_in.put(rd)
                        logger.info(f"got {rd!r}")
                        success = await queue_out.get()
                        if not success:
                            raise Exception(f"Could not push {rd!r}")
                    await asyncio.sleep(1.0)
                    logger.info(f"received={received!r}")
                    if received != sent:
                        raise Exception(f"received={received!r} != sent={sent!r}")
                    task_push.cancel()
                    await ldi.stop()

                    logger.info(f"test finished")
                    # task_server.cancel()
                    # await task_push
                    # await task_sub

    @test_timeout(10)
    async def test_static1(self):
        port = 8432
        args = ["--tcp-port", str(port)]
        dtps_server = DTPSServer.create()
        t = interpret_command_line_and_start(dtps_server, args)
        task = asyncio.create_task(t)
        await dtps_server.started.wait()

        # make http request using aiohttp
        # https://docs.aiohttp.org/en/stable/client_quickstart.html
        import aiohttp

        paths = ["/", "/static/style.css", "/static/send.js"]
        urls = [f"http://localhost:{port}{p}" for p in paths]

        for url in urls:
            logger.info(f"GET {url!r}")
            async with aiohttp.ClientSession() as session:
                logger.info(f"GET {url!r}")

                async with session.get(url) as resp:
                    logger.info(f"GET {url!r} status={resp.status}")
                    resp.raise_for_status()

        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

    @test_timeout(10)
    async def test_patch1_json_json(self):
        await doit("json", "json")

    @test_timeout(10)
    async def test_patch1_json_cbor(self):
        await doit("json", "cbor")

    @test_timeout(10)
    async def test_patch1_cbor_json(self):
        await doit("cbor", "json")

    @test_timeout(10)
    async def test_patch1_cbor_cbor(self):
        await doit("cbor", "cbor")

    @test_timeout(10)
    async def test_patch1_cbor_yaml(self):
        await doit("cbor", "yaml")

    @test_timeout(10)
    async def test_patch1_yaml_cbor(self):
        await doit("yaml", "cbor")

    @test_timeout(10)
    async def test_patch1_json_yaml(self):
        await doit("json", "yaml")

    @test_timeout(10)
    async def test_patch1_yaml_json(self):
        await doit("yaml", "json")

    @test_timeout(10)
    async def test_patch_creation(self):
        port = 8435
        args = ["--tcp-port", str(port)]
        dtps_server = DTPSServer.create()
        t = interpret_command_line_and_start(dtps_server, args)
        task = asyncio.create_task(t)
        logger.info("waiting for server to start")
        await dtps_server.started.wait()
        logger.info("waiting for server to start: done")
        url = URLIndexer(parse_url_unescape(URLString(f"http://localhost:{port}/")))

        tra = TopicRefAdd(
            content_info=ContentInfo.simple(MIME_JSON),
            properties=TopicProperties.rw_pushable(),
            app_data={},
        )
        async with DTPSClient.create() as client:
            await client.add_topic(url, TopicNameV.from_dash_sep("a/b"), tra)

        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass


async def doit(topic_mime: Literal["json", "cbor", "yaml"], patch_mime: Literal["json", "cbor", "yaml"]):
    with tempfile.TemporaryDirectory() as td:
        socket_node = os.path.join(td, "node")
        dtps_server = DTPSServer.create(nickname="node")
        server = await app_start(
            dtps_server,
            unix_paths=[socket_node],
        )

        url_server = make_http_unix_url(socket_node)
        async with server:
            assert os.path.exists(socket_node)

            topic = TopicNameV.from_relative_url("config/")

            if topic_mime == "json":
                topic_content_type = MIME_JSON
            elif topic_mime == "cbor":
                topic_content_type = MIME_CBOR
            elif topic_mime == "yaml":
                topic_content_type = MIME_YAML
            else:
                raise Exception(f"Unknown topic_mime={topic_mime!r}")

            oq = await dtps_server.create_oq(topic, content_info=ContentInfo.simple(topic_content_type))
            ob1 = {"A": {"B": ["C", "D"]}}

            if topic_mime == "json":
                await oq.publish_json(ob1)
            elif topic_mime == "cbor":
                await oq.publish_cbor(ob1)
            elif topic_mime == "yaml":
                await oq.publish_yaml(ob1)
            else:
                raise Exception(f"Unknown topic_mime={patch_mime!r}")

            logger.info(str(list(dtps_server._oqs.keys())))
            url = join(url_server, topic.as_relative_url())

            patch = [
                {"op": "add", "path": "/A/B/-", "value": "E"},
            ]

            ob2_expected = {"A": {"B": ["C", "D", "E"]}}

            if patch_mime == "json":
                headers = {"Content-type": CONTENT_TYPE_PATCH_JSON}
                data = json.dumps(patch)
            elif patch_mime == "cbor":
                headers = {"Content-type": CONTENT_TYPE_PATCH_CBOR}
                data = cbor2.dumps(patch)
            elif patch_mime == "yaml":
                headers = {"Content-type": CONTENT_TYPE_PATCH_YAML}
                data = yaml.dump(patch)
            else:
                raise Exception(f"Unknown patch_mime={patch_mime!r}")

            async with DTPSClient.create() as client:
                async with client.my_session(url) as (session, use_url):
                    resp = await session.patch(use_url, headers=headers, data=data)
                    resp.raise_for_status()

            rd2 = oq.last_data()
            ob2 = rd2.get_as_native_object()

            logger.info(f"ob1={ob1!r}")
            logger.info(f"ob2={ob2!r}")

            if ob2 != ob2_expected:
                raise Exception(f"ob2={ob2!r} != ob2_expected={ob2_expected!r}")


# This allows running the tests with `nose2` command.
if __name__ == "__main__":
    unittest.main()
