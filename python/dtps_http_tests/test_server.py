import asyncio
import json
import unittest
from typing import Literal

import cbor2
import yaml

from dtps_http import (
    CONTENT_TYPE_PATCH_CBOR,
    CONTENT_TYPE_PATCH_JSON,
    CONTENT_TYPE_PATCH_YAML,
    ContentInfo,
    DTPSClient,
    DTPSServer,
    interpret_command_line_and_start,
    MIME_CBOR,
    MIME_JSON,
    MIME_YAML,
    parse_url_unescape,
    RawData,
    TopicNameV,
    URLString,
)
from dtps_http.structures import TopicProperties, TopicRefAdd
from dtps_http.urls import join, URLIndexer
from . import logger


class TestAsyncServerFunction(unittest.IsolatedAsyncioTestCase):
    async def test_push1(self):
        port = 8432
        args = ["--tcp-port", str(port)]
        dtps_server = DTPSServer.create()
        t = interpret_command_line_and_start(dtps_server, args)
        task_server = asyncio.create_task(t)
        await dtps_server.started.wait()
        url0 = parse_url_unescape(URLString(f"http://localhost:{port}/"))

        async with DTPSClient.create() as client:
            # await client.
            parameters = TopicRefAdd(
                content_info=ContentInfo.simple(MIME_JSON),
                properties=TopicProperties.rw_pushable(),
                app_data={},
            )
            topic = TopicNameV.from_dash_sep("a/b")

            await client.add_topic(url0, topic, parameters)

            queue_in: "asyncio.Queue[RawData]" = asyncio.Queue()
            queue_out = asyncio.Queue()
            url_topic = join(url0, topic.as_relative_url())

            received = []

            async def found(rd_: RawData) -> None:
                logger.info(f"found {rd!r}")
                received.append(rd_)

            task_sub = await client.listen_url(url_topic, found, inline_data=True, raise_on_error=True)
            task_push = await client.push_continuous(url_topic, queue_in=queue_in, queue_out=queue_out)
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
            task_sub.cancel()
            task_server.cancel()
            # await task_push
            # await task_sub

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
                async with session.get(url) as resp:
                    logger.info(f"GET {url!r} status={resp.status}")
                    resp.raise_for_status()

        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

    async def test_patch1_json_json(self):
        await doit("json", "json")

    async def test_patch1_json_cbor(self):
        await doit("json", "cbor")

    async def test_patch1_cbor_json(self):
        await doit("cbor", "json")

    async def test_patch1_cbor_cbor(self):
        await doit("cbor", "cbor")

    async def test_patch1_cbor_yaml(self):
        await doit("cbor", "yaml")

    async def test_patch1_yaml_cbor(self):
        await doit("yaml", "cbor")

    async def test_patch1_json_yaml(self):
        await doit("json", "yaml")

    async def test_patch1_yaml_json(self):
        await doit("yaml", "json")

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
    port = 8432
    args = ["--tcp-port", str(port)]
    dtps_server = DTPSServer.create()
    t = interpret_command_line_and_start(dtps_server, args)
    task = asyncio.create_task(t)
    await dtps_server.started.wait()

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
    url = f"http://localhost:{port}/config/"
    import aiohttp

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

    async with aiohttp.ClientSession() as session:
        async with session.patch(url, headers=headers, data=data) as resp:
            resp.raise_for_status()

    rd2 = oq.last_data()
    ob2 = rd2.get_as_native_object()

    logger.info(f"ob1={ob1!r}")
    logger.info(f"ob2={ob2!r}")

    if ob2 != ob2_expected:
        raise Exception(f"ob2={ob2!r} != ob2_expected={ob2_expected!r}")

    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass


# This allows running the tests with `nose2` command.
if __name__ == "__main__":
    unittest.main()
