import asyncio
import unittest

import jsonpatch

from dtps_http import ContentInfo, DTPSServer, interpret_command_line_and_start, MIME_JSON, TopicNameV
from . import logger


class TestAsyncServerFunction(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def test_static1(self):
        async def doit():
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

        self.loop.run_until_complete(doit())

    def test_patch1(self):
        async def doit():
            port = 8432
            args = ["--tcp-port", str(port)]
            dtps_server = DTPSServer.create()
            t = interpret_command_line_and_start(dtps_server, args)
            task = asyncio.create_task(t)
            await dtps_server.started.wait()

            topic = TopicNameV.from_relative_url("config/")
            oq = await dtps_server.create_oq(topic, content_info=ContentInfo.simple(MIME_JSON))
            ob1 = {"A": {"B": ["C", "D"]}}
            await oq.publish_json(ob1)
            logger.info(str(list(dtps_server._oqs.keys())))
            url = f"http://localhost:{port}/config/"
            import aiohttp

            patch = jsonpatch.JsonPatch(
                [
                    {"op": "add", "path": "/A/B/-", "value": "E"},
                ]
            )
            ob2_expected = {"A": {"B": ["C", "D", "E"]}}
            patch_json = patch.to_string()
            headers = {"Content-type": "application/json-patch+json"}

            async with aiohttp.ClientSession() as session:
                async with session.patch(url, headers=headers, data=patch_json) as resp:
                    logger.info(f"PATCH {url!r} status={resp.status}")
                    resp.raise_for_status()

            rd2 = oq.last_data()
            ob2 = rd2.get_as_native_object()

            logger.info(f"ob1={ob1!r}")
            logger.info(f"ob2={ob2!r}")

            self.assertEqual(ob2, ob2_expected)

            task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                pass

        self.loop.run_until_complete(doit())

    def tearDown(self):
        self.loop.close()


# This allows running the tests with `nose2` command.
if __name__ == "__main__":
    unittest.main()
