import unittest
import asyncio

from dtps_http import DTPSServer, interpret_command_line_and_start
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

        self.loop.run_until_complete(doit())

    def tearDown(self):
        self.loop.close()


# This allows running the tests with `nose2` command.
if __name__ == "__main__":
    unittest.main()
