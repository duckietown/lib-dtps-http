import asyncio
import os
import tempfile
from unittest import IsolatedAsyncioTestCase

from dtps import context_cleanup
from dtps_http import (
    app_start,
    async_error_catcher,
    check_is_unix_socket,
    DTPSServer,
    make_http_unix_url,
    MIME_TEXT,
    RawData,
)
from dtps_http_tests.utils import test_timeout
from dtps_tests import logger


class TestExpose(IsolatedAsyncioTestCase):
    @test_timeout(20)
    @async_error_catcher
    async def test_expose(self):
        with tempfile.TemporaryDirectory() as td:
            socket_switchboard = os.path.join(td, "expose-switchboard")
            socket_node = os.path.join(td, "expose-node")

            url_switchboard = make_http_unix_url(socket_switchboard)
            url_node = make_http_unix_url(socket_node)

            logger.info(f"switchboard: {url_switchboard}")
            logger.info(f"node: {url_node}")

            switchboard = await app_start(
                DTPSServer.create(nickname="switchboard"),
                unix_paths=[socket_switchboard],
            )

            async with switchboard:
                environment = {
                    "DTPS_BASE_EXPOSENODE": f"create:{url_node}",
                    "DTPS_BASE_EXPOSESWITCHBOARD": f"{url_switchboard}",
                }
                logger.info(f"environment: {environment}")
                async with context_cleanup("exposenode", environment) as context_self:
                    check_is_unix_socket(socket_node)

                    async with context_cleanup("exposeswitchboard", environment) as context_switchboard:
                        out = context_self / "out"
                        await out.queue_create()
                        rd = RawData(content=b"hello", content_type=MIME_TEXT)
                        await out.publish(rd)
                        mountpoint = context_switchboard / "dtps" / "node" / "nodename"
                        await mountpoint.expose(context_self)
                        await asyncio.sleep(2)

                        out_mounted = mountpoint / "out"

                        found = await out_mounted.data_get()
                        if found.content != b"hello":
                            raise Exception("unexpected content")

                        logger.debug("ok, received")
                    logger.debug("switchboard use context cleaned")
                logger.debug("self contexst cleaned")

            logger.debug("switchboard server cleaned")
