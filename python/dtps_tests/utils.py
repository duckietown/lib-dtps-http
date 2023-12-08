import os
import tempfile
from contextlib import asynccontextmanager
from typing import AsyncIterator, Tuple

from dtps import context_cleanup
from dtps.ergo_ui import DTPSContext
from dtps_http import (
    make_http_unix_url,
    parse_url_unescape,
    url_to_string,
)
from dtps_tests import logger

__all__ = [
    "create_use_pair",
]


@asynccontextmanager
async def create_use_pair(testname: str) -> AsyncIterator[Tuple[DTPSContext, DTPSContext]]:
    if "_" in testname:
        raise ValueError(f"testname cannot contain underscore: {testname}")
    with tempfile.TemporaryDirectory() as td:
        socket_node = os.path.join(td, testname)

        url_node = make_http_unix_url(socket_node)
        url_node_s = url_to_string(url_node)
        parse_url_unescape(url_node_s)

        c1 = f"{testname}server"
        c2 = f"{testname}client"
        environment = {
            f"DTPS_BASE_{c1}": f"create:{url_node_s}",
            f"DTPS_BASE_{c2}": f"{url_node_s}",
        }
        logger.info(f"environment: {environment}")
        async with context_cleanup(c1, environment) as context_rpcserver:
            async with context_cleanup(c2, environment) as context_rpcclient:
                yield context_rpcserver, context_rpcclient
