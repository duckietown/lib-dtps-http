import asyncio.subprocess
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
    wait_for_unix_socket,
)
from dtps_tests import logger

__all__ = [
    "create_rust_server",
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


@asynccontextmanager
async def create_rust_server(testname: str) -> AsyncIterator[DTPSContext]:
    if "_" in testname:
        raise ValueError(f"testname cannot contain underscore: {testname}")
    with tempfile.TemporaryDirectory() as td:
        socket_node = os.path.join(td, testname)

        url_node = make_http_unix_url(socket_node)
        url_node_s = url_to_string(url_node)
        parse_url_unescape(url_node_s)

        cmd = f"cargo run --bin dtps-http-rs-server -- --unix-path {socket_node}"
        p = await asyncio.create_subprocess_shell(
            cmd,
        )
        try:
            await asyncio.wait_for(wait_for_unix_socket(socket_node), timeout=5)

            c1 = f"{testname}rust"
            environment = {
                f"DTPS_BASE_{c1}": f"{url_node_s}",
            }
            logger.info(f"environment: {environment}")
            async with context_cleanup(c1, environment) as context_rust:
                yield context_rust

        except Exception as e:
            logger.error(f"create_rust_server: {e}")
            p.terminate()
            raise
