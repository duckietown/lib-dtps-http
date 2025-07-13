"""Utilities."""

__all__ = ["TEST_APP_DATA", "create_rust_server", "create_use_pair"]

import asyncio
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from dtps import context_cleanup
from dtps.ergo_abstract import AbstractDTPSContext
from dtps_http import (
    make_http_unix_url,
    parse_url_unescape,
    url_to_string,
    wait_for_unix_socket,
)
from dtps_tests import logger

TEST_APP_DATA = {
    "bytes": b"hello",
}


@asynccontextmanager
async def create_python_at_known_socket(
    socket_node: str,
    testname: str,
) -> AsyncIterator[tuple[AbstractDTPSContext, AbstractDTPSContext]]:
    url_node = make_http_unix_url(socket_node)
    url_node_string = url_to_string(url_node)
    parse_url_unescape(url_node_string)
    c1 = f"{testname}server"
    c2 = f"{testname}client"
    environment = {
        f"DTPS_BASE_{c1}": f"create:{url_node_string}",
        f"DTPS_BASE_{c2}": f"{url_node_string}",
    }
    logger.info("Environment: %s", environment)
    async with (
        context_cleanup(c1, environment) as context_rpcserver,
        context_cleanup(c2, environment) as context_rpcclient,
    ):
        yield context_rpcserver, context_rpcclient


@asynccontextmanager
async def create_use(
    *,
    name: str,
    socket_node: str,
) -> AsyncIterator[AbstractDTPSContext]:
    url_node = make_http_unix_url(socket_node)
    url_node_string = url_to_string(url_node)
    parse_url_unescape(url_node_string)
    c1 = f"{name}use"
    environment = {
        f"DTPS_BASE_{c1}": f"{url_node_string}",
    }
    async with context_cleanup(c1, environment) as context_rpcserver:
        yield context_rpcserver


@asynccontextmanager
async def create_use_pair(
    testname: str,
) -> AsyncIterator[tuple[AbstractDTPSContext, AbstractDTPSContext]]:
    """Create use pair."""
    if "_" in testname:
        message = f"testname cannot contain underscore: {testname}"
        raise ValueError(message)
    with tempfile.TemporaryDirectory() as temporary_directory:
        path = Path(temporary_directory) / testname
        socket_node = path.as_posix()
        async with create_python_at_known_socket(socket_node, testname) as (
            context_rpcserver,
            context_rpcclient,
        ):
            yield context_rpcserver, context_rpcclient


@asynccontextmanager
async def create_rust_server(
    testname: str,
) -> AsyncIterator[AbstractDTPSContext]:
    """Create Rust server."""
    if "_" in testname:
        message = f"testname cannot contain underscore: {testname}"
        raise ValueError(message)
    with tempfile.TemporaryDirectory() as temporary_directory:
        path = Path(temporary_directory) / testname
        socket_node = path.as_posix()
        url_node = make_http_unix_url(socket_node)
        url_node_string = url_to_string(url_node)
        parse_url_unescape(url_node_string)
        cmd = (
            "cargo",
            "run",
            "--bin",
            "dtps-http-rs-server",
            "--",
            "--unix-path",
            socket_node,
        )
        process = await asyncio.create_subprocess_exec(*cmd)
        try:
            future = wait_for_unix_socket(socket_node)
            await asyncio.wait_for(future, 60)
            c1 = f"{testname}rust"
            environment = {
                f"DTPS_BASE_{c1}": f"{url_node_string}",
            }
            logger.info(f"environment: {environment}")
            async with context_cleanup(c1, environment) as context_rust:
                yield context_rust
        finally:
            try:
                process.kill()
            except Exception:
                logger.warning("Cannot kill Rust process.")
