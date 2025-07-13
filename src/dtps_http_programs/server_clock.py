"""Server clock."""

__all__ = [
    "clock_main",
    "get_clock_app",
    "server_main",
]

import asyncio
import time

from aiohttp import web

from dtps_http import (
    MIME_JSON,
    ContentInfo,
    DTPSServer,
    TopicNameV,
    async_error_catcher,
    interpret_command_line_and_start,
)
from dtps_http_programs import logger


@async_error_catcher
async def run_clock(
    server: DTPSServer,
    topic_name: TopicNameV,
    interval: float,
    initial_delay: float,
) -> None:
    await asyncio.sleep(initial_delay)
    relative_url = topic_name.as_relative_url()
    logger.info(
        "Starting clock %s with interval %s",
        relative_url,
        interval,
    )
    content_info = ContentInfo.simple(MIME_JSON)
    object_queue = await server.create_object_queue(
        topic_name,
        content_info,
        topic_properties=None,
        bounds=None,
    )
    while True:
        time_ns = time.time_ns()
        await object_queue.publish_json(time_ns)
        await asyncio.sleep(interval)


async def on_clock_startup(server: DTPSServer) -> None:
    clocks = {
        "clock": {
            "interval": 1,
            "initial_delay": 0,
        },
        "clock5": {
            "interval": 5,
            "initial_delay": 0,
        },
        "clock7": {
            "interval": 7,
            "initial_delay": 7,
        },
        "clock11": {
            "interval": 11,
            "initial_delay": 20,
        },
    }
    for clock, interval_and_initial_delay in clocks.items():
        topic_name = TopicNameV.from_dash_sep(clock)
        coroutine = run_clock(
            server,
            topic_name,
            interval_and_initial_delay["interval"],
            interval_and_initial_delay["initial_delay"],
        )
        task = asyncio.create_task(coroutine)
        server.remember_task(task)


def get_clock_dtps() -> DTPSServer:
    return DTPSServer.create(on_startup=[on_clock_startup])


def get_clock_app() -> web.Application:
    """Return clock app."""
    server = get_clock_dtps()
    return server.app


def clock_main(args: list[str] | None = None) -> None:
    """Run clock."""
    dtps_server = get_clock_dtps()
    coroutine = interpret_command_line_and_start(dtps_server, args)
    asyncio.run(coroutine)


def server_main(args: list[str] | None = None) -> None:
    """Run server."""
    server = DTPSServer.create(on_startup=[])
    coroutine = interpret_command_line_and_start(server, args)
    asyncio.run(coroutine)
