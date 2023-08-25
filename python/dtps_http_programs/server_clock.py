import asyncio
import time
from typing import Optional

from aiohttp import web

from dtps_http import async_error_catcher, DTPSServer, interpret_command_line_and_start, RawData, TopicNameV
from . import logger

__all__ = [
    "clock_main",
    "get_clock_app",
]


@async_error_catcher
async def run_clock(s: DTPSServer, topic_name: TopicNameV, interval: float, initial_delay: float) -> None:
    await asyncio.sleep(initial_delay)
    logger.info(f"Starting clock {topic_name.as_relative_url()} with interval {interval}")
    oq = await s.create_oq(topic_name)
    while True:
        t = time.time_ns()
        data = str(t).encode()
        oq.publish(RawData(data, "application/json"))
        await asyncio.sleep(interval)


async def on_clock_startup(s: DTPSServer) -> None:
    s.remember_task(asyncio.create_task(run_clock(s, TopicNameV.from_relative_url("clock"), 1.0, 0.0)))
    s.remember_task(asyncio.create_task(run_clock(s, TopicNameV.from_relative_url("clock5"), 5.0, 0.0)))
    s.remember_task(asyncio.create_task(run_clock(s, TopicNameV.from_relative_url("clock7"), 7.0, 7.0)))
    s.remember_task(asyncio.create_task(run_clock(s, TopicNameV.from_relative_url("clock11"), 11.0, 20.0)))


def get_clock_dtps() -> DTPSServer:
    s = DTPSServer(topics_prefix=TopicNameV.root(), on_startup=[on_clock_startup])
    return s


def get_clock_app() -> web.Application:
    s = get_clock_dtps()
    return s.app


def clock_main(args: Optional[list[str]] = None) -> None:
    dtps_server = get_clock_dtps()
    asyncio.run(interpret_command_line_and_start(dtps_server, args))
