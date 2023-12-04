import argparse
import asyncio
from typing import Dict, List, Optional

from pydantic.dataclasses import dataclass

from dtps_http import (
    async_error_catcher,
    DTPSServer,
    interpret_command_line_and_start,
    TopicNameV,
    URLString,
)

__all__ = [
    "dtps_proxy_main",
]


@dataclass
class ProxyNamed:
    index_url: URLString
    topic_name: TopicNameV


@dataclass
class ProxyConfig:
    proxied: Dict[TopicNameV, ProxyNamed]


@async_error_catcher
async def go_proxy(args: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument("--add-prefix", type=str, default="proxied", required=False)
    parser.add_argument("--url", required=True)
    parser.add_argument("--mask-origin", default=False, action="store_true")

    parsed, args2 = parser.parse_known_args(args)

    urlbase = parsed.url
    mask_origin = parsed.mask_origin
    dtps_server = DTPSServer.create()

    t = interpret_command_line_and_start(dtps_server, args2)
    server_task = asyncio.create_task(t)
    await dtps_server.started.wait()

    # url0 = cast(URLIndexer, parse_url_unescape(urlbase))
    # previously_seen: Set[TopicNameV] = set()
    use_prefix = TopicNameV.from_dash_sep(parsed.add_prefix)

    await dtps_server.expose(use_prefix, None, urls=[urlbase], mask_origin=mask_origin)

    never = asyncio.Event()
    await never.wait()


def dtps_proxy_main(args: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Connects to DTPS server and listens and subscribes to all topics"
    )

    parsed, rest = parser.parse_known_args(args)

    f = go_proxy(rest)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(f)
