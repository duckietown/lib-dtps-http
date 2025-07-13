"""DTPS proxy."""

__all__ = ["dtps_proxy_main"]

import argparse
import asyncio

from pydantic.dataclasses import dataclass

from dtps_http import (
    DTPSServer,
    TopicNameV,
    URLString,
    async_error_catcher,
)


@dataclass
class ProxyConfig:
    proxied: dict[TopicNameV, "ProxyNamed"]


@dataclass
class ProxyNamed:
    index_url: URLString
    topic_name: TopicNameV


def dtps_proxy_main(args: list[str] | None = None) -> None:
    """Run DTPS proxy."""
    description = (
        "Connects to a DTPS server and then listens and subscribes to all "
        "topics."
    )
    parser = argparse.ArgumentParser(description=description)
    _, rest = parser.parse_known_args(args)
    future = go_proxy(rest)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(future)


@async_error_catcher
async def go_proxy(args: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--add-prefix",
        type=str,
        default="proxied",
        required=False,
    )
    parser.add_argument("--url", required=True)
    parser.add_argument("--mask-origin", default=False, action="store_true")
    parsed, _ = parser.parse_known_args(args)
    urlbase = parsed.url
    mask_origin = parsed.mask_origin
    dtps_server = DTPSServer.create()
    await dtps_server.started.wait()
    use_prefix = TopicNameV.from_dash_sep(parsed.add_prefix)
    await dtps_server.expose(
        use_prefix,
        None,
        urls=[urlbase],
        mask_origin=mask_origin,
    )
    never = asyncio.Event()
    await never.wait()
