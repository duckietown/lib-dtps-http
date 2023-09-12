import argparse
import asyncio
from typing import List, Optional

from dtps_http import async_error_catcher, DTPSClient, URLString
from dtps_http.types import NodeID
from dtps_http.urls import URL, parse_url_unescape
from . import logger
from rich import print

__all__ = [
    "dtps_listen_main",
]


@async_error_catcher
async def dtps_listen_main_f(url: URL, expect_node: Optional[NodeID], switch_identity_ok: bool) -> None:
    async with DTPSClient.create() as client:
        async for d in client.listen_continuous(url, expect_node, switch_identity_ok):
            print(d)


def dtps_listen_main(args: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Listens to a DTPS source using websockets")
    parser.add_argument("--url", required=True, help="Topic URL inline data")
    parser.add_argument("--expect", help="Expected node id")
    parsed = parser.parse_args(args=args)
    url = parse_url_unescape(parsed.url)

    f = dtps_listen_main_f(url, expect_node=parsed.expect, switch_identity_ok=True)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(f)
