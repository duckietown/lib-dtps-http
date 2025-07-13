"""DTPS send continuous."""

__all__ = ["dtps_send_continuous_main"]

import argparse
import asyncio

from dtps_http import (
    URL,
    ContentType,
    DTPSClient,
    URLString,
    parse_url_unescape,
)


async def send_continuous(urlbase0: URL) -> None:
    async with DTPSClient.create() as dtps_client:
        metadata = await dtps_client.get_metadata(urlbase0)
        if metadata.events_url is None:
            message = "No events URL."
            raise Exception(message)
        async with dtps_client.push_through_websocket(
            metadata.events_url,
        ) as push_interface:
            while True:
                content_type = ContentType("text/plain")
                await push_interface.push_through(b"hello!", content_type)
                await asyncio.sleep(1)


def dtps_send_continuous_main(args: list[str] | None = None) -> None:
    """Run DTPS send continuous."""
    description = (
        "Connects to a DTPS server and then pushes through a websocket."
    )
    parser = argparse.ArgumentParser(
        description=description,
    )
    parser.add_argument("--url", required=True, help="Topic URL inline data")
    parsed = parser.parse_args(args=args)
    url = parsed.url
    url_string = URLString(url)
    url = parse_url_unescape(url_string)
    future = send_continuous(url)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(future)
