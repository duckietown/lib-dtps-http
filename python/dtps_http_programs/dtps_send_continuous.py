import argparse
import asyncio
import json
from dataclasses import asdict
from typing import Optional

from dtps_http import DTPSClient, RawData, URLString
from . import logger

__all__ = ["dtps_send_continuous_main", ]


async def send_continuous(urlbase0: URLString) -> None:
    async with DTPSClient.create() as dtpsclient:
        md = await dtpsclient.get_metadata(urlbase0)
        logger.info(f"Metadata for {urlbase0!r}:\n" + json.dumps(asdict(md),
                                                                 indent=2))  # available = await dtpsclient.ask_topics(urlbase0)
        async with dtpsclient.push_through_websocket(md.events_url) as d:
            # rd = RawData(b'hello!', content_type="text/plain")
            while True:
                await d.push_through(b'hello!', content_type="text/plain")

                await asyncio.sleep(1.0)


def dtps_send_continuous_main(args: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Connects to DTPS server and pushes through websocket")
    parser.add_argument("--url", required=True, help="Topic URL inline data")
    parsed = parser.parse_args(args=args)
    url = parsed.url

    f = send_continuous(url)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(f)
