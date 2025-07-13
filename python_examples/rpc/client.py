"""Client."""

import argparse
import asyncio

from aiohttp import ClientResponseError
from utils import read_continuous

from dtps_http import (
    URL,
    DTPSClient,
    RawData,
    URLString,
    logger,
    parse_url_unescape,
)


async def go(url: URL) -> None:
    """Go."""
    coroutine = read_continuous(url)
    asyncio.create_task(coroutine)
    async with DTPSClient.create() as dtps_client:
        i = 0
        while True:
            i += 1
            await asyncio.sleep(1)
            try:
                raw_data = RawData.cbor_from_native_object(i)
                await dtps_client.publish(url, raw_data)
            except ClientResponseError as e:
                logger.info("Error publishing: %s %s", i, e)
            else:
                logger.info("Published %s", i)


def subscribe_main() -> None:
    """Subscribe."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="Topic URL")
    parsed = parser.parse_args()
    # Use `parse_url_unescape` to handle special unix socket URLs with
    # escaped slashes
    url_string = URLString(parsed.url)
    url = parse_url_unescape(url_string)
    coroutine = go(url)
    asyncio.run(coroutine)


if __name__ == "__main__":
    subscribe_main()
