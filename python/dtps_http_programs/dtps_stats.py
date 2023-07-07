import argparse
import asyncio
import functools
import json
import sys
import time
from dataclasses import asdict
from typing import cast, Optional

from dtps_http import DTPSClient, parse_url_unescape, RawData, URLString
from dtps_http.urls import URLIndexer, URLTopic
from . import logger

__all__ = [
    "dtps_stats_main",
]


async def listen_to_all_topics(urlbase0: URLString, *, inline_data: bool) -> None:
    url = cast(URLIndexer, parse_url_unescape(urlbase0))

    def new_observation(topic_name: str, data: RawData) -> None:
        current = time.time_ns()
        if "clock" not in topic_name:
            return
        j = json.loads(data.content.decode())

        diff = current - j
        # convert nanoseconds to milliseconds
        diff_ms = diff / 1000000.0
        logger.info(f"{topic_name=}: latency {diff_ms:.3f} ms")

    subcriptions: list[asyncio.Task[None]] = []
    async with DTPSClient.create() as dtpsclient:
        available = await dtpsclient.ask_topics(url)

        for name, desc in available.items():
            # list_urls = "".join(f"\t{u} \n" for u in desc.urls)
            logger.info(
                f"Found topic {name!r}:\n"
                + json.dumps(asdict(desc), indent=2)
                + "\n"
                # + f"unique_id: {desc.unique_id}\n"
                # + f"origin_node: {desc.origin_node}\n"
                # + f"forwarders: {desc.forwarders}\n"
            )

            url = cast(URLTopic, await dtpsclient.choose_best(desc.reachability))
            t = await dtpsclient.listen_url(
                url, functools.partial(new_observation, name), inline_data=inline_data
            )
            subcriptions.append(t)

        await asyncio.gather(*subcriptions)


def dtps_stats_main(args: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Connects to DTPS server and listens and subscribes to all topics"
    )
    parser.add_argument("--inline-data", default=False, action="store_true", help="Use inline data")
    parsed, rest = parser.parse_known_args(args=args)
    if len(rest) != 1:
        msg = f"Expected exactly one argument.\nObtained: {args!r}\n"
        logger.error(msg)
        sys.exit(2)

    urlbase = URLString(rest[0])

    use_inline_data = parsed.inline_data

    f = listen_to_all_topics(urlbase, inline_data=use_inline_data)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(f)
