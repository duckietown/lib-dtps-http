import argparse
import asyncio
import functools
import json
import sys
import time
from dataclasses import asdict
from typing import cast, List, Optional

from dtps_http import DTPSClient, parse_url_unescape, RawData, TopicNameV, URLIndexer, URLString, URLTopic
from . import logger

__all__ = [
    "dtps_stats_main",
]


async def listen_to_all_topics(urlbase0: URLString, *, inline_data: bool) -> None:
    url = cast(URLIndexer, parse_url_unescape(urlbase0))
    last = []
    i = 0

    async def new_observation(topic_name: TopicNameV, data: RawData) -> None:
        nonlocal i

        current = time.time_ns()
        if "clock" not in topic_name.as_relative_url():
            return

        j = int(data.content.decode())
        # j = json.loads(data.content.decode())

        diff = current - j
        # convert nanoseconds to milliseconds
        diff_ms = diff / 1_000_000.0
        if i > 0:
            last.append(diff_ms)
        i += 1
        if len(last) > 10:
            last.pop(0)
        if last:
            min_ = min(last)
            max_ = max(last)
            avg = sum(last) / len(last)

            logger.info(
                f"{topic_name.as_dash_sep():24}: latency {diff_ms:.3f}ms  [last {len(last)}  mean: "
                f"{avg:.3f}ms" + f" min: {min_:.3f}ms max: {max_:.3f}ms]"
            )

    subcriptions: "List[asyncio.Task[None]]" = []
    async with DTPSClient.create() as dtpsclient:
        available = await dtpsclient.ask_index(url)

        for name, desc in available.topics.items():
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
            ldi = await dtpsclient.listen_url(
                url,
                functools.partial(new_observation, name),
                inline_data=inline_data,
                raise_on_error=False,
            )
            t = asyncio.create_task(ldi.wait_for_done())
            subcriptions.append(t)

        await asyncio.gather(*subcriptions)


def dtps_stats_main(args: Optional[List[str]] = None) -> None:
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
