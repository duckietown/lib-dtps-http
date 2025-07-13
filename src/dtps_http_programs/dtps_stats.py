"""DTPS statistics."""

__all__ = ["dtps_stats_main"]

import argparse
import asyncio
import sys
import time
from typing import Any, cast

from dtps_http import (
    DTPSClient,
    RawData,
    TopicNameV,
    URLIndexer,
    URLString,
    URLTopic,
    parse_url_unescape,
    pretty,
)
from dtps_http_programs import logger

LAST_MAXIMUM_LENGTH = 10


def new_observation(i: int, last: list[float], topic_name: TopicNameV) -> Any:
    def wrapper(data: RawData) -> None:
        nonlocal i
        time_ns = time.time_ns()
        if "clock" not in topic_name.as_relative_url():
            return
        decoded_data_content = data.content.decode()
        j = int(decoded_data_content)
        difference = time_ns - j
        # convert nanoseconds to milliseconds
        difference_ms = difference / 1_000_000
        if i > 0:
            last.append(difference_ms)
        i += 1
        if len(last) > LAST_MAXIMUM_LENGTH:
            last.pop(0)
        if last:
            min_ = min(last)
            max_ = max(last)
            last_length = len(last)
            last_summation = sum(last)
            avg = last_summation / last_length
            dash_separated_topic_name = topic_name.as_dash_sep()
            message = (
                "%24s: latency %.3fms  [last %s  mean: %.3fms min: %.3fms max:"
                " %.3fms]"
            )
            logger.info(
                message,
                dash_separated_topic_name,
                difference_ms,
                last_length,
                avg,
                min_,
                max_,
            )

    return wrapper


async def listen_to_all_topics(
    urlbase0: URLString,
    *,
    inline_data: bool,
) -> None:
    url = parse_url_unescape(urlbase0)
    url_indexer = cast(URLIndexer, url)
    i = 0
    last: list[float] = []
    subcriptions = []
    async with DTPSClient.create() as dtps_client:
        available = await dtps_client.ask_index(url_indexer)
        for topic_name, topic_reference in available.topics.items():
            pretty_topic_reference = pretty(topic_reference)
            logger.info(
                "Found topic %r:\n%s\n",
                topic_name,
                pretty_topic_reference,
            )
            best_alternative = await dtps_client.choose_best_alternative(
                topic_reference.reachability,
            )
            url_topic = cast(URLTopic, best_alternative)
            callback = new_observation(i, last, topic_name)
            listen_data_interface = await dtps_client.listen_url(
                url_topic,
                callback,
                inline_data=inline_data,
                raise_on_error=False,
                max_frequency=None,
            )
            coroutine = listen_data_interface.wait_for_done()
            task = asyncio.create_task(coroutine)
            subcriptions.append(task)
        await asyncio.gather(*subcriptions)


def dtps_stats_main(args: list[str] | None = None) -> None:
    """Run DTPS statistics."""
    description = (
        "Connects to a DTPS server and then listens and subscribes to all "
        "topics."
    )
    parser = argparse.ArgumentParser(
        description=description,
    )
    parser.add_argument(
        "--inline-data",
        default=False,
        action="store_true",
        help="Use inline data",
    )
    parsed, rest = parser.parse_known_args(args=args)
    if len(rest) != 1:
        message = f"Expected exactly one argument.\nObtained: {args!r}\n"
        logger.exception(message)
        sys.exit(2)
    urlbase = URLString(rest[0])
    use_inline_data = parsed.inline_data
    future = listen_to_all_topics(urlbase, inline_data=use_inline_data)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(future)
