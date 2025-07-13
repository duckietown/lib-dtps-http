"""Memory use test."""

import asyncio
import gc
from typing import Any

from pympler import muppy, summary, tracker

from dtps_http import MIME_OCTET, Bounds, RawData
from dtps_http.utils_every_once_in_a_while import EveryOnceInAWhile
from dtps_tests.utils import create_use_pair
from python_benchmark_scripts import logger
from python_benchmark_scripts.utils import generate_random_string

LENGTH = 1024 * 1024
MAXIMUM_OBJECT_LENGTH_LENGTHS = (500, 50000, 1500)
PUBLISH_PERIOD = 0.001


async def on_data(_: RawData) -> None:
    """Run on data."""


async def async_main() -> None:
    """Run asynchronously."""
    async with create_use_pair("testuse") as (_, context_use):
        topic = context_use / "topic"
        bounds = Bounds.max_length(1)
        await topic.queue_create(
            bounds=bounds,
        )
        await topic.subscribe(on_data)
        every_once = EveryOnceInAWhile(10)
        number_of_bytes = 0
        number_of_messages = 0
        summary_tracker = tracker.SummaryTracker()
        random_string = generate_random_string(1024 * 100)
        encoded_random_string = random_string.encode("utf-8")
        objects: list[Any] | None
        all_objects_now: list[Any] | None
        while True:
            await asyncio.sleep(PUBLISH_PERIOD)
            # Create a random string and publish it
            if True:
                encoded_number_of_messages_string = (
                    f"{number_of_messages:10d}".encode()
                )
                random_string_data = (
                    encoded_random_string + encoded_number_of_messages_string
                )
                number_of_bytes += len(random_string_data)
                raw_data = RawData(
                    content=random_string_data,
                    content_type=MIME_OCTET,
                )
                await topic.publish(raw_data)
                number_of_messages += 1
            if every_once.now():
                objects = gc.get_objects()
                for object_ in objects:
                    if object_ is objects:
                        continue
                    if isinstance(object_, type | bytes):
                        continue
                    try:
                        object_length = len(object_)
                    except Exception:
                        logger.exception(
                            "Could not get length of object %s.",
                            object_,
                        )
                    else:
                        if object_length > MAXIMUM_OBJECT_LENGTH_LENGTHS[0]:
                            object_class = type(object_)
                            logger.info(
                                "Found %s of length %s.",
                                object_class,
                                object_length,
                            )
                            if (
                                object_length
                                > MAXIMUM_OBJECT_LENGTH_LENGTHS[1]
                            ):
                                logger.info("First element: %s", object_[0])
                                logger.info("Last element: %s", object_[-1])
                    if (
                        isinstance(object_, dict)
                        and object_length > MAXIMUM_OBJECT_LENGTH_LENGTHS[2]
                    ):
                        first_keys = list(object_)[:4]
                        last_keys = list(object_)[-4:]
                        message = (
                            "Found `dict` of length %s; first_keys=%s; "
                            "last_keys=%s"
                        )
                        logger.info(
                            message,
                            object_length,
                            first_keys,
                            last_keys,
                        )
                        referrers = gc.get_referrers(object_)
                        referrers = [
                            referrer
                            for referrer in referrers
                            if referrer is not objects
                            and referrer is not object_
                        ]
                        referrer_classes = [
                            type(referrer) for referrer in referrers
                        ]
                        logger.info(
                            "Referrences to `dict`: %s",
                            referrer_classes,
                        )
                objects = None
                number_of_megabytes = number_of_bytes / LENGTH
                logger.info(
                    "Pushed %s with %.1f MB.",
                    number_of_messages,
                    number_of_megabytes,
                )
                if True:
                    logger.info("New objects since last time:")
                    summary_tracker.print_diff()
                    all_objects_now = muppy.get_objects()
                    summary_ = summary.summarize(all_objects_now)
                    logger.info("Current summary:")
                    summary.print_(summary_, limit=50)
                    summary_ = None
                    all_objects_now = None


def server_main() -> None:
    """Run server."""
    coroutine = async_main()
    asyncio.run(coroutine)


if __name__ == "__main__":
    server_main()
