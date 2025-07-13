"""Memory test."""

import asyncio
import gc
from typing import Any

from pympler import muppy, summary, tracker

from dtps_http import (
    MIME_OCTET,
    ContentInfo,
    DTPSServer,
    RawData,
    TopicNameV,
    interpret_command_line_and_start,
)
from dtps_http.structures import Bounds
from dtps_http.utils_every_once_in_a_while import EveryOnceInAWhile
from python_benchmark_scripts import logger
from python_benchmark_scripts.utils import generate_random_string

LENGTH = 1024 * 1024
MAXIMUM_OBJECT_LENGTH_LENGTHS = (500, 50000, 1500)
PUBLISH_PERIOD = 0.001


async def async_main() -> None:
    """Run asynchronously."""
    server = DTPSServer.create([], enable_clock=False)
    args = ["--unix-path", "/tmp/test_memory.sock"]
    coroutine = interpret_command_line_and_start(server, args)
    asyncio.create_task(coroutine)
    await asyncio.sleep(1)
    topic_name = TopicNameV.from_dash_sep("topic1")
    content_info = ContentInfo.simple(MIME_OCTET)
    bounds = Bounds.max_length(2)
    object_queue = await server.create_object_queue(
        topic_name,
        content_info,
        topic_properties=None,
        bounds=bounds,
    )
    every_once_in_a_while = EveryOnceInAWhile(10)
    number_of_bytes = 0
    number_of_messages = 0
    blob_manager = server.blob_manager
    random_string = generate_random_string(LENGTH)
    encoded_random_string = random_string.encode("utf-8")
    summary_tracker = tracker.SummaryTracker()
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
            await object_queue.publish(raw_data)
            number_of_messages += 1
        if every_once_in_a_while.now():
            blob_manager.clean_up_blobs()
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
                        if object_length > MAXIMUM_OBJECT_LENGTH_LENGTHS[1]:
                            logger.info("First element: %s", object_[0])
                            logger.info("Last element: %s", object_[-1])
                if (
                    isinstance(object_, dict)
                    and object_length > MAXIMUM_OBJECT_LENGTH_LENGTHS[2]
                ):
                    object_list = list(object_)
                    first_keys = object_list[:4]
                    last_keys = object_list[-4:]
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
                        if referrer is not objects and referrer is not object_
                    ]
                    referrer_classes = [
                        type(referrer) for referrer in referrers
                    ]
                    logger.info(f"Referrences to `dict`: {referrer_classes}")
            objects = None
            number_of_megabytes = number_of_bytes / LENGTH
            logger.info(
                "Pushed %s with %.1f MB.",
                number_of_messages,
                number_of_megabytes,
            )
            object_queue_saved_length = len(object_queue.saved)
            logger.info(
                "len(object_queue.saved)=%s",
                object_queue_saved_length,
            )
            object_queue_stored_length = len(object_queue.stored)
            logger.info(
                "len(object_queue.stored)=%s",
                object_queue_stored_length,
            )
            blob_manager_blobs_length = len(blob_manager.blobs)
            logger.info(
                "len(blob_manager.blobs)=%s",
                blob_manager_blobs_length,
            )
            blob_manager_blobs_forgotten_length = len(
                blob_manager.blobs_forgotten,
            )
            logger.info(
                "len(blob_manager.blobs_forgotten)=%s",
                blob_manager_blobs_forgotten_length,
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
