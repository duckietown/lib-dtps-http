"""Memory dashboard test."""

import asyncio
import math
import time
from typing import Any

from dtps import context_cleanup
from dtps_http import MIME_OCTET, Bounds, RawData
from dtps_http.utils_every_once_in_a_while import EveryOnceInAWhile
from python_benchmark_scripts import logger
from python_benchmark_scripts.utils import generate_random_string

LENGTH = 1024 * 1024
MAXIMUM_DELTA_TIMES_LENGTH = 10
MAXIMUM_NUMBER_OF_MESSAGES = 100000
PUBLISH_PERIOD = 0.01


def get_on_data(number_received: int, received_bytes: int) -> Any:
    """Return `on_data`."""

    async def on_data(raw_data: RawData) -> None:
        nonlocal number_received, received_bytes
        number_received += 1
        received_bytes += len(raw_data.content)

    return on_data


async def async_main() -> None:
    """Run asynchronously."""
    every_once = EveryOnceInAWhile(2)
    number_of_bytes = 0
    number_of_messages = 0
    random_string = generate_random_string(LENGTH)
    encoded_random_string = random_string.encode("utf-8")
    environment = {
        "DTPS_BASE_SWITCHBOARD": "http+unix://%2Ftmp%2Fdashboard/",
        "DTPS_BASE_NODE": "http+unix://%2Ftmp%2Fnode/",
    }
    delta_times = []
    async with (
        context_cleanup("node", environment) as node,
        context_cleanup("switchboard", environment) as context,
    ):
        bounds = Bounds.max_length(1)
        topic_orig = node / "topic1"
        await topic_orig.queue_create(bounds=bounds)
        topic = context / "topic2"
        await topic.expose(topic_orig, mask_origin=True)
        number_received = 0
        received_bytes = 0
        on_data = get_on_data(number_received, received_bytes)
        await topic.subscribe(on_data)
        async with topic.publisher_context() as publisher:
            while True:
                await asyncio.sleep(PUBLISH_PERIOD)
                if number_of_messages <= MAXIMUM_NUMBER_OF_MESSAGES:
                    # Create a random string and publish it
                    encoded_number_of_messages_string = (
                        f"{number_of_messages:10d}".encode()
                    )
                    random_string_data = (
                        encoded_random_string
                        + encoded_number_of_messages_string
                    )
                    number_of_bytes += len(random_string_data)
                    raw_data = RawData(
                        content=random_string_data,
                        content_type=MIME_OCTET,
                    )
                    start_time = time.monotonic()
                    await publisher.publish(raw_data)
                    end_time = time.monotonic()
                    delta_time = end_time - start_time
                    delta_times.append(delta_time)
                    number_of_messages += 1
                if every_once.now():
                    number_of_megabytes = number_of_bytes / LENGTH
                    logger.info(
                        "Pushed %s with %.1f MB",
                        number_of_messages,
                        number_of_megabytes,
                    )
                    if len(delta_times) > MAXIMUM_DELTA_TIMES_LENGTH:
                        last_delta_times = delta_times[-10:]
                        last_delta_times_summation = math.fsum(
                            last_delta_times,
                        )
                        last_delta_times_length = len(last_delta_times)
                        average = (
                            last_delta_times_summation
                            / last_delta_times_length
                        )
                        average_ms = average * 1000
                        logger.info(
                            "Average push delay: %.3fms",
                            average_ms,
                        )
                    received_megabytes = received_bytes / LENGTH
                    logger.info(
                        "Received %s messages with %.1f MB.",
                        number_received,
                        received_megabytes,
                    )


def server_main() -> None:
    """Run server."""
    coroutine = async_main()
    asyncio.run(coroutine)


if __name__ == "__main__":
    server_main()
