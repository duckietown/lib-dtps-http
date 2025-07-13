"""Two-flows publisher."""

import argparse
import asyncio

from dtps_http import (
    MIME_JSON,
    ContentInfo,
    DTPSServer,
    ObjectQueue,
    TopicNameV,
    async_error_catcher,
    interpret_command_line_and_start,
)


@async_error_catcher
async def periodic_publish(queue_out: ObjectQueue, period: float) -> None:
    """Publish periodically."""
    i = 0
    while True:
        await asyncio.sleep(period)
        await queue_out.publish_json(
            {
                "counter": i,
            },
        )
        i += 1


@async_error_catcher
async def on_startup(server: DTPSServer) -> None:
    """Run on startup."""
    # Create 3 topics node/out/X from this prefix
    prefix = TopicNameV.from_dash_sep("node/out")
    # Each topic has a different period
    topic_to_period = {
        "slow": 15,
        "medium": 5,
        "fast": 1,
    }
    for name, period in topic_to_period.items():
        # The topic name is a concatenation of the prefix and name
        # (overload of __plus__)
        topic_name = prefix + TopicNameV.from_dash_sep(name)
        # Creates the queue for the topic
        content_info = ContentInfo.simple(MIME_JSON)
        queue_out = await server.create_object_queue(
            topic_name,
            content_info,
            topic_properties=None,
            bounds=None,
        )
        # Creates the task that periodically publishes to the queue
        coroutine = periodic_publish(queue_out, period)
        asyncio.create_task(coroutine)


def simple_publisher() -> None:
    """Run simple publisher."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True, help="Topic name")
    _, args = parser.parse_known_args()
    # Create a server and give it the `on_startup` callback to do work
    # when the server is setup
    dtps_server = DTPSServer.create(on_startup=[on_startup])
    # Let `asyncio` run the program
    coroutine = interpret_command_line_and_start(dtps_server, args)
    asyncio.run(coroutine)


if __name__ == "__main__":
    simple_publisher()
