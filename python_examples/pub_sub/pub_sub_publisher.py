"""Publisher-subscriber publisher."""

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
    logger,
)


# We create a task that periodically publishes to the queue
@async_error_catcher
async def periodic_publish(queue_out: ObjectQueue) -> None:
    """Publish periodically."""
    i = 0
    while True:
        await asyncio.sleep(1)
        # We publish a json object
        await queue_out.publish_json(
            {
                "counter": i,
            },
        )
        i += 1
        logger.info("Published %s", i)


@async_error_catcher
async def on_startup(server: DTPSServer) -> None:
    """Run on startup."""
    # Create a topic.
    # The topic name is given by the class "TopicNameV" which has
    # parsing funcvtions
    topic_name = TopicNameV.from_dash_sep("node/out")
    # Create the output queue
    queue_out = await server.create_object_queue(
        topic_name,
        ContentInfo.simple(MIME_JSON),
        topic_properties=None,
        bounds=None,
    )
    coroutine = periodic_publish(queue_out)
    asyncio.create_task(coroutine)


def simple_publisher() -> None:
    """Run simple publisher."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True, help="Topic name")
    _, args = parser.parse_known_args()
    # Create a server and give it the `on_startup`` callback to do work
    # when the server is ready
    server = DTPSServer.create(on_startup=[on_startup])
    coroutine = interpret_command_line_and_start(server, args)
    asyncio.run(coroutine)


if __name__ == "__main__":
    simple_publisher()
