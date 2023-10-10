import argparse
import asyncio

from dtps_http import (
    async_error_catcher, ContentInfo, DTPSServer, interpret_command_line_and_start, logger, MIME_JSON,
    TopicNameV,
)


@async_error_catcher
async def on_startup(s: DTPSServer) -> None:
    # We create a topic.
    # Topic name is given by the class TopicNameV which has parsing funcvtions
    OUT = TopicNameV.from_dash_sep("node/out")
    # We create the output queue.
    # You can give a lot of details
    queue_out = await s.create_oq(OUT, content_info=ContentInfo.simple(MIME_JSON))

    # We create a task that periodically publishes to the queue
    @async_error_catcher
    async def periodic_publish():
        i = 0
        while True:
            await asyncio.sleep(1)
            # We publish a json object
            queue_out.publish_json({"counter": i})
            i += 1
            logger.info(f"Published {i}")

    asyncio.create_task(periodic_publish())


def simple_publisher() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True, help="Topic name")
    parsed, args = parser.parse_known_args()

    # We create a server and give it the on_startup callback to do work when
    # the server is ready
    dtps_server = DTPSServer.create(on_startup=[on_startup])
    asyncio.run(interpret_command_line_and_start(dtps_server, args))


if __name__ == '__main__':
    simple_publisher()
