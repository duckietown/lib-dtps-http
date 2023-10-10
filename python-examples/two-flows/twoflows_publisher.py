import argparse
import asyncio

from dtps_http import (
    async_error_catcher, ContentInfo, DTPSServer, interpret_command_line_and_start, MIME_JSON,
    ObjectQueue, TopicNameV,
)


@async_error_catcher
async def periodic_publish(queue_out: ObjectQueue, period: float) -> None:
    i = 0
    while True:
        await asyncio.sleep(period)
        queue_out.publish_json({"counter": i})
        i += 1


@async_error_catcher
async def on_startup(s: DTPSServer) -> None:
    # we create 3 topics node/out/X from this prefix
    prefix = TopicNameV.from_dash_sep("node/out")
    # each topic has different period
    topic2period = {
        'slow': 15.0,
        'medium': 5.0,
        'fast': 1.0,

    }
    for name, period in topic2period.items():
        # Topic name is a concatenation of prefix and name (overload of __plus__)
        topic_name = prefix + TopicNameV.from_dash_sep(name)
        # Creates the queue for the topic
        queue_out = await s.create_oq(topic_name, content_info=ContentInfo.simple(MIME_JSON))
        # Creates the task that periodically publishes to the queue
        asyncio.create_task(periodic_publish(queue_out, period))


def simple_publisher() -> None:
    # We can parse some options for us
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True, help="Topic name")
    # Then we let DTPS parse its options later
    parsed, args = parser.parse_known_args()

    # We create a server and give it the on_startup callback to do work when
    # the server is setup
    dtps_server = DTPSServer.create(on_startup=[on_startup])

    # Let asyncio run the program
    asyncio.run(interpret_command_line_and_start(dtps_server, args))


if __name__ == '__main__':
    simple_publisher()
