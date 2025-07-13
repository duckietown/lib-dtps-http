"""Filtering filter."""

__all__ = ["dtps_example_manual_filter_main"]


import asyncio
from typing import Any

from dtps_http import (
    MIME_JSON,
    Bounds,
    ContentInfo,
    DTPSServer,
    InsertNotification,
    ObjectQueue,
    TopicNameV,
    TopicProperties,
    async_error_catcher,
    interpret_command_line_and_start,
    logger,
)


def get_on_received_in(queue_out: ObjectQueue) -> Any:
    @async_error_catcher
    async def on_received_in(
        _: ObjectQueue,
        insert_notification: InsertNotification,
    ) -> None:
        await queue_out.publish(insert_notification.raw_data)

    return on_received_in


@async_error_catcher
async def on_startup(server: DTPSServer) -> None:
    topic_name = TopicNameV.from_dash_sep("node/in")
    content_info = ContentInfo.simple(MIME_JSON)
    topic_properties = TopicProperties.rw_pushable()
    bounds = Bounds.max_length(2)
    queue_in = await server.create_object_queue(
        topic_name,
        content_info,
        topic_properties=topic_properties,
        bounds=bounds,
    )
    topic_name = TopicNameV.from_dash_sep("node/out")
    topic_properties = TopicProperties.streamable_readonly()
    queue_out = await server.create_object_queue(
        topic_name,
        content_info,
        topic_properties=topic_properties,
        bounds=bounds,
    )
    on_received_in = get_on_received_in(queue_out)
    queue_in.subscribe(on_received_in)
    await queue_in.publish_json("First message.")


def dtps_example_manual_filter_main(args: list[str] | None = None) -> None:
    """Run manual filter DTPS example."""
    dtps_server = DTPSServer.create(on_startup=[on_startup])
    data = "{'key1': 'value1', 'key2': 'value2'}"
    logger.info(
        "Try this:\n"
        f"curl -X POST -H 'Content-Type: application/json' -d '{data}'\n"
        "http://localhost:PORT/node/in/",
    )
    coroutine = interpret_command_line_and_start(dtps_server, args)
    asyncio.run(coroutine)


if __name__ == "__main__":
    dtps_example_manual_filter_main()
