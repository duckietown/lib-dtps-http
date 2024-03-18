import asyncio
from typing import List, Optional

from dtps_http import (
    async_error_catcher, ContentInfo, DataSaved, DTPSServer,
    interpret_command_line_and_start, logger, MIME_JSON, ObjectQueue, RawData, TopicNameV, TopicProperties,
)
from dtps_http.structures import Bounds, InsertNotification

__all__ = [
    "dtps_example_manual_filter_main",
]


# IN, OUT my topics
@async_error_catcher
async def on_startup(s: DTPSServer) -> None:
    IN = TopicNameV.from_dash_sep("node/in")
    OUT = TopicNameV.from_dash_sep("node/out")
    queue_in = await s.create_oq(IN, content_info=ContentInfo.simple(MIME_JSON), bounds=Bounds.max_length(2),
                                 tp=TopicProperties.rw_pushable())
    queue_out = await s.create_oq(OUT, content_info=ContentInfo.simple(MIME_JSON), bounds=Bounds.max_length(2),
                                  tp=TopicProperties.streamable_readonly())

    @async_error_catcher
    async def on_received_in(q: ObjectQueue, inot: InsertNotification) -> None:
        # just publish the same data
        await queue_out.publish(inot.raw_data)

    queue_in.subscribe(on_received_in)

    await queue_in.publish_json('first msg')


def dtps_example_manual_filter_main(args: Optional[List[str]] = None) -> None:
    dtps_server = DTPSServer.create(on_startup=[on_startup])

    msg = f"""curl -X POST -H "Content-Type: application/json" -d '{{"key1":"value1", "key2":"value2"}}' 
    http://localhost:PORT/node/in/"""
    logger.info(f"Try this:\n{msg}")

    asyncio.run(interpret_command_line_and_start(dtps_server, args))


if __name__ == '__main__':
    dtps_example_manual_filter_main()
