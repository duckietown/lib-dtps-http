import asyncio
from typing import List, Optional

from dtps_http import (
    async_error_catcher, ContentInfo, DataSaved, DTPSClient, DTPSServer, InsertNotification,
    interpret_command_line_and_start, logger, MIME_JSON, ObjectQueue, parse_url_unescape, RawData, TopicNameV,
    TopicProperties, URLString,
)

__all__ = [
    "dtps_example_manual_filter_main",
]


# in = arbitrary, out = arbitrary URLS
async def main():
    URL_IN = parse_url_unescape(URLString("http://localhost:8000/the/input/"))
    URL_OUT = parse_url_unescape(URLString("http://localhost:8000/the/output/"))
    client = DTPSClient()

    metadata = await client.get_metadata(URL_IN)

    if metadata.events_data_inline_url is None:
        raise AssertionError

    async for d in client.listen_url_events(
        metadata.events_data_inline_url, raise_on_error=False, inline_data=True, add_silence=None
    ):
        if isinstance(d, InsertNotification):
            await client.publish(URL_OUT, d.raw_data)


#
#
# @async_error_catcher
# async def on_startup2_mixed(s: DTPSServer) -> None:
#     IN = TopicNameV.from_dash_sep("node/in")
#     # OUT = TopicNameV.from_dash_sep("node/out")
#     queue_in = await s.create_oq(IN, content_info=ContentInfo.simple(MIME_JSON))
#     # queue_out = await s.create_oq(OUT, content_info=ContentInfo.simple(MIME_JSON))
#     client = DTPSClient()
#
#     URL_IN = parse_url_unescape(URLString("http://localhost:8000/the/input/"))
#     URL_OUT = parse_url_unescape(URLString("http://localhost:8000/the/output/"))
#
#     metadata = await client.get_metadata(URL_IN)
#     if metadata.events_data_inline_url is None:
#         raise AssertionError
#
#     async for d in client.listen_url_events(
#         metadata.events_data_inline_url, raise_on_error=False, inline_data=True, add_silence=None
#     ):
#         if isinstance(d, InsertNotification):
#             await client.publish(URL_OUT, d.raw_data)
#
#     @async_error_catcher
#     async def on_received_in(q: ObjectQueue, i: int) -> None:
#         saved: DataSaved = q.saved[i]
#         data_: RawData = q.get(saved.digest)
#
#         await client.publish(URL_OUT, data_)
#
#     queue_in.subscribe(on_received_in)


# IN, OUT my topics
@async_error_catcher
async def on_startup(s: DTPSServer) -> None:
    IN = TopicNameV.from_dash_sep("node/in")
    OUT = TopicNameV.from_dash_sep("node/out")
    queue_in = await s.create_oq(IN, content_info=ContentInfo.simple(MIME_JSON),
                                 tp=TopicProperties.rw_pushable())
    queue_out = await s.create_oq(OUT, content_info=ContentInfo.simple(MIME_JSON),
                                  tp=TopicProperties.streamable_readonly())

    @async_error_catcher
    async def on_received_in(q: ObjectQueue, i: int) -> None:
        saved: DataSaved = q.saved[i]
        data: RawData = q.get(saved.digest)
        # just publish the same data
        queue_out.publish(data)

    queue_in.subscribe(on_received_in)

    queue_in.publish_json('first msg')


def dtps_example_manual_filter_main(args: Optional[List[str]] = None) -> None:
    dtps_server = DTPSServer.create(on_startup=[on_startup])

    msg = f"""curl -X POST -H "Content-Type: application/json" -d '{{"key1":"value1", "key2":"value2"}}' 
    http://localhost:PORT/node/in/"""
    logger.info(f"Try this:\n{msg}")

    asyncio.run(interpret_command_line_and_start(dtps_server, args))


if __name__ == '__main__':
    dtps_example_manual_filter_main()
