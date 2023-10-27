import asyncio
from typing import List, Optional

from dtps_http import (
    async_error_catcher, ContentInfo, DataDesc, DTPSServer,
    interpret_command_line_and_start, logger, MIME_CBOR, MIME_JSON, ObjectTransformContext,
    ObjectTransformFunction, ObjectTransformResult, RawData, TopicNameV, TopicProperties, TransformError,
)

__all__ = [
    "rpc1_main",
]


async def transform(x: ObjectTransformContext) -> ObjectTransformResult:
    # use this to get the raw data
    raw_data_input = x.raw_data
    logger.info(f'Got raw data: {raw_data_input}')
    # interpet json, yaml, cbor, etc.
    number = raw_data_input.get_as_native_object()
    logger.info(f'Got raw data: {number!r}')
    if not isinstance(number, int):
        # return error
        return TransformError(400, f"Expected an integer for this parameter, got {type(number)}")

    if number % 2 == 0:
        # if the number is even, we return a string
        result = RawData.cbor_from_native_object(
            {'ok': True, 'msg': f"Got an even number {number}"}
        )
        return result
    else:
        # return error
        return TransformError(400, f"Expected an even integer, got {number}")


# IN, OUT my topics
@async_error_catcher
async def on_startup(s: DTPSServer) -> None:
    IN = TopicNameV.from_dash_sep("node/rpc")
    t: ObjectTransformFunction = transform

    content_info = ContentInfo(
        accept={
            'json': DataDesc(
                content_type=MIME_JSON,
                jschema=None,
                examples=[]
            ),
            'cbor': DataDesc(
                content_type=MIME_CBOR,
                jschema=None,
                examples=[]
            ),
        },
        storage=DataDesc(
            content_type=MIME_CBOR,
            jschema=None,
            examples=[]
        ),
        produces_content_type=[MIME_CBOR, MIME_JSON]
    )

    queue_in = await s.create_oq(IN, content_info=content_info,
                                 tp=TopicProperties.rw_pushable(), transform=t)

    r1 = await queue_in.publish_json(0)
    r2 = await queue_in.publish_cbor(1)
    assert isinstance(r1, RawData), r1
    assert isinstance(r2, TransformError), r2


def rpc1_main(args: Optional[List[str]] = None) -> None:
    dtps_server = DTPSServer.create(on_startup=[on_startup])

    asyncio.run(interpret_command_line_and_start(dtps_server, args))


if __name__ == '__main__':
    rpc1_main()
