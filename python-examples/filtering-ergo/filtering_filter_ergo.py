import asyncio

from dtps import context, logger, RawData
from dtps_http import MIME_TEXT


async def go() -> None:
    me = await context("self")

    node_input = await (me / 'dtps' / 'node' / 'in').queue_create()
    node_output = await (me / 'dtps' / 'node' / 'out').queue_create()

    publisher = await node_output.publisher()

    async def on_input(data: RawData, /) -> None:
        await publisher.publish(data)

    event = asyncio.Event()

    async def on_output(_: RawData, /) -> None:
        event.set()

    await node_input.subscribe(on_input)
    await node_output.subscribe(on_output)

    rd = RawData(content=b'hello', content_type=MIME_TEXT)
    await node_input.publish(rd)
    await event.wait()

    logger.info("received event")
    await asyncio.sleep(100)

    # await subscription.unsubscribe()


def main() -> None:
    asyncio.run(go())


if __name__ == '__main__':
    main()
