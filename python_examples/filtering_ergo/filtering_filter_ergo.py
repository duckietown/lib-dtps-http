"""Filtering filter ergo."""

import asyncio
from asyncio import Event
from typing import Any

from dtps import AbstractPublisherInterface, RawData, context, logger
from dtps_http import MIME_TEXT


def get_on_input(publisher: AbstractPublisherInterface) -> Any:
    """Return `on_input`."""

    async def on_input(raw_data: RawData, /) -> None:
        await publisher.publish(raw_data)

    return on_input


def get_on_output(event: Event) -> Any:
    """Return `on_output`."""

    async def on_output(_: RawData, /) -> None:
        event.set()

    return on_output


async def go() -> None:
    """Go."""
    self_context = await context("self")
    node_input = self_context / "dtps" / "node" / "in"
    await node_input.queue_create()
    node_output = self_context / "dtps" / "node" / "out"
    await node_output.queue_create()
    publisher = await node_output.publisher()
    event = Event()
    on_input = get_on_input(publisher)
    on_output = get_on_output(event)
    await node_input.subscribe(on_input)
    await node_output.subscribe(on_output)
    raw_data = RawData(content=b"hello", content_type=MIME_TEXT)
    await node_input.publish(raw_data)
    await event.wait()
    logger.info("Received event.")
    await asyncio.sleep(100)


def main() -> None:
    """Run filtering filter ergo."""
    asyncio.run(go())


if __name__ == "__main__":
    main()
