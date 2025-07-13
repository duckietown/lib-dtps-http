"""Examples."""

import asyncio

from dtps import context
from dtps_http import RawData


async def example_process_data_1() -> None:
    """Run first process data example.

    Environment:
        DTPS_BASE_SELF = "http://:8000/"
    """
    self_context = await context("self")
    node_input = self_context / "dtps" / "node" / "in"
    await node_input.queue_create()
    node_output = self_context / "dtps" / "node" / "out"
    await node_output.queue_create()
    async with node_output.publisher_context() as publisher:

        async def on_input(data: RawData) -> None:
            await publisher.publish(data)

        subscription = await node_input.subscribe(on_input)
        await asyncio.sleep(10)
        await subscription.unsubscribe()


async def process_data_example_2() -> None:
    """Run second process data example.

    Environment example:
        DTPS_BASE_SOURCE = "http://:8000/dtps/node/out"
        DTPS_BASE_TARGET = "http://:8001/dtps/node/in"
    """
    source = await context("source")
    target = await context("target")

    async def on_input(data: RawData) -> None:
        await target.publish(data)

    subscription = await source.subscribe(on_input)
    await asyncio.sleep(10)
    await subscription.unsubscribe()


async def example_register_1() -> None:
    """Run first register example.

    Environment example:
        DTPS_BASE_SELF_0 = "create:http://:212/"
        DTPS_BASE_SELF_1 = "create:/tmp/sockets/nodename/"
        DTPS_BASE_SWITCHBOARD = "http://:8000/"
    """
    self_context = await context("self")
    switchboard = await context("switchboard")
    nodename = switchboard.navigate("dtps/node/nodename")
    await nodename.expose(self_context)


async def example_register_2() -> None:
    """Run second register example.

    Environment example:
        DTPS_BASE_SELF_0 = "create:http://localhost:0/"
        DTPS_BASE_SELF_1 = "create:/tmp/sockets/nodename/"
        DTPS_BASE_SWITCHBOARD_ADD_0 = "http://:8000/dtps/node/nodename/"
        DTPS_BASE_SWITCHBOARD_ADD_1 = "http+unix://:8000/dtps/node/nodename/"
    """
    self_context = await context("self")
    switchboard = await context("switchboard_add")
    await switchboard.expose(self_context)


async def example_connect() -> None:
    """Run connect example.

    Environment example:
        DTPS_BASE_SWITCHBOARD = "http://:8000/dtps/node/nodename/"
        DTPS_BASE_SWITCHBOARD_ADD_1 = "http+unix://:8000/dtps/node/nodename/"
    """
    switchboard = await context("switchboard_add")
    node1_out = switchboard / "dtps/node/node1"
    node2_in = switchboard / "dtps/node/node2"
    await node1_out.connect_to(node2_in)
