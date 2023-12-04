import asyncio

from dtps import context
from dtps.ergo_ui import SubscriptionInterface
from dtps_http import RawData


async def example1_process_data() -> None:
    # Environment
    # DTPS_BASE_SELF = "http://:8000/"

    me = await context("self")

    node_input = await (me / "dtps" / "node" / "in").queue_create()
    node_output = await (me / "dtps" / "node" / "out").queue_create()

    async with node_output.publisher() as publisher:

        async def on_input(data: RawData) -> None:
            await publisher.publish(data)

        subscription: SubscriptionInterface = await node_input.subscribe(on_input)

        await asyncio.sleep(10)

        await subscription.unsubscribe()


async def example2_process_data() -> None:
    # Environment example:
    #   DTPS_BASE_SOURCE = "http://:8000/dtps/node/out"
    #   DTPS_BASE_TARGET = "http://:8001/dtps/node/in"

    source = await context("source")
    target = await context("target")

    async def on_input(data: RawData) -> None:
        await target.publish(data)

    subscription = await source.subscribe(on_input)

    await asyncio.sleep(10)

    await subscription.unsubscribe()


async def example2_register() -> None:
    # Environment example:
    #   DTPS_BASE_SELF_0 = "create:http://:212/"
    #   DTPS_BASE_SELF_1 = "create:/tmp/sockets/nodename/"
    #   DTPS_BASE_SWITCHBOARD = "http://:8000/"
    me = await context("self")
    switchboard = await context("switchboard")

    await switchboard.navigate("dtps/node/nodename").expose(me)


async def example3_register() -> None:
    # Environment example:
    #   DTPS_BASE_SELF_0 = "create:http://localhost:0/"
    #   DTPS_BASE_SELF_1 = "create:/tmp/sockets/nodename/"
    #   DTPS_BASE_SWITCHBOARD_ADD_0 = "http://:8000/dtps/node/nodename/"
    #   DTPS_BASE_SWITCHBOARD_ADD_1 = "http+unix://:8000/dtps/node/nodename/"

    me = await context("self")
    switchboard = await context("switchboard_add")

    # await switchboard.proxy_create(await me.urls())

    await switchboard.expose(me)


async def example4_connect2() -> None:
    # Environment example:
    #   DTPS_BASE_SWITCHBOARD = "http://:8000/dtps/node/nodename/"
    #   DTPS_BASE_SWITCHBOARD_ADD_1 = "http+unix://:8000/dtps/node/nodename/"

    switchboard = await context("switchboard_add")

    node1_out = switchboard.navigate("dtps/node/node1")
    node2_in = switchboard.navigate("dtps/node/node2")
    # await switchboard.proxy_create(await me.urls())

    await node1_out.connect_to(node2_in)
