import asyncio

from dtps import context, SubscriptionInterface, RawData



# # IN, OUT my topics
# @async_error_catcher
# async def on_startup(s: DTPSServer) -> None:
#     IN = TopicNameV.from_dash_sep("node/in")
#     OUT = TopicNameV.from_dash_sep("node/out")
#     queue_in = await s.create_oq(IN, content_info=ContentInfo.simple(MIME_JSON),
#                                  tp=TopicProperties.rw_pushable())
#     queue_out = await s.create_oq(OUT, content_info=ContentInfo.simple(MIME_JSON),
#                                   tp=TopicProperties.streamable_readonly())
#
#     @async_error_catcher
#     async def on_received_in(q: ObjectQueue, i: int) -> None:
#         saved: DataSaved = q.saved[i]
#         data: RawData = q.get(saved.digest)
#         # just publish the same data
#         await queue_out.publish(data)
#
#     queue_in.subscribe(on_received_in)
#
#     await queue_in.publish_json('first msg')


async def go() -> None:
    me = await context("self")

    node_input = await (me / 'dtps' / 'node' / 'in').queue_create()
    node_output = await (me / 'dtps' / 'node' / 'out').queue_create()

    async with node_output.publisher() as publisher:
        async def on_input(data: RawData) -> None:
            await publisher.publish(data)

        subscription: SubscriptionInterface = await node_input.subscribe(on_input)

        await asyncio.sleep(10)

        await subscription.unsubscribe()


def main() -> None:
    asyncio.run(go())


if __name__ == '__main__':
    main()
