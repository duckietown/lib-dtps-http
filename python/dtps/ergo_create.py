import os
import stat
from contextlib import asynccontextmanager
from typing import AsyncIterator, Awaitable, Callable, List, Optional, Tuple, Sequence

from dtps_http import (
    app_start,
    check_is_unix_socket,
    ContentInfo,
    DataSaved,
    DTPSServer,
    MIME_OCTET,
    ObjectQueue,
    RawData,
    ServerWrapped,
    TopicNameV,
    TopicProperties,
    TopicRefAdd,
)
from .config import ContextInfo, ContextManager
from .ergo_ui import (
    ConnectionInterface,
    DTPSContext,
    HistoryInterface,
    PublisherInterface,
    SubscriptionInterface,
)

__all__ = [
    "ContextManagerCreate",
]

from . import logger


class ContextManagerCreate(ContextManager):
    dtps_server_wrap: Optional[ServerWrapped]

    def __init__(self, base_name: str, context_info: "ContextInfo"):
        self.base_name = base_name
        self.context_info = context_info
        self.dtps_server_wrap = None
        self.contexts = {}
        assert self.context_info.is_create()

    async def init(self) -> None:
        # self.client = DTPSClient.create()
        # await self.client.init()
        dtps_server = DTPSServer.create(nickname=self.base_name)
        tcps, unix_paths = self.context_info.get_tcp_and_unix()

        a = await app_start(
            dtps_server,
            tcps=tcps,
            unix_paths=unix_paths,
            tunnel=None,
        )
        for u in unix_paths:
            check_is_unix_socket(u)

        self.dtps_server_wrap = a

    async def aclose(self) -> None:
        if self.dtps_server_wrap is not None:
            await self.dtps_server_wrap.aclose()

    def get_context_by_components(self, components: Tuple[str, ...]) -> "DTPSContext":
        if components not in self.contexts:
            self.contexts[components] = ContextManagerCreateContext(self, components)

        return self.contexts[components]

    def get_context(self) -> "DTPSContext":
        return self.get_context_by_components(())


class ContextManagerCreateContextPublisher(PublisherInterface):
    def __init__(self, master: "ContextManagerCreateContext"):
        self.master = master

    async def publish(self, rd: RawData, /) -> None:
        # nothing more to do for this
        await self.master.publish(rd)

    async def terminate(self) -> None:
        # nothing more to do for this
        pass


class ContextManagerCreateContextSubscriber(SubscriptionInterface):
    async def unsubscribe(self) -> None:
        # TODO: DTSW-4792: implement
        pass


class ContextManagerCreateContext(DTPSContext):
    _publisher: ContextManagerCreateContextPublisher

    def __init__(self, master: ContextManagerCreate, components: Tuple[str, ...]):
        self.master = master
        self.components = components
        self._publisher = ContextManagerCreateContextPublisher(self)

    async def aclose(self) -> None:
        await self.master.aclose()

    async def get_urls(self) -> List[str]:
        server = self._get_server()
        urls = server.available_urls
        rurl = self._get_components_as_topic().as_relative_url()
        return [f"{u}{rurl}" for u in urls]

    async def get_node_id(self) -> Optional[str]:
        # TODO: DTSW-4793: this could be remote
        server = self._get_server()
        return server.node_id

    def _get_server(self) -> DTPSServer:
        if self.master.dtps_server_wrap is None:
            raise AssertionError("ContextManagerCreateContext: server not initialized")
        return self.master.dtps_server_wrap.server

    def _get_components_as_topic(self) -> TopicNameV:
        return TopicNameV.from_components(self.components)

    def navigate(self, *components: str) -> "DTPSContext":
        return self.master.get_context_by_components(self.components + components)

    async def list(self) -> List[str]:
        # TODO: DTSW-4798: implement list
        raise NotImplementedError()

    async def remove(self) -> None:
        # TODO: DTSW-4799: implement remove
        raise NotImplementedError()

    async def data_get(self) -> RawData:
        oq0 = self._get_server().get_oq(self._get_components_as_topic())
        return oq0.last_data()

    async def subscribe(self, on_data: Callable[[RawData], Awaitable[None]], /) -> "SubscriptionInterface":
        oq0 = self._get_server().get_oq(self._get_components_as_topic())

        async def wrap(oq: ObjectQueue, i: int) -> None:
            saved: DataSaved = oq.saved[i]
            data: RawData = oq.get(saved.digest)
            await on_data(data)

        sub_id = oq0.subscribe(wrap)

        class Subscription(SubscriptionInterface):
            async def unsubscribe(self) -> None:
                await oq0.unsubscribe(sub_id)

        return Subscription()

    async def history(self) -> "Optional[HistoryInterface]":
        # TODO: DTSW-4794: implement history
        raise NotImplementedError()

    async def publish(self, data: RawData, /) -> None:
        server = self._get_server()
        topic = self._get_components_as_topic()
        queue = server.get_oq(topic)
        await queue.publish(data)

    async def publisher(self) -> "PublisherInterface":
        return self._publisher

    @asynccontextmanager
    async def publisher_context(self) -> AsyncIterator["PublisherInterface"]:
        yield self._publisher

    async def call(self, data: RawData, /) -> RawData:
        # TODO: DTSW-4795: implement call
        raise NotImplementedError()

    async def expose(self, urls: "Sequence[str] | DTPSContext", /) -> "DTPSContext":
        # TODO: DTSW-4796: implement expose
        raise NotImplementedError()

    async def queue_create(self, parameters: Optional[TopicRefAdd] = None, /) -> "DTPSContext":
        server = self._get_server()
        topic = self._get_components_as_topic()
        if parameters is None:
            parameters = TopicRefAdd(
                content_info=ContentInfo.simple(MIME_OCTET),
                properties=TopicProperties.rw_pushable(),
                app_data={},
            )

        await server.create_oq(topic, content_info=parameters.content_info, tp=parameters.properties)

        return self

    async def connect_to(self, c: "DTPSContext", /) -> "ConnectionInterface":
        # TODO: DTSW-4797: ContextManagerCreateContext: implement connect()
        raise NotImplementedError()
