from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Callable, cast, Dict, List, Optional, Sequence, Tuple, Union

from dtps_http import (
    app_start,
    check_is_unix_socket,
    ContentInfo,
    DataSaved,
    DTPSServer,
    join,
    MIME_OCTET,
    NodeID,
    ObjectQueue,
    parse_url_unescape,
    RawData,
    ServerWrapped,
    TopicNameV,
    TopicProperties,
    TopicRefAdd,
    url_to_string,
    URLString,
)
from dtps_http.constants import DEFAULT_MAX_HISTORY
from dtps_http.object_queue import ObjectTransformContext, transform_identity, TransformError
from dtps_http.types_of_source import (
    ForwardedQueue,
    Native,
    NotAvailableYet,
    NotFound,
    OurQueue,
    SourceComposition,
)
from .config import ContextInfo, ContextManager
from .ergo_ui import (
    ConnectionInterface,
    DTPSContext,
    HistoryInterface,
    PublisherInterface,
    RPCFunction,
    SubscriptionInterface,
)

__all__ = [
    "ContextManagerCreate",
]


class ContextManagerCreate(ContextManager):
    dtps_server_wrap: Optional[ServerWrapped]

    def __init__(self, base_name: str, context_info: "ContextInfo"):
        self.base_name = base_name
        self.context_info = context_info
        self.dtps_server_wrap = None
        self.contexts = {}
        assert self.context_info.is_create()

    async def init(self) -> None:
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

    def __repr__(self) -> str:
        return f"ContextManagerCreate({self.base_name!r})"


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
    def __init__(self, sub_id, oq0: ObjectQueue) -> None:
        self.sub_id = sub_id
        self.oq0 = oq0

    async def unsubscribe(self) -> None:
        await self.oq0.unsubscribe(self.sub_id)


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
        res = []
        for u in urls:
            u2 = parse_url_unescape(u)
            um = join(u2, rurl)
            res.append(url_to_string(um))

        for u in res:
            parse_url_unescape(u)
        return res

    async def get_node_id(self) -> Optional[NodeID]:
        server = self._get_server()
        topic = self._get_components_as_topic()
        resolve = server._resolve_tn(topic, url0=topic.as_relative_url())
        # server.logger.info(f"get_node_id - resolve: {resolve}")
        return await resolve.get_source_node_id(server)

    def _get_server(self) -> DTPSServer:
        if self.master.dtps_server_wrap is None:
            raise AssertionError("ContextManagerCreateContext: server not initialized")
        return self.master.dtps_server_wrap.server

    def _get_components_as_topic(self) -> TopicNameV:
        return TopicNameV.from_components(self.components)

    def navigate(self, *components: str) -> "DTPSContext":
        c = []
        for comp in components:
            c.extend([_ for _ in comp.split("/") if _])
        return self.master.get_context_by_components(self.components + tuple(c))

    async def list(self) -> List[str]:
        # TODO: DTSW-4798: implement list
        raise NotImplementedError()

    async def remove(self) -> None:
        topic = self._get_components_as_topic()
        server = self._get_server()
        try:
            source = server._resolve_tn(topic, url0=topic.as_relative_url())
        except KeyError:
            raise

        if isinstance(source, OurQueue):
            await server.remove_oq(topic)
        elif isinstance(source, ForwardedQueue):
            msg = "Cannot remove a forwarded queue"
            raise NotImplementedError(msg)
        elif isinstance(source, SourceComposition):
            msg = "Cannot remove a source composition queue"
            raise NotImplementedError(msg)
        else:
            msg = f"Cannot remove a {source}"
            raise NotImplementedError(msg)

    async def exists(self) -> bool:
        topic = self._get_components_as_topic()
        server = self._get_server()
        try:
            server._resolve_tn(topic, url0=topic.as_relative_url())
        except KeyError:
            return False
        else:
            return True

    async def data_get(self) -> RawData:
        topic = self._get_components_as_topic()
        server = self._get_server()
        url0 = topic.as_relative_url()
        source = server._resolve_tn(topic, url0=url0)
        res = await source.get_resolved_data(url0, server)
        if isinstance(res, RawData):
            return res
        elif isinstance(res, NotFound):
            raise KeyError(f"Topic {topic} not found")
        elif isinstance(res, NotAvailableYet):
            raise Exception("Not available yet")  # XXX
        elif isinstance(res, Native):
            return RawData.cbor_from_native_object(res.ob)
        else:
            raise AssertionError(f"Unexpected {res}")

    async def subscribe(
        self, on_data: Callable[[RawData], Awaitable[None]], /, max_frequency: Optional[float] = None
    ) -> "SubscriptionInterface":
        oq0 = self._get_server().get_oq(self._get_components_as_topic())

        async def wrap(oq: ObjectQueue, i: int) -> None:
            saved: DataSaved = oq.saved[i]
            data: RawData = oq.get(saved.digest)
            await on_data(data)

        if oq0.stored:
            data2: RawData = oq0.get(oq0.last().digest)
            await on_data(data2)
        sub_id = oq0.subscribe(wrap)

        return ContextManagerCreateContextSubscriber(sub_id, oq0)

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

    async def patch(self, patch_data: List[Dict[str, Any]], /) -> None:
        raise NotImplementedError

    async def call(self, data: RawData, /) -> RawData:
        server = self._get_server()
        topic = self._get_components_as_topic()
        url0 = topic.as_relative_url()
        resolve = server._resolve_tn(topic, url0=url0)
        res = await resolve.call(url0, server, data)
        # queue = server.get_oq(topic)
        # res = await queue.publish(data)
        if isinstance(res, TransformError):
            raise Exception(f"{res.http_code}: {res.message}")
        return res

    async def expose(self, p: "Sequence[str] | DTPSContext", /) -> None:
        if isinstance(p, DTPSContext):
            urls = await p.get_urls()
            node_id = await p.get_node_id()
        else:
            urls = cast(Sequence[URLString], p)
            node_id = None

        mask_origin = False
        server = self._get_server()
        topic = self._get_components_as_topic()
        await server.expose(topic, node_id, urls, mask_origin=mask_origin)

    async def queue_create(
        self,
        *,
        max_history: int = DEFAULT_MAX_HISTORY,
        parameters: Optional[TopicRefAdd] = None,
        transform: Optional[RPCFunction] = None,
    ) -> "DTPSContext":
        server = self._get_server()
        topic = self._get_components_as_topic()
        if parameters is None:
            parameters = TopicRefAdd(
                content_info=ContentInfo.simple(MIME_OCTET),
                properties=TopicProperties.rw_pushable(),
                app_data={},
            )
        if transform is None:
            transform_use = transform_identity
        else:

            async def transform_use(otc: ObjectTransformContext) -> Union[RawData, TransformError]:
                return await transform(otc.raw_data)

        await server.create_oq(
            topic, content_info=parameters.content_info, tp=parameters.properties, transform=transform_use,
            max_history=max_history
        )

        return self

    async def until_ready(
            self,
            retry_every: float = 2.0,
            retry_max: Optional[int] = None,
            timeout: Optional[float] = None,
            print_every: float = 10.0,
            quiet: bool = False,
    ) -> "DTPSContext":
        return self

    async def connect_to(self, c: "DTPSContext", /) -> "ConnectionInterface":
        msg = 'Cannot use this method for "create" contexts because Python does not support the functionality'
        raise NotImplementedError(msg)

    def __repr__(self) -> str:
        return f"ContextManagerCreateContext({self.components!r}, {self.master!r})"
