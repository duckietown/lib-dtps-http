from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Callable, cast, Dict, List, Optional, Sequence, Tuple, Union

from jsonpatch import JsonPatch

from dtps_http import (
    app_start,
    Bounds,
    check_is_unix_socket,
    ContentInfo,
    DTPSServer,
    EveryOnceInAWhile,
    ForwardedQueue,
    InsertNotification,
    join,
    MIME_OCTET,
    Native,
    NodeID,
    NotAvailableYet,
    NotFound,
    ObjectQueue,
    ObjectTransformContext,
    OurQueue,
    parse_url_unescape,
    RawData,
    ServerWrapped,
    SourceComposition,
    SUB_ID,
    TopicNameV,
    TopicProperties,
    transform_identity,
    TransformError,
    url_to_string,
    URLString,
)
from .config import ContextInfo, ContextManager
from .ergo_ui import (
    ConnectionInterface,
    ContextConfig,
    DTPSContext,
    HistoryInterface,
    ListenerInfo,
    PatchType,
    PublisherInterface,
    RPCFunction,
    ServeFunction,
    SubscriptionInterface,
)

__all__ = [
    "ContextManagerCreate",
]


class ContextManagerCreate(ContextManager):
    dtps_server_wrap: Optional[ServerWrapped]
    contexts: "Dict[Tuple[Tuple[str, ...], ContextConfig], ContextManagerCreateContext]"

    def __init__(self, base_name: str, context_info: "ContextInfo"):
        self.base_name = base_name
        self.context_info = context_info
        self.dtps_server_wrap = None
        self.contexts = {}
        self.base_config = ContextConfig.default()
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

    def get_context_by_components(self, components: Tuple[str, ...], config: ContextConfig) -> "DTPSContext":
        key = components, config
        if key not in self.contexts:
            self.contexts[key] = ContextManagerCreateContext(self, components, config)

        return self.contexts[key]

    def get_context(self) -> "DTPSContext":
        return self.get_context_by_components((), self.base_config)

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

    async def get_listener_info(self) -> Optional[ListenerInfo]:
        return self.master.get_listener_info()


class ContextManagerCreateContextSubscriber(SubscriptionInterface):
    def __init__(self, sub_id: SUB_ID, oq0: ObjectQueue) -> None:
        self.sub_id = sub_id
        self.oq0 = oq0

    async def unsubscribe(self) -> None:
        await self.oq0.unsubscribe(self.sub_id)


class ContextManagerCreateContext(DTPSContext):
    _publisher: ContextManagerCreateContextPublisher

    def __init__(self, master: ContextManagerCreate, components: Tuple[str, ...], config: ContextConfig):
        self.master = master
        self.components = components
        self._publisher = ContextManagerCreateContextPublisher(self)
        self._topic = TopicNameV.from_components(components)
        self.config = config

    async def aclose(self) -> None:
        await self.master.aclose()

    async def get_urls(self) -> List[URLString]:
        server = self._get_server()
        urls = server.available_urls

        rurl = self._topic.as_relative_url()
        res: list[URLString] = []
        for u in urls:
            u2 = parse_url_unescape(u)
            um = join(u2, rurl)
            res.append(url_to_string(um))

        for u in res:
            parse_url_unescape(u)
        return res

    async def get_node_id(self) -> Optional[NodeID]:
        server = self._get_server()
        topic = self._topic
        resolve = server._resolve_tn(topic, url0=topic.as_relative_url())
        # server.logger.info(f"get_node_id - resolve: {resolve}")
        return await resolve.get_source_node_id(server)

    def _get_server(self) -> DTPSServer:
        if self.master.dtps_server_wrap is None:
            raise AssertionError("ContextManagerCreateContext: server not initialized")
        return self.master.dtps_server_wrap.server

    # def _get_components_as_topic(self) -> TopicNameV:
    #     return TopicNameV.from_components(self.components)

    def meta(self) -> "DTPSContext":
        return self / ":meta"  # TODO: actually we can do some error checks here

    def navigate(self, *components: str) -> "DTPSContext":
        c: list[str] = []
        for comp in components:
            c.extend([_ for _ in comp.split("/") if _])
        return self.master.get_context_by_components(self.components + tuple(c), self.config)

    def get_config(self) -> ContextConfig:
        return self.config

    def configure(self, cc: ContextConfig, /) -> "DTPSContext":
        merged = self.config.specialize(cc)
        return self.master.get_context_by_components(self.components, merged)

    async def list(self) -> List[str]:
        # TODO: DTSW-4798: implement list
        raise NotImplementedError()

    async def remove(self) -> None:
        topic = self._topic
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
        topic = self._topic
        server = self._get_server()
        try:
            server._resolve_tn(topic, url0=topic.as_relative_url())
        except KeyError:
            return False
        else:
            return True

    async def data_get(self) -> RawData:
        topic = self._topic
        server = self._get_server()
        url0 = topic.as_relative_url()
        source = server._resolve_tn(topic, url0=url0)
        res = await source.get_resolved_data(url0, server, None)
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
        self,
        on_data: Callable[[RawData], Awaitable[None]],
        /,
        max_frequency: Optional[float] = None,
        inline: bool = True,
    ) -> "SubscriptionInterface":
        oq0 = self._get_server().get_oq(self._topic)

        when = EveryOnceInAWhile(1.0 / max_frequency if max_frequency is not None else 0)

        async def wrap(_: ObjectQueue, inot: InsertNotification) -> None:
            if when.now():
                await on_data(inot.raw_data)

        if oq0.stored:
            last = oq0.last_data()
            # data2: RawData = oq0.get(oq0.last().digest)
            await on_data(last)
        _ = inline
        sub_id = oq0.subscribe(wrap, max_frequency=max_frequency)

        return ContextManagerCreateContextSubscriber(sub_id, oq0)

    async def history(self) -> "Optional[HistoryInterface]":
        # TODO: DTSW-4794: implement history
        raise NotImplementedError()

    async def publish(self, data: RawData, /) -> None:
        server = self._get_server()
        topic = self._topic
        queue = server.get_oq(topic)
        await queue.publish(data)

    async def publisher(self) -> "PublisherInterface":
        return self._publisher

    @asynccontextmanager
    async def publisher_context(self) -> AsyncIterator["PublisherInterface"]:
        yield self._publisher

    async def patch(self, patch_data: List[Dict[str, Any]], /) -> None:
        server = self._get_server()
        topic = self._topic
        url0 = topic.as_relative_url()
        resolve = server._resolve_tn(topic, url0=url0)
        pdata: JsonPatch
        pdata = JsonPatch(patch_data)  # type: ignore
        await resolve.patch(url0, server, pdata)

    async def call(self, data: RawData, /) -> RawData:
        server = self._get_server()
        topic = self._topic
        url0 = topic.as_relative_url()
        resolve = server._resolve_tn(topic, url0=url0)
        res = await resolve.call(url0, server, data)
        # queue = server.get_oq(topic)
        # res = await queue.publish(data)
        if isinstance(res, TransformError):
            raise Exception(f"{res.http_code}: {res.message}")
        return res

    async def expose(
        self, p: "Sequence[str] | DTPSContext", /, *, mask_origin: bool = False
    ) -> "DTPSContext":
        if isinstance(p, DTPSContext):
            urls = await p.get_urls()
            node_id = await p.get_node_id()
        else:
            urls = cast(Sequence[URLString], p)
            node_id = None

        server = self._get_server()
        topic = self._topic
        await server.expose(topic, node_id, urls, mask_origin=mask_origin)
        return self

    async def queue_create(
        self,
        *,
        transform: Optional[RPCFunction] = None,
        serve: Optional[ServeFunction] = None,
        #
        content_info: Optional[ContentInfo] = None,
        topic_properties: Optional[TopicProperties] = None,
        app_data: Optional[Dict[str, Any]] = None,
        bounds: Optional[Bounds] = None,
    ) -> "DTPSContext":
        if bounds is None:
            bounds = Bounds.default()
        server = self._get_server()
        topic = self._topic

        if transform is None:
            transform_use = transform_identity
        else:

            async def transform_use(otc: ObjectTransformContext) -> Union[RawData, TransformError]:
                return await transform(otc.raw_data)

        if bounds is None:
            bounds = Bounds.default()

        if content_info is None:
            content_info = ContentInfo.simple(MIME_OCTET)

        if topic_properties is None:
            topic_properties = TopicProperties.rw_pushable()

        if app_data is None:
            app_data = {}

        await server.create_oq(
            topic,
            content_info=content_info,
            tp=topic_properties,
            transform=transform_use,
            serve=serve,
            bounds=bounds,
            app_data=app_data,
        )

        return self

    def get_listener_info(self) -> Optional[ListenerInfo]:
        server = self._get_server()
        topic = self._topic
        if topic in server._oqs:
            oq = server._oqs[topic]
            return oq.get_listener_info()
        # not available if not queues
        return None

    async def until_ready(
        self,
        retry_every: float = 2.0,
        retry_max: Optional[int] = None,
        timeout: Optional[float] = None,
        print_every: float = 10.0,
        quiet: bool = False,
    ) -> "DTPSContext":
        return self

    async def connect_to(self, c: "DTPSContext", /) -> "ConnectionInterface":  # type: ignore
        msg = 'Cannot use this method for "create" contexts because Python does not support the functionality'
        raise NotImplementedError(msg)

    async def subscribe_diff(
        self, on_data: Callable[[PatchType], Awaitable[None]], /
    ) -> "SubscriptionInterface":
        differ = Differ()

        async def sub_diff(data: RawData) -> None:
            patch = differ.push(data)
            if patch is None:
                return
            await on_data(patch)

        return await self.subscribe(sub_diff)

    def __repr__(self) -> str:
        return f"ContextManagerCreateContext({self.components!r}, {self.master!r})"


class Differ:
    def __init__(self) -> None:
        self.first_arrived = False
        self.current = None

    def push(self, data: RawData) -> Optional[PatchType]:
        if not self.first_arrived:
            self.first_arrived = True
            self.current = data.get_as_native_object()
            patch = [
                {"op": "replace", "path": "", "value": self.current},
            ]
            return patch

        prev = self.current
        new_one = data.get_as_native_object()
        if prev == new_one:
            return []
        patch = JsonPatch.from_diff(prev, new_one)  # type: ignore
        operations = patch.to_string(lambda f: f)  # type: ignore
        self.current = new_one
        return cast(PatchType, operations)
