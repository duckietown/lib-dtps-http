"""Ergo create."""

__all__ = ["ContextManagerCreate"]

import asyncio
import builtins
import traceback
import typing
from asyncio import Queue, QueueEmpty, QueueFull
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager, suppress
from typing import Any

from jsonpatch import JsonPatch

import dtps_http
from dtps import logger
from dtps.config import ContextInfo, ContextManager
from dtps.ergo_abstract import (
    AbstractDTPSContext,
    AbstractHistoryInterface,
    AbstractPublisherInterface,
    AbstractSubscriptionInterface,
    ContextConfig,
    ListenerInfo,
    PatchType,
    RPCFunction,
    ServeFunction,
)
from dtps.ergo_use import ConnectionInterface
from dtps_http import (
    DEFAULT_CALLBACK_QUEUE_SIZE,
    MIME_OCTET,
    SUB_ID,
    Bounds,
    ContentInfo,
    DTPSError,
    DTPSServer,
    EveryOnceInAWhile,
    ForwardedQueue,
    InsertNotification,
    Native,
    NodeID,
    NotAvailableYet,
    NotFound,
    ObjectQueue,
    ObjectTransformContext,
    OurQueue,
    RawData,
    ServerWrapped,
    SourceComposition,
    TopicNameV,
    TopicProperties,
    TransformError,
    URLString,
)


class ContextManagerCreate(ContextManager):
    """Context manager creator."""

    contexts: dict[
        tuple[tuple[str, ...], ContextConfig],
        "ContextManagerCreateContext",
    ]
    dtps_server_wrap: ServerWrapped | None

    def __init__(self, base_name: str, context_info: ContextInfo) -> None:
        """Initialize context manager creator."""
        self.base_name = base_name
        self.context_info = context_info
        self.dtps_server_wrap = None
        self.contexts = {}
        self.base_config = ContextConfig.default()
        if not self.context_info.is_create():
            raise DTPSError

    def __repr__(self) -> str:
        """Return representation of context manager creator."""
        return f"ContextManagerCreate({self.base_name!r})"

    async def aclose(self) -> None:
        """Close asynchronously."""
        if self.dtps_server_wrap is not None:
            await self.dtps_server_wrap.aclose()

    def get_context(self) -> AbstractDTPSContext:
        """Return context."""
        return self.get_context_by_components((), self.base_config)

    def get_context_by_components(
        self,
        components: tuple[str, ...],
        config: ContextConfig,
    ) -> AbstractDTPSContext:
        """Return context from components."""
        key = components, config
        if key not in self.contexts:
            self.contexts[key] = ContextManagerCreateContext(
                self,
                components,
                config,
            )
        return self.contexts[key]

    async def initialize(self) -> None:
        """Initialize."""
        dtps_server = DTPSServer.create(nickname=self.base_name)
        tcps, unix_paths = self.context_info.get_tcp_and_unix()
        dtps_server_wrap = await dtps_http.app_start(
            dtps_server,
            tcps=tcps,
            unix_paths=unix_paths,
            tunnel=None,
        )
        for unix_path in unix_paths:
            dtps_http.check_is_unix_socket(unix_path)
        self.dtps_server_wrap = dtps_server_wrap


class ContextManagerCreateContextPublisher(AbstractPublisherInterface):
    """Context manager creator context publisher."""

    def __init__(self, master: "ContextManagerCreateContext") -> None:
        """Initialize context manager creator context publisher."""
        self.master = master

    async def get_listener_info(self) -> ListenerInfo | None:
        return self.master.get_listener_info()

    async def publish(self, raw_data: RawData, /) -> None:
        # Nothing more to do for this
        await self.master.publish(raw_data)

    async def terminate(self) -> None:
        # Nothing more to do for this
        pass


class ContextManagerCreateContextSubscriber(AbstractSubscriptionInterface):
    def __init__(self, sub_id: SUB_ID, object_queue0: ObjectQueue) -> None:
        self.sub_id = sub_id
        self.object_queue0 = object_queue0

    async def unsubscribe(self) -> None:
        await self.object_queue0.unsubscribe(self.sub_id)


class ContextManagerCreateContext(AbstractDTPSContext):
    """Context manager creator context."""

    _publisher: ContextManagerCreateContextPublisher
    _topic: TopicNameV
    components: tuple[str, ...]
    config: ContextConfig
    master: ContextManagerCreate

    def __init__(
        self,
        master: ContextManagerCreate,
        components: tuple[str, ...],
        config: ContextConfig,
    ) -> None:
        """Initialize context manager creator context."""
        self.master = master
        self.components = components
        self._publisher = ContextManagerCreateContextPublisher(self)
        self._topic = TopicNameV.from_components(components)
        self.config = config

    def __repr__(self) -> str:
        return (
            f"ContextManagerCreateContext({self.components!r}, "
            f"{self.master!r})"
        )

    def _get_server(self) -> DTPSServer:
        if self.master.dtps_server_wrap is None:
            message = "ContextManagerCreateContext: server not initialized"
            raise AssertionError(message)
        return self.master.dtps_server_wrap.server

    async def aclose(self) -> None:
        await self.master.aclose()

    async def call(self, data: RawData, /) -> RawData:
        server = self._get_server()
        topic = self._topic
        url0 = topic.as_relative_url()
        resolve = server.resolve_topic_name(topic, url0=url0)
        result = await resolve.call(url0, server, data)
        if isinstance(result, TransformError):
            message = f"{result.http_code}: {result.message}"
            raise TypeError(message)
        return result

    def configure(
        self,
        context_configuration: ContextConfig,
        /,
    ) -> AbstractDTPSContext:
        merged = self.config.specialize(context_configuration)
        return self.master.get_context_by_components(self.components, merged)

    async def connect_to(
        self,
        _: AbstractDTPSContext,
        /,
    ) -> ConnectionInterface:
        message = (
            "Cannot use this method for `create` contexts because Python does "
            "not support this functionality."
        )
        raise NotImplementedError(message)

    async def data_get(self) -> RawData:
        topic = self._topic
        server = self._get_server()
        url = topic.as_relative_url()
        source = server.resolve_topic_name(topic, url0=url)
        result = await source.get_resolved_data(url, server, None)
        if isinstance(result, RawData):
            return result
        if isinstance(result, NotFound):
            message = f"Topic {topic} not found."
            raise KeyError(message)
        if isinstance(result, NotAvailableYet):
            message = "Not available yet."
            raise TypeError(message)
        if isinstance(result, Native):
            return RawData.cbor_from_native_object(result.ob)
        message = f"Unexpected {result}."
        raise AssertionError(message)

    async def exists(self) -> bool:
        topic = self._topic
        server = self._get_server()
        try:
            url = topic.as_relative_url()
            server.resolve_topic_name(topic, url0=url)
        except KeyError:
            return False
        else:
            return True

    async def expose(
        self,
        context: Sequence[str] | AbstractDTPSContext,
        /,
        *,
        mask_origin: bool = False,
    ) -> None:
        urls: Sequence[URLString]
        if isinstance(context, AbstractDTPSContext):
            urls = await context.get_urls()
            node_id = await context.get_node_id()
        else:
            urls = typing.cast(Sequence[URLString], context)
            node_id = None
        server = self._get_server()
        topic = self._topic
        await server.expose(topic, node_id, urls, mask_origin=mask_origin)

    def get_config(self) -> ContextConfig:
        return self.config

    def get_listener_info(self) -> ListenerInfo | None:
        server = self._get_server()
        topic = self._topic
        if topic in server.object_queues:
            return server.object_queues[topic].get_listener_info()
        return None

    async def get_node_id(self) -> NodeID | None:
        server = self._get_server()
        topic = self._topic
        url = topic.as_relative_url()
        resolve = server.resolve_topic_name(topic, url0=url)
        return await resolve.get_source_node_id(server)

    def get_path_components(self) -> tuple[str, ...]:
        return self.components

    async def get_urls(self) -> list[URLString]:
        server = self._get_server()
        relative_url = self._topic.as_relative_url()
        results: list[URLString] = []
        for available_url in server.available_urls:
            url = dtps_http.parse_url_unescape(available_url)
            url = dtps_http.join(url, relative_url)
            result = dtps_http.url_to_string(url)
            results.append(result)
        for result in results:
            dtps_http.parse_url_unescape(result)
        return results

    async def history(self) -> AbstractHistoryInterface | None:
        # TODO: DTSW-4794: Implement history
        raise NotImplementedError

    async def list(self) -> list[str]:
        # TODO: DTSW-4798: Implement list
        raise NotImplementedError

    def meta(self) -> AbstractDTPSContext:
        return (
            self / ":meta"
        )  # TODO: Actually we can do some error checks here

    def navigate(self, *components: str) -> AbstractDTPSContext:
        component_list: list[str] = []
        for component in components:
            split_component = component.split("/")
            component_list_extension = [
                component_ for component_ in split_component if component_
            ]
            component_list.extend(component_list_extension)
        new_components = self.components + tuple(component_list)
        return self.master.get_context_by_components(
            new_components,
            self.config,
        )

    async def patch(
        self,
        patch_data: builtins.list[dict[str, Any]],
        /,
    ) -> None:
        server = self._get_server()
        topic = self._topic
        url0 = topic.as_relative_url()
        resolve = server.resolve_topic_name(topic, url0=url0)
        pdata = JsonPatch(patch_data)
        await resolve.patch(url0, server, pdata)

    async def publish(self, data: RawData, /) -> None:
        server = self._get_server()
        topic = self._topic
        queue = server.get_object_queue(topic)
        await queue.publish(data)

    async def publisher(self) -> AbstractPublisherInterface:
        return self._publisher

    @asynccontextmanager
    async def publisher_context(
        self,
    ) -> AsyncIterator[AbstractPublisherInterface]:
        yield self._publisher

    @staticmethod
    def _get_transform_use(transform: RPCFunction) -> Any:
        async def transform_use(
            object_transform_context: ObjectTransformContext,
        ) -> RawData | TransformError:
            return await transform(object_transform_context.raw_data)

        return transform_use

    async def queue_create(
        self,
        *,
        transform: RPCFunction | None = None,
        serve: ServeFunction | None = None,
        content_info: ContentInfo | None = None,
        topic_properties: TopicProperties | None = None,
        app_data: dict[str, bytes] | None = None,
        bounds: Bounds | None = None,
    ) -> "ContextManagerCreateContext":
        if bounds is None:
            bounds = Bounds.default()
        server = self._get_server()
        topic = self._topic
        if transform is None:
            transform_use = dtps_http.transform_identity
        else:
            transform_use = self._get_transform_use(transform)
        if bounds is None:
            bounds = Bounds.default()
        if content_info is None:
            content_info = ContentInfo.simple(MIME_OCTET)
        if topic_properties is None:
            topic_properties = TopicProperties.rw_pushable()
        if app_data is None:
            app_data = {}
        await server.create_object_queue(
            topic,
            content_info=content_info,
            topic_properties=topic_properties,
            transform=transform_use,
            serve=serve,
            bounds=bounds,
            app_data=app_data,
        )
        return self

    async def remove(self) -> None:
        topic = self._topic
        server = self._get_server()
        url = topic.as_relative_url()
        source = server.resolve_topic_name(topic, url0=url)
        if isinstance(source, OurQueue):
            await server.remove_object_queue(topic)
        elif isinstance(source, ForwardedQueue):
            message = "Cannot remove a forwarded queue."
            raise NotImplementedError(message)
        elif isinstance(source, SourceComposition):
            message = "Cannot remove a source composition queue."
            raise NotImplementedError(message)
        else:
            message = f"Cannot remove a {source}."
            raise NotImplementedError(message)

    def _get_processor(
        self,
        queue: Queue,
        on_data: Callable[[RawData], Awaitable[None]],
    ) -> Any:
        async def processor() -> None:
            while True:
                data = await queue.get()
                try:
                    await on_data(data)
                except Exception:
                    logger.exception(
                        "Exception in user callback for queue %s:",
                        self,
                    )
                    traceback.print_exc()

        return processor

    @staticmethod
    def _get_wrapped_on_data(queue: Queue) -> Any:
        def wrapped_on_data(data: RawData) -> None:
            try:
                queue.put_nowait(data)
            except QueueFull:
                with suppress(QueueEmpty):
                    queue.get_nowait()
                queue.put_nowait(data)

        return wrapped_on_data

    @staticmethod
    def _get_wrap(when: EveryOnceInAWhile, wrapped_on_data: Any) -> Any:
        def wrap(
            _: ObjectQueue,
            insert_notification: InsertNotification,
        ) -> None:
            if when.now():
                wrapped_on_data(insert_notification.raw_data)

        return wrap

    async def subscribe(
        self,
        on_data: Callable[[RawData], Awaitable[None]],
        /,
        max_frequency: float | None = None,
        queue_size: int = DEFAULT_CALLBACK_QUEUE_SIZE,
        *,
        inline: bool = True,
    ) -> AbstractSubscriptionInterface:
        server = self._get_server()
        object_queue0 = server.get_object_queue(self._topic)
        interval = 1 / max_frequency if max_frequency is not None else 0
        when = EveryOnceInAWhile(interval)
        queue: Queue = Queue(maxsize=queue_size)
        processor = self._get_processor(queue, on_data)
        coroutine = processor()
        loop = asyncio.get_event_loop()
        asyncio.run_coroutine_threadsafe(coroutine, loop)
        wrapped_on_data = self._get_wrapped_on_data(queue)
        wrap = self._get_wrap(when, wrapped_on_data)
        if object_queue0.stored:
            last = object_queue0.last_data()
            wrapped_on_data(last)
        sub_id = object_queue0.subscribe(wrap, max_frequency=max_frequency)
        return ContextManagerCreateContextSubscriber(sub_id, object_queue0)

    @staticmethod
    def _get_sub_diff(
        differ: "Differ",
        on_data: Callable[[PatchType], Awaitable[None]],
    ) -> Any:
        async def sub_diff(data: RawData) -> None:
            patch = differ.push(data)
            if patch is None:
                return
            await on_data(patch)

        return sub_diff

    async def subscribe_diff(
        self,
        on_data: Callable[[PatchType], Awaitable[None]],
        /,
    ) -> AbstractSubscriptionInterface:
        differ = Differ()
        sub_diff = self._get_sub_diff(differ, on_data)
        return await self.subscribe(sub_diff)

    async def until_ready(
        self,
        _: float = 1,
        __: int | None = None,
        ___: float | None = None,
        ____: float = 10,
        *,
        quiet: bool = False,
    ) -> None:
        return


class Differ:
    """Differ."""

    current: object | None
    first_arrived: bool

    def __init__(self) -> None:
        """Initialize differ."""
        self.current = None
        self.first_arrived = False

    def push(self, raw_data: RawData) -> PatchType | None:
        if not self.first_arrived:
            self.first_arrived = True
            self.current = raw_data.get_as_native_object()
            return [
                {
                    "op": "replace",
                    "path": "",
                    "value": self.current,
                },
            ]
        previous = self.current
        new = raw_data.get_as_native_object()
        if previous == new:
            return []
        patch = JsonPatch.from_diff(previous, new)
        operations = patch.to_string(lambda f: f)
        self.current = new
        return typing.cast(PatchType, operations)
