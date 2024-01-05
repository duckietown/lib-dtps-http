import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Callable, cast, Dict, List, Optional, Tuple

import cbor2
from aiohttp import ClientResponseError

from dtps_http import (
    CONTENT_TYPE_PATCH_CBOR,
    ContentInfo,
    DTPSClient,
    join,
    MIME_OCTET,
    NodeID,
    ObjectTransformFunction,
    parse_url_unescape,
    RawData,
    TopicNameV,
    TopicProperties,
    TopicRefAdd,
    URL,
    url_to_string,
    URLIndexer,
)
from dtps_http.client import ListenDataInterface
from dtps_http.structures import ConnectionJob
from . import logger
from .config import ContextInfo, ContextManager
from .ergo_ui import (
    ConnectionInterface,
    DTPSContext,
    HistoryInterface,
    PublisherInterface,
    SubscriptionInterface,
)

__all__ = [
    "ContextManagerUse",
]


class ContextManagerUse(ContextManager):
    best_url: URLIndexer
    all_urls: List[URL]

    client: DTPSClient

    def __init__(self, base_name: str, context_info: "ContextInfo"):
        self.client = DTPSClient(nickname=base_name, shutdown_event=None)
        self.context_info = context_info
        self.contexts = {}
        self.base_name = base_name
        assert not self.context_info.is_create()

    async def init(self) -> None:
        await self.client.init()
        alternatives = [(cast(URLIndexer, parse_url_unescape(_.url)), None) for _ in self.context_info.urls]
        best_url = await self.client.find_best_alternative(alternatives)

        self.all_urls = [u for (u, _) in alternatives]
        if best_url is None:
            msg = f"Could not connect to any of {alternatives}"
            raise ValueError(msg)

        self.best_url = best_url

    async def aclose(self) -> None:
        await self.client.aclose()

    def get_context_by_components(self, components: Tuple[str, ...]) -> "DTPSContext":
        if components not in self.contexts:
            self.contexts[components] = ContextManagerUseContext(self, components)

        return self.contexts[components]

    def get_context(self) -> "DTPSContext":
        return self.get_context_by_components(())


class ContextManagerUseContextPublisher(PublisherInterface):
    queue_in: "asyncio.Queue[RawData]"
    queue_out: "asyncio.Queue[bool]"
    task_push: asyncio.Task

    def __init__(self, master: "ContextManagerUseContext"):
        self.master = master

        self.queue_in = asyncio.Queue()
        self.queue_out = asyncio.Queue()

    async def init(self) -> None:
        url_topic = await self.master._get_best_url()
        self.task_push = await self.master.master.client.push_continuous(
            url_topic, queue_in=self.queue_in, queue_out=self.queue_out
        )

    async def publish(self, rd: RawData, /) -> None:
        await self.queue_in.put(rd)
        success = await self.queue_out.get()
        if not success:
            raise Exception(f"Could not push {rd!r}")

    async def terminate(self) -> None:
        self.task_push.cancel()


class ContextManagerUseSubscription(SubscriptionInterface):
    def __init__(self, ldi: ListenDataInterface):
        self.ldi = ldi

    async def unsubscribe(self) -> None:
        await self.ldi.stop()


class ContextManagerUseContext(DTPSContext):
    def __init__(self, master: ContextManagerUse, components: Tuple[str, ...]):
        self.master = master
        self.components = components

    async def aclose(self) -> None:
        await self.master.aclose()

    async def get_urls(self) -> List[str]:
        all_urls = self.master.all_urls
        rurl = self._get_components_as_topic().as_relative_url()
        return [url_to_string(join(u, rurl)) for u in all_urls]

    async def get_node_id(self) -> Optional[NodeID]:
        url = await self._get_best_url()
        md = await self.master.client.get_metadata(url)
        return md.origin_node

    async def exists(self) -> bool:
        url = await self._get_best_url()
        client = self.master.client
        try:
            await client.get_metadata(url)
            return True
        except ClientResponseError as e:
            if e.status == 404:
                # logger.debug(f"exists: {url} -> 404 -> {e}")
                return False
            else:
                raise

    async def patch(self, patch_data: List[Dict[str, Any]], /) -> None:
        url = await self._get_best_url()
        data = cbor2.dumps(patch_data)
        res = await self.master.client.patch(url, CONTENT_TYPE_PATCH_CBOR, data)

    def _get_components_as_topic(self) -> TopicNameV:
        return TopicNameV.from_components(self.components)

    def navigate(self, *components: str) -> "DTPSContext":
        return self.master.get_context_by_components(self.components + components)

    async def list(self) -> List[str]:
        # TODO: DTSW-4801: implement list()
        raise NotImplementedError()

    async def remove(self) -> None:
        url = await self._get_best_url()
        return await self.master.client.delete(url)

    async def data_get(self) -> RawData:
        url = await self._get_best_url()
        return await self.master.client.get(url, None)

    async def subscribe(
        self, on_data: Callable[[RawData], Awaitable[None]], /, max_frequency: Optional[float] = None
    ) -> "SubscriptionInterface":
        url = await self._get_best_url()
        # ldi = await self.master.client.listen_url(url, on_data, inline_data=False, raise_on_error=False)
        inline_data = max_frequency is None
        ldi = await self.master.client.listen_url(url, on_data, inline_data=inline_data, raise_on_error=True)
        # logger.debug(f"subscribed to {url} -> {t}")
        return ContextManagerUseSubscription(ldi)

    async def history(self) -> "Optional[HistoryInterface]":
        # TODO: DTSW-4803: [use] implement history
        raise NotImplementedError()

    async def _get_best_url(self) -> URL:
        topic = self._get_components_as_topic()
        url = join(self.master.best_url, topic.as_relative_url())
        return url

    async def publish(self, data: RawData) -> None:
        url = await self._get_best_url()
        await self.master.client.publish(url, data)

    async def publisher(self) -> "ContextManagerUseContextPublisher":
        publisher = ContextManagerUseContextPublisher(self)
        await publisher.init()
        return publisher

    @asynccontextmanager
    async def publisher_context(self) -> AsyncIterator["PublisherInterface"]:
        publisher = await self.publisher()
        try:
            yield publisher
        finally:
            await publisher.terminate()

    async def call(self, data: RawData) -> RawData:
        client = self.master.client
        url = await self._get_best_url()
        return await client.call(url, data)

    async def expose(self, c: DTPSContext) -> "DTPSContext":
        topic = self._get_components_as_topic()
        url0 = self.master.best_url
        urls = await c.get_urls()
        node_id = await c.get_node_id()
        await self.master.client.add_proxy(cast(URLIndexer, url0), topic, node_id, urls, mask_origin=False)
        return self

    async def queue_create(
        self,
        *,
        parameters: Optional[TopicRefAdd] = None,
        transform: Optional[ObjectTransformFunction] = None,
    ) -> "DTPSContext":
        topic = self._get_components_as_topic()

        url = await self._get_best_url()

        if transform is not None:
            msg = "transform is not supported for remote queues"
            raise ValueError(msg)

        try:
            md = await self.master.client.get_metadata(url)
        except ClientResponseError:
            logger.debug("OK: queue_create: does not exist: %s", url)
            # TODO: check 404
            pass
        else:
            logger.debug(f"queue_create: already exists: {url}")
            return self

        if parameters is None:
            parameters = TopicRefAdd(
                content_info=ContentInfo.simple(MIME_OCTET),
                properties=TopicProperties.rw_pushable(),
                app_data={},
            )

        await self.master.client.add_topic(self.master.best_url, topic, parameters)
        return self

    async def connect_to(self, c: "DTPSContext", /) -> "ConnectionInterface":
        # TODO: DTSW-4805: [use] implement connect_to

        if not isinstance(c, ContextManagerUseContext):
            raise TypeError(f"Expected ContextManagerUseContext, got {type(c)}")

        topic1 = self._get_components_as_topic()
        topic2 = c._get_components_as_topic()

        url = self.master.best_url

        connection_job = ConnectionJob(source=topic1, target=topic2, service_mode="AllMessages")
        name = topic1 + topic2
        await self.master.client.connect(url, name, connection_job)

        return ConnectionInterfaceImpl(self.master, url, name)


class ConnectionInterfaceImpl(ConnectionInterface):
    def __init__(self, master: ContextManagerUse, url: URLIndexer, connection_name: TopicNameV):
        self.master = master
        self.url = url

        self.connection_name = connection_name

    async def disconnect(self) -> None:
        await self.master.client.disconnect(self.url, self.connection_name)

        raise NotImplementedError()
        pass
