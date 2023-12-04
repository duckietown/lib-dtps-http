import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator, Awaitable, Callable, List, Optional, Tuple, cast

from aiohttp import ClientResponseError

from dtps_http import (
    ContentInfo,
    DTPSClient,
    join,
    MIME_OCTET,
    NodeID,
    parse_url_unescape,
    RawData,
    TopicNameV,
    TopicProperties,
    TopicRefAdd,
    URL,
    URLTopic,
)
from dtps_http.urls import URLIndexer
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

    # client: DTPSClient
    def __init__(self, base_name: str, context_info: "ContextInfo"):
        self.client = DTPSClient()
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
        # logger.debug(f'best_url for "{self.base_name}": {best_url}')

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
        # TODO: DTSW-4800: use websocket publishing
        await self.queue_in.put(rd)
        success = await self.queue_out.get()
        if not success:
            raise Exception(f"Could not push {rd!r}")

    async def terminate(self) -> None:
        self.task_push.cancel()


class ContextManagerUseContext(DTPSContext):
    def __init__(self, master: ContextManagerUse, components: Tuple[str, ...]):
        self.master = master
        self.components = components

    async def aclose(self) -> None:
        await self.master.aclose()

    async def get_urls(self) -> List[str]:
        all_urls = self.master.all_urls
        rurl = self._get_components_as_topic().as_relative_url()
        return [str(join(u, rurl)) for u in all_urls]

    async def get_node_id(self) -> Optional[NodeID]:
        url = await self._get_best_url()
        md = await self.master.client.get_metadata(url)
        return md.answering

    def _get_components_as_topic(self) -> TopicNameV:
        return TopicNameV.from_components(self.components)

    def navigate(self, *components: str) -> "DTPSContext":
        return self.master.get_context_by_components(self.components + components)

    async def list(self) -> List[str]:
        # TODO: DTSW-4801: implement list()
        raise NotImplementedError()

    async def remove(self) -> None:
        # TODO: DTSW-4801: implement remove()
        raise NotImplementedError()

    async def data_get(self) -> RawData:
        url = await self._get_best_url()
        return await self.master.client.get(url, None)

    async def subscribe(self, on_data: Callable[[RawData], Awaitable[None]], /) -> "SubscriptionInterface":
        url = await self._get_best_url()
        t = await self.master.client.listen_url(url, on_data, inline_data=False, raise_on_error=False)
        logger.debug(f"subscribed to {url} -> {t}")

        class Subscription(SubscriptionInterface):
            def __init__(self, t0: asyncio.Task):
                self.t0 = t0

            async def unsubscribe(self) -> None:
                self.t0.cancel()
                # await self.t0

        return Subscription(t)

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
        # TODO: DTSW-4804: [use] implement connect_to
        raise NotImplementedError()

    async def expose(self, c: DTPSContext) -> "DTPSContext":
        topic = self._get_components_as_topic()
        url0 = self.master.best_url
        urls = await c.get_urls()
        node_id = await c.get_node_id()
        await self.master.client.add_proxy(cast(URLIndexer, url0), topic, node_id, urls)
        return self

    async def queue_create(self, parameters: Optional[TopicRefAdd] = None, /) -> "DTPSContext":
        topic = self._get_components_as_topic()

        url = await self._get_best_url()

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
        raise NotImplementedError()
