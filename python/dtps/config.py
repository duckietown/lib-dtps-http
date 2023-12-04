import asyncio
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Awaitable, Callable, cast, ClassVar, Dict, Iterator, List, Mapping, Optional, Tuple

from aiohttp import ClientResponseError

from dtps_http import (
    DTPSClient,
    DTPSServer,
    join,
    NodeID,
    ObjectQueue,
    parse_url_unescape,
    RawData,
    TopicNameV,
    URL,
    URLString,
    URLTopic,
)
from dtps_http.constants import MIME_OCTET
from dtps_http.server_start import app_start, ServerWrapped
from dtps_http.structures import ContentInfo, DataSaved, TopicProperties, TopicRefAdd
from . import logger
from .ergo_ui import (
    ConnectionInterface,
    DTPSContext,
    HistoryContext,
    PublisherInterface,
    SubscriptionInterface,
)

__all__ = [
    "context",
    "context_cleanup",
]


async def context(base_name: str = "self", environment: Optional[Mapping[str, str]] = None) -> "DTPSContext":
    """
    Initialize a DTPS interface from the environment from a given base name.

    base_name is case-insensitive.

    Environment variables of the form DTPS_BASE_<base_name> are used to get the info needed.

    For example:

        DTPS_BASE_SELF = "http://localhost:2120/" # use an existing server
        DTPS_BASE_SELF = "http+unix://[socket]/" # use an existing unix socket

    We can also use the special prefix "create:" to create a new server.
    For example:

        DTPS_BASE_SELF = "create:http://localhost:2120/" # create a new server

    Moreover, we can use more than one base name, by adding a number at the end:

        DTPS_BASE_SELF_0 = "create:http://localhost:2120/" #
        DTPS_BASE_SELF_1 = "create:http+unix://[socket]/"


    You need to call context.aclose() at the end to clean up resources.

    """
    base_name = base_name.lower()

    if base_name not in ContextManager.instances:
        await create_context(base_name, environment)
    return ContextManager.instances[base_name].get_context()


@asynccontextmanager
async def context_cleanup(
    base_name: str = "self", environment: Optional[Mapping[str, str]] = None
) -> Iterator[DTPSContext]:
    """Context manager to open a context and clean-up later."""
    c = await context(base_name, environment)
    try:
        yield c
    finally:
        await c.aclose()


async def create_context(base_name: str, environment: Optional[Mapping[str, str]]) -> None:
    contexts = get_context_info(environment)
    if base_name not in contexts.contexts:
        msg = f'Cannot find context "{base_name}" among {list(contexts.contexts)}'
        raise KeyError(msg)

    context_info = contexts.contexts[base_name]
    logger.info(f'Creating context "{base_name}" with {context_info}')
    await ContextManager.create(base_name, context_info)


class ContextManager:
    instances: ClassVar[Dict[str, "ContextManager"]] = {}

    context_info: "ContextInfo"

    dtps_server_wrap: Optional[ServerWrapped]

    @classmethod
    async def create(cls, base_name: str, context_info: "ContextInfo") -> "ContextManager":
        if base_name in cls.instances:
            msg = f'Context "{base_name}" already exists'
            raise KeyError(msg)

        if context_info.is_create():
            cm = ContextManagerCreate(base_name, context_info)
        else:
            cm = ContextManagerUse(base_name, context_info)

        cls.instances[base_name] = cm
        await cm.init()
        return cm

        #
        # self = cls(base_name, context_info)
        # await self.init()
        # cls.instances[base_name] = self
        # return self
        #

    def get_context(self) -> "DTPSContext":
        raise NotImplementedError()


class ContextManagerCreate(ContextManager):
    dtps_server_wrap: Optional[ServerWrapped]

    # client: DTPSClient
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
        await self.master.publish(rd)

    async def terminate(self) -> None:
        pass


class ContextManagerCreateContextSubscriber(SubscriptionInterface):
    async def unsubscribe(self) -> None:
        # TODO: implement
        pass


class ContextManagerCreateContext(DTPSContext):
    _publisher: Optional[ContextManagerCreateContextPublisher]

    def __init__(self, master: ContextManagerCreate, components: Tuple[str, ...]):
        self.master = master
        self.components = components
        self._publisher = ContextManagerCreateContextPublisher(self)

    async def aclose(self) -> None:
        await self.master.aclose()

    async def terminate_publisher(self) -> None:
        if self._publisher is not None:
            # TODO: cleanup here
            self._publisher = None

    async def get_urls(self) -> List[str]:
        server = self._get_server()
        urls = server.available_urls
        rurl = self._get_components_as_topic().as_relative_url()
        return [f"{u}{rurl}" for u in urls]

    async def get_node_id(self) -> Optional[str]:
        # TODO: this could be remote
        server = self._get_server()
        return server.node_id

    def _get_server(self) -> DTPSServer:
        return self.master.dtps_server_wrap.server

    def _get_components_as_topic(self) -> TopicNameV:
        return TopicNameV.from_components(self.components)

    def navigate(self, *components: str) -> "DTPSContext":
        return self.master.get_context_by_components(self.components + components)

    async def list(self) -> List[str]:
        raise NotImplementedError()

    async def remove(self) -> None:
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
                oq0.unsubscribe(sub_id)

        return Subscription()

    async def history(self) -> "Optional[HistoryContext]":
        raise NotImplementedError()

    async def publish(self, data: RawData, /) -> None:
        server = self._get_server()
        topic = self._get_components_as_topic()
        queue = server.get_oq(topic)
        await queue.publish(data)

    async def publisher(self) -> "PublisherInterface":
        return self._publisher

    async def call(self, data: RawData, /) -> RawData:
        raise NotImplementedError()

    async def expose(self, urls: "Sequence[str] | DTPSContext", /) -> "DTPSContext":
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
        raise NotImplementedError()


class ContextManagerUse(ContextManager):
    best_url: URLTopic
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
        alternatives = [(parse_url_unescape(_.url), None) for _ in self.context_info.urls]
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
    def __init__(self, master: "ContextManagerUseContext"):
        self.master = master

    async def init(self) -> None:
        pass

    async def aclose(self) -> None:
        pass

    async def publish(self, rd: RawData, /) -> None:
        await self.master.publish(rd)

    async def terminate(self) -> None:
        pass


class ContextManagerUseContext(DTPSContext):
    _publisher: Optional[ContextManagerUseContextPublisher]

    def __init__(self, master: ContextManagerUse, components: Tuple[str, ...]):
        self.master = master
        self.components = components
        self._publisher = None

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

    # async def terminate_publisher(self) -> None:
    # if self._publisher is not None:
    #     # TODO: cleanup here
    #     self._publisher = None

    def _get_components_as_topic(self) -> TopicNameV:
        return TopicNameV.from_components(self.components)

    def navigate(self, *components: str) -> "DTPSContext":
        return self.master.get_context_by_components(self.components + components)

    async def list(self) -> List[str]:
        raise NotImplementedError()

    async def remove(self) -> None:
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
                await self.t0

        return Subscription(t)

    async def history(self) -> "Optional[HistoryContext]":
        raise NotImplementedError()

    async def _get_best_url(self) -> URL:
        topic = self._get_components_as_topic()
        url = join(self.master.best_url, topic.as_relative_url())
        return url

    async def publish(self, data: RawData) -> None:
        url = await self._get_best_url()
        await self.master.client.publish(url, data)

    async def publisher(self) -> "PublisherInterface":
        if self._publisher is None:
            self._publisher = ContextManagerUseContextPublisher(self)
            await self._publisher.init()
        return self._publisher

    async def call(self, data: RawData) -> RawData:
        raise NotImplementedError()

    async def expose(self, c: DTPSContext) -> "DTPSContext":
        topic = self._get_components_as_topic()
        url0 = self.master.best_url
        urls = await c.get_urls()
        node_id = await c.get_node_id()
        await self.master.client.add_proxy(url0, topic, node_id, urls)
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
        raise NotImplementedError()


@dataclass
class ContextUrl:
    url: URLString
    create: bool


@dataclass
class ContextInfo:
    urls: List[ContextUrl]

    def is_create(self) -> bool:
        return all(x.create for x in self.urls)

    def get_tcp_and_unix(self) -> Tuple[List[Tuple[str, int]], List[str]]:
        tcp: List[Tuple[str, int]] = []
        unix: List[str] = []
        for u in self.urls:
            url_ = parse_url_unescape(u.url)
            if url_.scheme == "http+unix":
                host = url_.host
                unix.append(host)

            elif url_.scheme == "http" or url_.scheme == "https":
                # rest = url.url[len("http://") :]
                # host, _, rest = rest.partition("/")
                # host, _, port = host.partition(":")
                # port = int(port)
                host = url_.host or "localhost"

                tcp.append((host, url_.port))
            else:
                msg = f'Invalid url "{url_}". Must start with "http://" or "http+unix://".'
                raise ValueError(msg)
        return tcp, unix


@dataclass
class ContextsInfo:
    contexts: Dict[str, ContextInfo]


BASE = "DTPS_BASE_"


def get_context_info(environment: Optional[Mapping[str, str]]) -> ContextsInfo:
    if environment is None:
        environment = dict(os.environ)

    contexts = {}
    for k, v in environment.items():
        if not k.startswith(BASE):
            continue
        rest = k[len(BASE) :]

        name, _, rest = rest.partition("_")

        name = name.lower()
        if name not in contexts:
            contexts[name] = ContextInfo(urls=[])

        if v.startswith("create:"):
            rest = cast(URLString, v[len("create:") :])
            contexts[name].urls.append(ContextUrl(url=rest, create=True))
        else:
            v = cast(URLString, v)
            contexts[name].urls.append(ContextUrl(url=v, create=False))

    for name, info in contexts.items():
        all_create = all(x.create for x in info.urls)
        all_not_create = all(not x.create for x in info.urls)
        if not all_create and not all_not_create:
            msg = f'Invalid context "{name}". All urls must be either "create:" or not.'
            raise ValueError(msg)
    return ContextsInfo(contexts=contexts)
