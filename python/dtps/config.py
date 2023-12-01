import os
from dataclasses import dataclass
from typing import Callable, ClassVar, Dict, List, Optional, Tuple

from dtps_http import DTPSServer, parse_url_unescape, RawData, TopicNameV, URLString
from dtps_http.constants import MIME_OCTET
from dtps_http.server_start import app_start, ServerWrapped
from dtps_http.structures import ContentInfo, TopicProperties, TopicRefAdd
from .ergo_ui import (
    ConnectionInterface,
    DTPSContext,
    HistoryContext,
    PublisherInterface,
    SubscriptionInterface,
)

__all__ = [
    "context",
]


async def context(base_name: str = "self") -> "DTPSContext":
    """
    Initialize a DTPS interface from the environment from a given base name.

    base_name is case-insensitive.

    Environment variables of the form DTPS_BASE_<base_name> are used to get the info needed.

    For example:

        DTPS_BASE_SELF = "http://:212/" # use an existing server
        DTPS_BASE_SELF = "unix+http://[socket]/" # use an existing unix socket

    We can also use the special prefix "create:" to create a new server.
    For example:

        DTPS_BASE_SELF = "create:http://:212/" # create a new server

    Moreover, we can use more than one base name, by adding a number at the end:

        DTPS_BASE_SELF_0 = "create:http://:212/" #
        DTPS_BASE_SELF_1 = "create:unix+http://[socket]/"

    """
    base_name = base_name.lower()

    if base_name not in ContextManager.instances:
        await create_context(base_name)
    return ContextManager.instances[base_name].get_context()


async def create_context(base_name: str) -> None:
    contexts = get_context_info()
    if base_name not in contexts.contexts:
        msg = f'Cannot find context "{base_name}" among {list(contexts.contexts)}'
        raise KeyError(msg)

    context_info = contexts.contexts[base_name]
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
            cm = ContextManagerExisting(base_name, context_info)

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

    def __init__(self, base_name: str, context_info: "ContextInfo"):
        self.base_name = base_name
        self.context_info = context_info
        self.dtps_server_wrap = None
        self.contexts = {}
        assert self.context_info.is_create()

    async def init(self) -> None:
        dtps_server = DTPSServer.create()
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


class ContextManagerCreateContext(DTPSContext):
    def __init__(self, master: ContextManagerCreate, components: Tuple[str, ...]):
        self.master = master
        self.components = components

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
        raise NotImplementedError()

    async def subscribe(self, on_data: Callable, /) -> "SubscriptionInterface":
        raise NotImplementedError()

    async def history(self) -> "Optional[HistoryContext]":
        raise NotImplementedError()

    async def publish(self, data: RawData) -> None:
        raise NotImplementedError()

    async def publisher(self) -> "PublisherInterface":
        raise NotImplementedError()

    async def call(self, data: RawData) -> RawData:
        raise NotImplementedError()

    async def expose(self, urls: "Sequence[str] | DTPSContext") -> "DTPSContext":
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

    async def connect_to(self, context: "DTPSContext") -> "ConnectionInterface":
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
        for url in self.urls:
            if url.url.startswith("unix+http://"):
                u = parse_url_unescape(url.url)
                unix.append(str(u.path))
            elif url.url.startswith("http://"):
                rest = url.url[len("http://") :]
                host, _, rest = rest.partition("/")
                host, _, port = host.partition(":")
                port = int(port)
                tcp.append((host, port))
            else:
                msg = f'Invalid url "{url.url}". Must start with "http://" or "unix+http://".'
                raise ValueError(msg)
        return tcp, unix


@dataclass
class ContextsInfo:
    contexts: Dict[str, ContextInfo]


BASE = "DTPS_BASE_"


def get_context_info() -> ContextsInfo:
    envs = dict(os.environ)

    contexts = {}
    for k, v in envs.items():
        if not k.startswith(BASE):
            continue
        rest = k[len(BASE) :]

        name, _, rest = rest.partition("_")

        name = name.lower()
        if name not in contexts:
            contexts[name] = ContextInfo(urls=[])

        if v.startswith("create:"):
            rest = v[len("create:") :]
            contexts[name].urls.append(ContextUrl(url=rest, create=True))
        else:
            contexts[name].urls.append(ContextUrl(url=v, create=False))

    for name, info in contexts.items():
        all_create = all(x.create for x in info.urls)
        all_not_create = all(not x.create for x in info.urls)
        if not all_create and not all_not_create:
            msg = f'Invalid context "{name}". All urls must be either "create:" or not.'
            raise ValueError(msg)
    return ContextsInfo(contexts=contexts)
