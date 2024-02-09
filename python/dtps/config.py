import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import (
    AsyncContextManager,
    AsyncIterator,
    cast,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
    TYPE_CHECKING,
)

from dtps_http import parse_url_unescape, ServerWrapped, URLString
from . import logger
from .ergo_ui import (
    DTPSContext,
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


        c = context("mio", environment={'DTPS_BASE_MIO': url})

    """
    base_name = base_name.lower()
    if environment is None:
        if base_name in ContextManager.instances:
            return ContextManager.instances[base_name].get_context()
        else:
            context_manager = await create_context(base_name, environment)
            ContextManager.instances[base_name] = context_manager
            return context_manager.get_context()
    else:
        context_manager = await create_context(base_name, environment)
        return context_manager.get_context()


if TYPE_CHECKING:

    def context_cleanup(
        base_name: str = "self", environment: Optional[Mapping[str, str]] = None
    ) -> AsyncContextManager[DTPSContext]:
        ...

else:

    @asynccontextmanager
    async def context_cleanup(
        base_name: str = "self", environment: Optional[Mapping[str, str]] = None
    ) -> AsyncIterator[DTPSContext]:
        """Context manager to open a context and clean-up later."""
        c = await context(base_name, environment)
        try:
            yield c
        finally:
            await c.aclose()


async def create_context(base_name: str, environment: Optional[Mapping[str, str]]) -> "ContextManager":
    contexts = get_context_info(environment)
    if base_name not in contexts.contexts:
        msg = f'Cannot find context "{base_name}" among {list(contexts.contexts)}'
        raise KeyError(msg)

    context_info = contexts.contexts[base_name]
    logger.debug(f'Creating context "{base_name}" with {context_info} for environment {environment}')
    return await ContextManager.create(base_name, context_info)


class ContextManager:
    instances: ClassVar[Dict[str, "ContextManager"]] = {}

    context_info: "ContextInfo"

    dtps_server_wrap: Optional[ServerWrapped]

    @classmethod
    async def create(cls, base_name: str, context_info: "ContextInfo") -> "ContextManager":
        # if base_name in cls.instances:
        #     msg = f'Context "{base_name}" already exists'
        #     raise KeyError(msg)

        # logger.info(f'Creating context "{base_name}" with {context_info}')

        if context_info.is_create():
            from .ergo_create import ContextManagerCreate

            cm = ContextManagerCreate(base_name, context_info)
        else:
            from .ergo_use import ContextManagerUse

            cm = ContextManagerUse(base_name, context_info)

        # cls.instances[base_name] = cm
        await cm.init()
        return cm

    def get_context(self) -> "DTPSContext":
        raise NotImplementedError()


@dataclass
class ContextUrl:
    url: URLString
    create: bool

    def __post_init__(self) -> None:
        parse_url_unescape(self.url)


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
                if not url_.port:
                    port = 0
                elif isinstance(url_.port, str):
                    port = int(url_.port)
                else:
                    port = url_.port

                tcp.append((host, port))
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
        rest = k[len(BASE):]

        name, _, rest = rest.partition("_")

        name = name.lower()
        if name not in contexts:
            contexts[name] = ContextInfo(urls=[])

        if v.startswith("create:"):
            x = cast(URLString, v[len("create:"):])
            create = True

        else:
            x = cast(URLString, v)
            create = False
        try:
            parse_url_unescape(x)
        except ValueError as e:
            msg = f"Invalid url given by environment:\n{k} = {v}\nExtracted url: {x}"
            raise ValueError(msg) from e
        contexts[name].urls.append(ContextUrl(url=x, create=create))

    for name, info in contexts.items():
        all_create = all(x.create for x in info.urls)
        all_not_create = all(not x.create for x in info.urls)
        if not all_create and not all_not_create:
            msg = f'Invalid context "{name}". All urls must be either "create:" or not.'
            raise ValueError(msg)
    return ContextsInfo(contexts=contexts)
