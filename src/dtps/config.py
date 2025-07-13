"""Config."""

__all__ = ["context", "context_cleanup"]

import os
import typing
from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

import dtps_http
from dtps import logger
from dtps.ergo_abstract import AbstractDTPSContext
from dtps.ergo_create import ContextManagerCreate
from dtps.ergo_use import ContextManagerUse
from dtps_http import ServerWrapped, URLString

BASE = "DTPS_BASE_"


@dataclass
class ContextInfo:
    urls: list["ContextUrl"]

    def get_tcp_and_unix(self) -> tuple[list[tuple[str, int]], list[str]]:
        tcp: list[tuple[str, int]] = []
        unix: list[str] = []
        for url in self.urls:
            url_ = dtps_http.parse_url_unescape(url.url)
            if url_.scheme == "http+unix":
                host = url_.host
                unix.append(host)
            elif url_.scheme in ("http", "https"):
                host = url_.host or "localhost"
                if not url_.port:
                    port = 0
                elif isinstance(url_.port, str):
                    port = int(url_.port)
                else:
                    port = url_.port
                tcp.append((host, port))
            else:
                message = (
                    f"Invalid url '{url_}'. Must start with 'http://' or "
                    "'http+unix://'."
                )
                raise ValueError(message)
        return tcp, unix

    def is_create(self) -> bool:
        return all(url.create for url in self.urls)


class ContextManager:
    """Context manager."""

    context_info: "ContextInfo"
    dtps_server_wrap: ServerWrapped | None
    instances: ClassVar[dict[str, "ContextManager"]] = {}

    @classmethod
    async def create(
        cls,
        base_name: str,
        context_info: "ContextInfo",
    ) -> "ContextManager":
        context_manager: ContextManagerCreate | ContextManagerUse
        if context_info.is_create():
            context_manager = ContextManagerCreate(base_name, context_info)
        else:
            context_manager = ContextManagerUse(base_name, context_info)
        await context_manager.initialize()
        return context_manager

    async def initialize(self) -> None:
        """Initialize."""

    def get_context(self) -> AbstractDTPSContext:
        raise NotImplementedError


@dataclass
class ContextsInfo:
    contexts: dict[str, ContextInfo]


@dataclass
class ContextUrl:
    url: URLString
    create: bool

    def __post_init__(self) -> None:
        dtps_http.parse_url_unescape(self.url)


async def context(
    base_name: str = "self",
    environment: Mapping[str, str] | None = None,
    urls: list[str] | None = None,
) -> AbstractDTPSContext:
    """Initialize a DTPS interface.

    Initializes a DTPS interface from the environment from a given base
    name. For example:

        context_ = context("mio", environment={
            "DTPS_BASE_MIO": url
        })

    Note that `base_name` is case-insensitive.

    Environment variables of the form `DTPS_BASE_<base_name>` are used
    to get the info needed. For example:

        # use an existing server
        DTPS_BASE_SELF = "http://localhost:2120/"
        # use an existing unix socket
        DTPS_BASE_SELF = "http+unix://[socket]/"

    We can also use the special prefix `create:` to create a new
    server. For example:

        # create a new server
        DTPS_BASE_SELF = "create:http://localhost:2120/"

    Moreover, we can use more than one base name, by adding a number at
    the end:

        DTPS_BASE_SELF_0 = "create:http://localhost:2120/"
        DTPS_BASE_SELF_1 = "create:http+unix://[socket]/"


    You need to call `context.aclose()` at the end to clean up
    resources.
    """
    base_name = base_name.lower()
    if environment is not None and urls is not None:
        message = (
            "You cannot create a context while passing both 'environment' and "
            "'urls'."
        )
        raise ValueError(message)
    if urls:
        environment = environment_from_urls(base_name, urls)
    if environment is None:
        if base_name in ContextManager.instances:
            return ContextManager.instances[base_name].get_context()
        context_manager = await create_context(base_name, environment)
        ContextManager.instances[base_name] = context_manager
        return context_manager.get_context()
    context_manager = await create_context(base_name, environment)
    return context_manager.get_context()


if TYPE_CHECKING:

    def context_cleanup(
        base_name: str = "self",
        environment: Mapping[str, str] | None = None,
    ) -> AbstractAsyncContextManager[AbstractDTPSContext]:
        """Context cleanup."""
else:

    @asynccontextmanager
    async def context_cleanup(
        base_name: str = "self",
        environment: Mapping[str, str] | None = None,
    ) -> AsyncIterator[AbstractDTPSContext]:
        """Context manager to open a context and clean-up later."""
        context_ = await context(base_name, environment)
        try:
            yield context_
        finally:
            await context_.aclose()


async def create_context(
    base_name: str,
    environment: Mapping[str, str] | None,
) -> ContextManager:
    contexts = get_context_info(environment)
    if base_name not in contexts.contexts:
        message = (
            f"Cannot find context '{base_name}' among "
            f"{list(contexts.contexts)}."
        )
        raise KeyError(message)
    context_info = contexts.contexts[base_name]
    logger.debug(
        "Creating context '%s' with %s for environment %s...",
        base_name,
        context_info,
        environment,
    )
    return await ContextManager.create(base_name, context_info)


def environment_from_urls(name: str, urls: list[str]) -> dict[str, str]:
    return {f"{BASE}{name}_{i}": url for i, url in enumerate(urls)}


def get_context_info(environment: Mapping[str, str] | None) -> ContextsInfo:
    if environment is None:
        environment = dict(os.environ)
    contexts: dict[str, ContextInfo] = {}
    for key, value in environment.items():
        if not key.startswith(BASE):
            continue
        rest = key[len(BASE) :]
        name, _, rest = rest.partition("_")
        name = name.lower()
        if name not in contexts:
            contexts[name] = ContextInfo(urls=[])
        if value.startswith("create:"):
            url = typing.cast(URLString, value[7:])
            create = True
        else:
            url = typing.cast(URLString, value)
            create = False
        try:
            dtps_http.parse_url_unescape(url)
        except ValueError as error:
            message = (
                f"Invalid url given by environment:\n{key} = {value}\n"
                f"Extracted url: {url}"
            )
            raise ValueError(message) from error
        context_url = ContextUrl(url=url, create=create)
        contexts[name].urls.append(context_url)
    for name, info in contexts.items():
        all_create = all(url.create for url in info.urls)
        all_not_create = all(not url.create for url in info.urls)
        if not all_create and not all_not_create:
            message = (
                f"Invalid context '{name}'. All urls must be either 'create:' "
                "or not."
            )
            raise ValueError(message)
    return ContextsInfo(contexts=contexts)
