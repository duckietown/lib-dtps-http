"""Ergo UI."""

__all__ = [
    "AbstractDTPSContext",
    "AbstractHistoryInterface",
    "AbstractPublisherInterface",
    "AbstractSubscriptionInterface",
    "ContextConfig",
    "PatchType",
    "RPCFunction",
    "ServeFunction",
]

import builtins
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dtps.ergo_use import ConnectionInterface
from dtps_http import (
    DEFAULT_CALLBACK_QUEUE_SIZE,
    Bounds,
    ContentInfo,
    DataSaved,
    HTTPRequest,
    ListenerInfo,
    NodeID,
    ObjectServeResult,
    ObjectTransformResult,
    RawData,
    TopicProperties,
    URLString,
)

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager

_ = Sequence
RPCFunction = Callable[[RawData], Awaitable[ObjectTransformResult]]
PatchType = list[dict[str, Any]]
ServeFunction = Callable[[HTTPRequest], Awaitable[ObjectServeResult]]


class AbstractDTPSContext(ABC):
    """Abstract DTPS context."""

    @abstractmethod
    def navigate(self, *components: str) -> "AbstractDTPSContext":
        """Return a sub-resource.

        Returns a sub-resource. For example:

            context = context.navigate("a", "b", "c")

        Slashes are normalized, so the following is equivalent to the
        above:

            context = context.navigate("a/b/c")
        """

    def __truediv__(self, other: str) -> "AbstractDTPSContext":
        """Shortcut for `navigate`.

        Can be used to navigate to a sub-resource using a path-like
        syntax:

            context = context / "a" / "b" / "c"

        Slashes are normalized, so the following is equivalent to the
        above:

            context = context / "a/b" / "c"

        """
        components = other.split("/")
        return self.navigate(*components)

    @abstractmethod
    def get_config(self) -> "ContextConfig":
        """Return the configuration of the context."""

    @abstractmethod
    def configure(self, cc: "ContextConfig", /) -> "AbstractDTPSContext":
        """Configure the context (recursively).

        Returns a different context (representing the same resource)
        with the given configuration.
        """

    @abstractmethod
    async def exists(self) -> bool:
        """Check if this resource exists."""

    @abstractmethod
    async def list(self) -> list[str]:
        """List the subtopics.

        TODO: What information should be returned? Should it be a
        `dict`? Should it be recursive?
        """

    @abstractmethod
    async def get_urls(self) -> builtins.list[URLString]:
        """List urls that might reach this topic."""

    @abstractmethod
    async def get_node_id(self) -> NodeID | None:
        """Return the node_id if this is a DTPS node."""

    @abstractmethod
    def get_path_components(self) -> tuple[str, ...]:
        """Return the path of this context as a tuple of strings."""

    # creation and deletion

    @abstractmethod
    async def remove(self) -> None:
        """Remove."""

    # getting

    @abstractmethod
    async def data_get(self) -> RawData:
        """Return data."""

    @abstractmethod
    async def subscribe(
        self,
        on_data: Callable[[RawData], Awaitable[None]],
        /,
        max_frequency: float | None = None,
        queue_size: int = DEFAULT_CALLBACK_QUEUE_SIZE,
        *,
        inline: bool = True,
    ) -> "AbstractSubscriptionInterface":
        """Subscribe.

        The subscription is persistent: if the topic is not available,
        we wait until it is (up to a timeout).
        """

    @abstractmethod
    async def subscribe_diff(
        self,
        on_data: Callable[[PatchType], Awaitable[None]],
        /,
    ) -> "AbstractSubscriptionInterface":
        """Return the data stream as a series of diffs, as JSON patch.

        Note: the first call will return the full data (set / value).
        """

    @abstractmethod
    async def history(self) -> "AbstractHistoryInterface | None":
        """Return `None` if history is not available."""

    # pushing

    @abstractmethod
    async def publish(self, data: RawData, /) -> None:
        """Publish data to the resource.

        Meant to be used for infrequent pushes. For frequent pushes, use
        the `publisher` interface.
        """

    @abstractmethod
    async def publisher(self) -> "AbstractPublisherInterface":
        """Return a publisher.

        Returns a publisher that can be used to publish data to the
        resource. This call creates a connection that will be terminated
        only when the publisher is closed using the `terminate` method.

        For example:
            publisher = await context.publisher()

            try:
                for _ in range(10):
                    await publisher.publish(data)
            finally:
                await publisher.terminate()
        """

    @abstractmethod
    def publisher_context(
        self,
    ) -> "AbstractAsyncContextManager[AbstractPublisherInterface]":
        """Return an asynchronous context manager.

        Returns an asynchronous context manager that returns a publisher
        that is cleaned up when the context is exited.

        For example:
            async with context.publisher_context() as publisher:
                for _ in range(10):
                    await publisher.publish(data)
        """

    @abstractmethod
    async def call(self, data: RawData, /) -> RawData:
        """Create an RPC call (push with response)."""

    # patch
    @abstractmethod
    async def patch(self, patch_data: PatchType, /) -> None:
        """Apply a patch to the resource.

        The patch is a list of operations, as defined in `RFC 6902`.
        """

    # proxy

    @abstractmethod
    async def expose(
        self,
        urls: "Sequence[str] | AbstractDTPSContext",
        /,
        *,
        mask_origin: bool = False,
    ) -> None:
        """Expose.

        Creates this resource as a proxy to the given URLs or to the
        context.
        """

    @abstractmethod
    async def queue_create(
        self,
        *,
        transform: RPCFunction | None = None,
        serve: ServeFunction | None = None,
        content_info: ContentInfo | None = None,
        topic_properties: TopicProperties | None = None,
        app_data: dict[str, bytes] | None = None,
        bounds: Bounds | None = None,
    ) -> "AbstractDTPSContext":
        """Create queue.

        Creates this resource as a queue (if it doesn't exist). You can
        specify the parameters of the queue, such as the content type,
        bounds, etc.

        content_info: the content type of the data
          default= ContentInfo.simple(MIME_OCTET)

        topic_properties: the properties of the topic
            default= TopicProperties.rw_pushable()

        bounds: the bounds of the topic
            default= Bounds.default() (max length = 10)

        app_data: a dictionary with additional information for the
        application.

        Returns self.
        """

    @abstractmethod
    def meta(self) -> "AbstractDTPSContext":
        """Return the metadata of the resource."""

    @abstractmethod
    async def until_ready(
        self,
        retry_every: float = 1,
        retry_max: int | None = None,
        timeout: float | None = None,
        print_every: float = 10,
        *,
        quiet: bool = False,
    ) -> None:
        """Wait until the resource is ready."""

    # connection

    @abstractmethod
    async def connect_to(
        self,
        context: "AbstractDTPSContext",
        /,
    ) -> ConnectionInterface:
        """Connect to resource.

        Adds a connection between this resource and the resource
        identified by the argument.
        """

    @abstractmethod
    async def aclose(self) -> None:
        """Close asynchronously.

        Cleans up all resources associated with the root of this
        context.
        """


class AbstractHistoryInterface(ABC):
    """Abstract history interface."""

    @abstractmethod
    async def summary(self, nmax: int, /) -> dict[int, DataSaved]:
        """Return a summary of the history (at most nmax entries)."""

    @abstractmethod
    async def get(self, index: int, /) -> RawData:
        """Return the data at the given index."""


@dataclass
class AbstractPublisherInterface(ABC):
    """Abstract publisher interface."""

    @abstractmethod
    async def publish(self, raw_data: RawData, /) -> None:
        """Publish data to the resource."""

    @abstractmethod
    async def terminate(self) -> None:
        """Stop the publisher."""

    @abstractmethod
    async def get_listener_info(self) -> ListenerInfo | None:
        """Return information about the listener."""


class AbstractSubscriptionInterface(ABC):
    """Abstract subscription interface."""

    @abstractmethod
    async def unsubscribe(self) -> None:
        """Stop the subscription."""


@dataclass(frozen=True, eq=True, order=True, unsafe_hash=True)
class ContextConfig:
    """Context config.

    `None` means to use the default value.
    """

    patient: bool | None = None

    @classmethod
    def default(cls) -> "ContextConfig":
        """Return default."""
        return cls()

    def specialize(self, other: "ContextConfig") -> "ContextConfig":
        """Return specialize."""
        return ContextConfig(
            patient=other.patient
            if other.patient is not None
            else self.patient,
        )
