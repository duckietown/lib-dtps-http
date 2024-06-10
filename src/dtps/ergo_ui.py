from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    Any,
    AsyncContextManager,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
)

from dtps_http import (
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

__all__ = [
    "ConnectionInterface",
    "ContextConfig",
    "DTPSContext",
    "HistoryInterface",
    "PatchType",
    "PublisherInterface",
    "RPCFunction",
    "ServeFunction",
    "SubscriptionInterface",
]

_ = Sequence
RPCFunction = Callable[[RawData], Awaitable[ObjectTransformResult]]
ServeFunction = Callable[[HTTPRequest], Awaitable[ObjectServeResult]]

PatchType = List[Dict[str, Any]]


@dataclass(frozen=True, eq=True, order=True, unsafe_hash=True)
class ContextConfig:
    """
    None means to use the default value.



    """

    patient: Optional[bool] = None

    @classmethod
    def default(cls) -> "ContextConfig":
        return cls()

    def specialize(self, other: "ContextConfig") -> "ContextConfig":
        return ContextConfig(
            patient=other.patient if other.patient is not None else self.patient,
        )


class DTPSContext(ABC):
    @abstractmethod
    def navigate(self, *components: str) -> "DTPSContext":
        """
        Gets a sub-resource

        Example:

            context = context.navigate('a', 'b', 'c')

        Slashes are normalized, so the following is equivalent:

            context = context.navigate('a/b/c')
        """
        ...

    def __truediv__(self, other: str) -> "DTPSContext":
        """
        Shortcut for navigate.

        Can be used to navigate to a sub-resource using a path-like syntax:

            context = context / 'a' / 'b' / 'c'

        Slashes are normalized, so the following is equivalent:

            context = context / 'a/b' / 'c'

        """
        components = other.split("/")
        return self.navigate(*components)

    @abstractmethod
    def get_config(self) -> ContextConfig:
        """
        Returns the configuration of the context.

        """

    @abstractmethod
    def configure(self, cc: ContextConfig, /) -> "DTPSContext":
        """
        Configures the context (recursively).
        Returns a different context (representing the same resource) with the given configuration.

        """

    @abstractmethod
    async def exists(self) -> bool:
        """
        Checks if this resource exists.

        """

    @abstractmethod
    async def list(self) -> List[str]:
        """
        List the subtopics.

        TODO: what information should be returned? Should it be a dict? Should it be recursive?

        """

    @abstractmethod
    async def get_urls(self) -> List[URLString]:
        """List urls that might reach this topic"""

    @abstractmethod
    async def get_node_id(self) -> Optional[NodeID]:
        """Returns the node_id if this is a DTPS node."""

    # creation and deletion

    @abstractmethod
    async def remove(self) -> None: ...

    # getting

    @abstractmethod
    async def data_get(self) -> RawData: ...

    @abstractmethod
    async def subscribe(
        self,
        on_data: Callable[[RawData], Awaitable[None]],
        /,
        max_frequency: Optional[float] = None,
        inline: bool = True,
    ) -> "SubscriptionInterface":
        """
        The subscription is persistent: if the topic is not available, we wait until
        it is (up to a timeout).
        """
        ...

    @abstractmethod
    async def subscribe_diff(
        self,
        on_data: Callable[[PatchType], Awaitable[None]],
        /,
    ) -> "SubscriptionInterface":
        """
        Obtains the stream of data as a series of diffs, as JSON patch.

        Note: the first call will return the full data. (set / value)
        """
        ...

    @abstractmethod
    async def history(self) -> "Optional[HistoryInterface]":
        """Returns None if history is not available."""
        ...

    # pushing

    @abstractmethod
    async def publish(self, data: RawData, /) -> None:
        """Publishes data to the resource. Meant to be used for infrequent pushes.
        For frequent pushes, use the publisher interface."""
        ...

    @abstractmethod
    async def publisher(self) -> "PublisherInterface":
        """
        Returns a publisher that can be used to publish data to the resource.
        This call creates a connection that will be terminated only when the publisher is closed
        using the terminate() method.



        Example:

            publisher = await context.publisher()

            try:
                for _ in range(10):
                    await publisher.publish(data)
            finally:
                await publisher.terminate()

        """

    @abstractmethod
    def publisher_context(self) -> "AsyncContextManager[PublisherInterface]":
        """
        Returns an async context manager that returns a publisher that is cleaned up when the context is
        exited.

        Example:

            async with context.publisher_context() as publisher:
                for _ in range(10):
                    await publisher.publish(data)

        """

    @abstractmethod
    async def call(self, data: RawData, /) -> RawData:
        """RPC call (push with response)"""

    # patch
    @abstractmethod
    async def patch(self, patch_data: PatchType, /) -> None:
        """
        Applies a patch to the resource.
        The patch is a list of operations, as defined in RFC 6902.

        """

    # proxy

    @abstractmethod
    async def expose(
        self, urls: "Sequence[str] | DTPSContext", /, *, mask_origin: bool = False
    ) -> "DTPSContext":
        """
        Creates this topic as a proxy to the given urls or to the context..

        returns self
        """

    @abstractmethod
    async def queue_create(
        self,
        *,
        transform: Optional[RPCFunction] = None,
        serve: Optional[ServeFunction] = None,
        content_info: Optional[ContentInfo] = None,
        topic_properties: Optional[TopicProperties] = None,
        app_data: Optional[Dict[str, Any]] = None,
        bounds: Optional[Bounds] = None,
    ) -> "DTPSContext":
        """
        Creates this resource as a queue (if it doesn't exist).
        Returns self.

        You can specify the parameters of the queue, such as the content type, the bounds, etc.

        content_info: the content type of the data
          default= ContentInfo.simple(MIME_OCTET)

        topic_properties: the properties of the topic
            default= TopicProperties.rw_pushable()

        bounds: the bounds of the topic
            default= Bounds.default() (max length = 10)

        app_data: a dictionary with additional information for the application



        """

    @abstractmethod
    def meta(self) -> "DTPSContext":
        """
        Returns the metadata of the resource.

        """

    @abstractmethod
    async def until_ready(
        self,
        retry_every: float = 1.0,
        retry_max: Optional[int] = None,
        timeout: Optional[float] = None,
        print_every: float = 10.0,
        quiet: bool = False,
    ) -> "DTPSContext":
        """
        Waits until the resource is ready.
        Returns context pointing to a (now existing) resource.
        Retuns self.
        """

    # connection

    @abstractmethod
    async def connect_to(self, context: "DTPSContext", /) -> "ConnectionInterface":
        """Add a connection between this resource, and the resource identified by the argument"""

    @abstractmethod
    async def aclose(self) -> None:
        """
        Clean up all resources associated to the root of this context.

        """


class HistoryInterface(ABC):
    @abstractmethod
    async def summary(self, nmax: int, /) -> Dict[int, DataSaved]:
        """Returns a summary of the history, with at most nmax entries."""

    @abstractmethod
    async def get(self, index: int, /) -> RawData:
        """Returns the data at the given index."""
        ...


class ConnectionInterface(ABC):
    @abstractmethod
    async def disconnect(self) -> None:
        """Stops the connection"""
        ...


@dataclass
class PublisherInterface(ABC):
    @abstractmethod
    async def publish(self, rd: RawData, /) -> None:
        """Publishes data to the resource"""
        ...

    @abstractmethod
    async def terminate(self) -> None:
        """Stops the publisher"""

    @abstractmethod
    async def get_listener_info(self) -> Optional[ListenerInfo]:
        """Returns information about the listener"""
        ...


class SubscriptionInterface(ABC):
    @abstractmethod
    async def unsubscribe(self) -> None:
        """Stops the subscription"""
        ...


#
# class DTPSErgoException(DTPSException):
#     ...
#
#
# class DTPSErgoNoDataAvailableYet(DTPSErgoException):
#     ...
#
#
# class DTPSErgoNotReachable(DTPSErgoException):
#     ...
#
#
# class DTPSErgoNotFound(DTPSErgoException):
#     ...
#
#
# class DTPSHistoryNotAvailable(DTPSErgoException):
#     ...
#
#
# class DTPSErgoPersistentTimeout(asyncio.TimeoutError, DTPSErgoException):
#     pass
