from abc import ABC, abstractmethod
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
    DataSaved,
    NodeID,
    ObjectTransformResult,
    RawData,
    TopicRefAdd,
    URLString,
)

__all__ = [
    "ConnectionInterface",
    "DTPSContext",
    "HistoryInterface",
    "SubscriptionInterface",
    "PublisherInterface",
]

_ = Sequence
RPCFunction = Callable[[RawData], Awaitable[ObjectTransformResult]]


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
    async def remove(self) -> None:
        ...

    # getting

    @abstractmethod
    async def data_get(self) -> RawData:
        ...

    @abstractmethod
    async def subscribe(
        self,
        on_data: Callable[[RawData], Awaitable[None]],
        /,
        max_frequency: Optional[float] = None,
        # service_level=None,
        # timeout: Optional[float] = None
    ) -> "SubscriptionInterface":
        """
        The subscription is persistent: if the topic is not available, we wait until
        it is (up to a timeout).
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
    async def patch(self, patch_data: List[Dict[str, Any]], /) -> None:
        """
        Applies a patch to the resource.
        The patch is a list of operations, as defined in RFC 6902.

        """

    # proxy

    @abstractmethod
    async def expose(self, urls: "Sequence[str] | DTPSContext", /) -> None:
        """
        Creates this topic as a proxy to the given urls or to the context..

        returns self
        """

    @abstractmethod
    async def queue_create(
        self,
        *,
        parameters: Optional[TopicRefAdd] = None,
        transform: Optional[RPCFunction] = None,
    ) -> "DTPSContext":
        """
        Creates this resource (if it doesn't exist).
        Returns self.
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

    async def get(self, index: int, /) -> RawData:
        """Returns the data at the given index."""
        ...


class ConnectionInterface(ABC):
    @abstractmethod
    async def disconnect(self) -> None:
        """Stops the connection"""
        ...


class PublisherInterface(ABC):
    @abstractmethod
    async def publish(self, rd: RawData, /) -> None:
        """Publishes data to the resource"""
        ...

    @abstractmethod
    async def terminate(self) -> None:
        """Stops the publisher"""

    # TODO: DTSW-4880: add function to get number of connections
    # TODO: DTSW-4879: should we pass back the desired frequency? (max of all frequencies) or bandwidth constraints?
    # @abstractmethod
    # async def num_connections(self) -> RawData:
    #     """Publishes data to the resource and waits for the response"""
    #     ...


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
