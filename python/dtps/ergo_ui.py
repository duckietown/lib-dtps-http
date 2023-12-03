from abc import ABC, abstractmethod
from typing import Awaitable, Callable, Dict, List, Optional

from dtps_http.structures import DataSaved, RawData, TopicRefAdd

__all__ = [
    "ConnectionInterface",
    "DTPSContext",
    "HistoryContext",
    "SubscriptionInterface",
]


class DTPSContext(ABC):
    @abstractmethod
    def navigate(self, *components: str) -> "DTPSContext":
        """gets a sub-resource"""
        ...

    def __truediv__(self, other: str) -> "DTPSContext":
        components = other.split("/")
        return self.navigate(*components)

    @abstractmethod
    async def list(self) -> List[str]:
        """List subtopics"""
        ...

    # TODO: - more information - dict[str, ...]

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
        # service_level=None,
        # timeout: Optional[float] = None
    ) -> "SubscriptionInterface":
        """
        The subscription is persistent: if the topic is not available, we wait until
        it is (up to a timeout).
        """
        ...

    @abstractmethod
    async def history(self) -> "Optional[HistoryContext]":
        """Returns None if history is not available."""
        ...

    # pushing

    @abstractmethod
    async def publish(self, data: RawData, /) -> None:
        ...

    @abstractmethod
    async def publisher(self) -> "PublisherInterface":
        ...

    @abstractmethod
    async def call(self, data: RawData, /) -> RawData:
        """RPC call (push with response)"""
        ...

    # proxy

    @abstractmethod
    async def expose(self, urls: "Sequence[str] | DTPSContext", /) -> "DTPSContext":
        """
        Creates this topic as a proxy to the given urls or to the context..

        returns self
        """
        ...

    @abstractmethod
    async def queue_create(self, parameters: Optional[TopicRefAdd] = None, /) -> "DTPSContext":
        """Returns self"""
        ...

    # connection

    @abstractmethod
    async def connect_to(self, context: "DTPSContext", /) -> "ConnectionInterface":
        """Add a connection between topics with the given service level."""
        ...

    @abstractmethod
    async def aclose(self) -> None:
        """Clean up all resources"""


class HistoryContext(ABC):
    @abstractmethod
    async def summary(self, nmax: int, /) -> Dict[int, DataSaved]:
        ...

    async def get(self, index: int, /) -> RawData:
        ...


class ConnectionInterface(ABC):
    @abstractmethod
    async def disconnect(self) -> None:
        """Stops the connection"""
        ...


class PublisherInterface(ABC):
    @abstractmethod
    async def publish(self, rd: RawData, /) -> None:
        pass
        # await self.publish(rd)

    @abstractmethod
    async def terminate(self) -> None:
        """Stops the publisher"""
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
