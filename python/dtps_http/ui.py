import asyncio
from abc import ABC, abstractmethod
from functools import wraps
from typing import AsyncContextManager, AsyncIterator, List, Optional, Sequence, Type

from dtps_http import RawData, TopicNameV


def allow_exceptions(*allowed_exceptions: Type[Exception]):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except allowed_exceptions as e:
                # Re-raise the allowed exception
                raise e
            except Exception as e:
                # Handle disallowed exceptions
                msg = (
                    f"This function only allows the exceptions: {allowed_exceptions} but got "
                    f"{e.__class__.__name__}"
                )
                raise RuntimeError(msg) from e

        wrapper._allowed_exceptions = allowed_exceptions
        return wrapper

    return decorator


def reapply_decorators(cls):
    for b in cls.__bases__:
        for attr_name, attr_value in cls.__dict__.items():
            if callable(attr_value):
                base_method = getattr(b, attr_name, None)
                if base_method and hasattr(base_method, "_allowed_exceptions"):
                    allowed_exceptions = base_method._allowed_exceptions
                    setattr(cls, attr_name, allow_exceptions(*allowed_exceptions)(attr_value))
    return cls


class DTPSException(Exception):
    ...


class NoDataAvailableYet(DTPSException):
    ...


class NotReachable(DTPSException):
    ...


class NotFound(DTPSException):
    ...


# @dataclass
# class TopicList:
#


class PersistentTimeout(asyncio.TimeoutError, DTPSException):
    pass


class DTPSSession(ABC):
    @abstractmethod
    @allow_exceptions()
    async def topic_list(self, topic_name) -> List[str]:
        ...

    @abstractmethod
    @allow_exceptions()
    async def topic_create(self, topic_name, topic_parameters) -> List[str]:
        ...

    @abstractmethod
    @allow_exceptions()
    async def topic_create(self, topic_name, topic_parameters) -> List[str]:
        ...

    @abstractmethod
    @allow_exceptions()
    async def inside(self, *path: str) -> "DTPSContext":
        ...

    @abstractmethod
    @allow_exceptions(NoDataAvailableYet, NotReachable, NotFound)
    async def data_get(self) -> RawData:
        ...

    @abstractmethod
    @allow_exceptions(PersistentTimeout)
    async def data_get_persistent(self, timeout: Optional[float] = None) -> RawData:
        ...

    @abstractmethod
    @allow_exceptions(NotReachable, NotFound)
    async def data_push(self, data: RawData) -> None:
        ...

    @abstractmethod
    @allow_exceptions(NotReachable, NotFound)
    async def data_push_persistent(self, data: RawData, timeout: Optional[float] = None) -> None:
        ...

    @abstractmethod
    @allow_exceptions(...)
    async def history_get(self, data: RawData) -> None:
        ...

    @abstractmethod
    @allow_exceptions(NotReachable, NotFound)
    def subscribe(self, topic: TopicNameV, service_level) -> AsyncIterator[RawData]:
        """Subscribes to a topic with the given service level"""
        ...

    @abstractmethod
    @allow_exceptions(PersistentTimeout)
    def subscribe_persistent(
        self, topic: TopicNameV, service_level, timeout: Optional[float] = None
    ) -> AsyncIterator[RawData]:
        """Same as subscribe, but the subscription is persistent: if the topic is not available,
        we wait until it is (up to a timeout).
        """
        ...

    @abstractmethod
    @allow_exceptions(NotReachable, NotFound)
    async def push(self, topic: TopicNameV, raw_data: RawData) -> None:
        ...

    @abstractmethod
    @allow_exceptions(PersistentTimeout)
    async def push_persistent(
        self, topic: TopicNameV, raw_data: RawData, timeout: Optional[float] = None
    ) -> None:
        ...

    # meta things
    @abstractmethod
    @allow_exceptions()
    async def connection_add(self, topic1: TopicNameV, topic2: TopicNameV, service_level) -> None:
        """Add a connection between topics with the given service leve."""
        ...

    @abstractmethod
    @allow_exceptions()
    async def connection_remove(self, topic1: TopicNameV, topic2: TopicNameV, service_level) -> None:
        """Removes a connection between topics"""
        ...

    @abstractmethod
    @allow_exceptions()
    async def proxy_add(self, topic: TopicNameV, urls: Sequence[str]) -> None:
        ...

    @abstractmethod
    @allow_exceptions()
    async def proxy_remove(self, topic: TopicNameV, urls: Sequence[str]) -> None:
        ...


class DTPSContext(ABC):
    def session(self, name: str) -> "AsyncContextManager[DTPSSession]":
        ...


async def dtps_init() -> AsyncContextManager[DTPSContext]:
    ...


async def main() -> None:
    async with dtps_init() as context:
        pass
