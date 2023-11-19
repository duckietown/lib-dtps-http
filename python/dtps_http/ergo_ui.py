import asyncio
from abc import ABC, abstractmethod
from typing import Callable, List, Optional, Sequence

from .structures import RawData, TopicRefAdd


async def dtps_init(base_name: str = "self") -> "DTPSContext":
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
        DTPS_BASE_SELF_1 = "unix+http://[socket]/"

    """
    _ = base_name.lower()
    ...


class DTPSContext(ABC):
    @abstractmethod
    def inside(self, *components: str) -> "DTPSContext":
        """gets a sub-resource"""
        ...

    @abstractmethod
    async def urls(self) -> List[str]:
        ...

    # todo: add

    @abstractmethod
    async def list(self) -> List[str]:
        """List subtopics"""
        ...

    # creation and deletion

    @abstractmethod
    async def queue_create(self, parameters: Optional[TopicRefAdd] = None, /) -> "DTPSContext":
        """Returns self"""
        ...

    @abstractmethod
    async def proxy_remove(self) -> None:
        ...

    # getting

    @abstractmethod
    # @allow_exceptions(DTPSErgoNoDataAvailableYet, DTPSErgoNotReachable, DTPSErgoNotFound)
    async def data_get(self) -> RawData:
        ...

    @abstractmethod
    # @allow_exceptions(DTPSErgoPersistentTimeout)
    async def subscribe(
        self,
        on_data: Callable,
        /,
        # service_level=None,
        # timeout: Optional[float] = None
    ) -> "SubscriptionInterface":
        """
        Same as subscribe, but the subscription is persistent: if the topic is not available, we wait until
        it is (up to a timeout).
        """
        ...

    # pushing

    @abstractmethod
    async def push(self, data: RawData) -> None:
        ...

    @abstractmethod
    async def call(self, data: RawData) -> RawData:
        """RPC call (push with response)"""
        ...

    # connection

    @abstractmethod
    async def connect_to(self, context: "DTPSContext") -> "ConnectionInterface":
        """Add a connection between topics with the given service leve."""
        ...

    # proxy

    @abstractmethod
    async def proxy_create(self, urls: Sequence[str]) -> "DTPSContext":
        """Creates this topic as a proxy to the given urls

        returns self
        """
        ...


class ConnectionInterface(ABC):
    @abstractmethod
    async def disconnect(self) -> None:
        """Stops the connection"""
        ...


class SubscriptionInterface(ABC):
    @abstractmethod
    async def desubscribe(self) -> None:
        """Stops the subscription"""
        ...


async def example1_process_data() -> None:
    # Environment
    # DTPS_BASE_SELF = "create:http://:8000/"

    me = await dtps_init("self")

    node_input = await me.inside("dtps/node/in").queue_create()
    node_output = await me.inside("dtps/node/out").queue_create()

    async def on_input(data: RawData) -> None:
        await node_output.push(data)

    subscription = node_input.subscribe(on_input)

    await asyncio.sleep(10)

    await subscription.desubscribe()


async def example2_process_data() -> None:
    # Environment example:
    #   DTPS_BASE_SOURCE = "http://:8000/dtps/node/out"
    #   DTPS_BASE_TARGET = "http://:8001/dtps/node/in"

    source = await dtps_init("source")
    target = await dtps_init("target")

    async def on_input(data: RawData) -> None:
        await target.push(data)

    subscription = await source.subscribe(on_input)

    await asyncio.sleep(10)

    await subscription.desubscribe()


async def example2_register() -> None:
    # Environment example:
    #   DTPS_BASE_SELF_0 = "create:http://:212/"
    #   DTPS_BASE_SELF_1 = "create:/tmp/sockets/nodename/"
    #   DTPS_BASE_SWITCHBOARD = "http://:8000/"
    me = await dtps_init("self")
    switchboard = await dtps_init("switchboard")

    await switchboard.inside("dtps/node/nodename").proxy_create(await me.urls())


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
