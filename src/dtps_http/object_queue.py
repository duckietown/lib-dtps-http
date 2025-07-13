"""Object queue."""

__all__ = [
    "SUB_ID",
    "ObjectQueue",
    "ObjectServeContext",
    "ObjectServeFunction",
    "ObjectServeResult",
    "ObjectTransformContext",
    "ObjectTransformFunction",
    "ObjectTransformResult",
    "PostResult",
    "TransformError",
    "transform_identity",
]
import json
import time
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import NewType, cast

import cbor2
import yaml
from aiopubsub import Hub, Key, Publisher, Subscriber

from dtps_http import logger
from dtps_http.blob_manager import BlobManager
from dtps_http.constants import (
    DEFAULT_DATA_AVAILABILITY_TIMEOUT,
    MIME_CBOR,
    MIME_JSON,
    MIME_TEXT,
    MIME_YAML,
)
from dtps_http.structures import (
    Bounds,
    ChannelInfo,
    ChannelInfoDesc,
    Clocks,
    DataReady,
    DataSaved,
    InsertNotification,
    ListenerInfo,
    MinMax,
    RawData,
    ResourceAvailability,
    TopicRef,
)
from dtps_http.types_ import ContentType, HTTPRequest, HTTPResponse, TopicNameV

K_INDEX = "index"

SUB_ID = NewType("SUB_ID", int)


@dataclass
class ObjectTransformContext:
    """Object transform context."""

    raw_data: RawData
    topic: TopicNameV
    queue: "ObjectQueue"


@dataclass
class SuccessPostResult:
    redirect_url: str


@dataclass
class TransformError:
    """Transform error."""

    http_code: int
    message: str


ObjectTransformResult = RawData | TransformError
ObjectServeContext = HTTPRequest
ObjectServeResult = RawData | HTTPResponse


PostResult = DataReady | TransformError
GetResult = DataReady | HTTPResponse

ObjectTransformFunction = Callable[
    [ObjectTransformContext],
    Awaitable[ObjectTransformResult],
]
ObjectServeFunction = Callable[
    [ObjectServeContext],
    Awaitable[ObjectServeResult],
]


async def transform_identity(otc: ObjectTransformContext) -> RawData:
    """Return object transform context raw data."""
    return otc.raw_data


@dataclass
class ListenerData:
    key: Key
    wrapper: "Wrapper"
    max_frequency: float | None


class ObjectQueue:
    """Object queue."""

    stored: deque[int]
    saved: dict[int, DataSaved]
    _seq: int
    _topic_name: TopicNameV
    _hub: Hub
    _pub: Publisher
    _sub: Subscriber
    topic_reference: TopicRef
    bounds: Bounds
    transform: ObjectTransformFunction
    blob_manager: BlobManager
    serve: ObjectServeFunction | None
    listeners: "dict[SUB_ID,  ListenerData]"

    def __init__(
        self,
        hub: Hub,
        topic_name: TopicNameV,
        topic_reference: TopicRef,
        bounds: Bounds,
        blob_manager: BlobManager,
        transform: ObjectTransformFunction = transform_identity,
        serve: ObjectServeFunction | None = None,
    ) -> None:
        """Initialize object queue."""
        self.bounds = bounds
        self._hub = hub
        key = Key()
        self._pub = Publisher(self._hub, key)
        relative_url = topic_name.as_relative_url()
        self._sub = Subscriber(self._hub, relative_url)
        self._seq = 0
        self._topic_name = topic_name
        self.topic_reference = topic_reference
        self.stored = deque()
        self.saved = {}
        self._transform = transform
        self.serve = serve
        self.listeners = {}
        self.number_of_listeners = 0
        self.blob_manager = blob_manager
        self.name_for_blob_manager = topic_name.as_relative_url()
        self.request_counter = 0
        self.aclosing = False

    def get_channel_info(self) -> ChannelInfo:
        """Return channel information."""
        if not self.stored:
            newest = None
            oldest = None
        else:
            ds_oldest = self.saved[self.stored[0]]
            ds_newest = self.saved[self.stored[-1]]
            oldest = ChannelInfoDesc(
                sequence=ds_oldest.index,
                time_inserted=ds_oldest.time_inserted,
            )
            newest = ChannelInfoDesc(
                sequence=ds_newest.index,
                time_inserted=ds_newest.time_inserted,
            )
        return ChannelInfo(
            queue_created=self.topic_reference.created,
            num_total=self._seq,
            newest=newest,
            oldest=oldest,
        )

    async def publish_text(
        self,
        text: str,
        content_type: ContentType = MIME_TEXT,
    ) -> PostResult:
        """Publish text and return result."""
        data = text.encode("utf-8")
        raw_data = RawData(data, content_type)
        return await self.publish(raw_data)

    async def publish_cbor(
        self,
        object_: object,
        content_type: ContentType = MIME_CBOR,
    ) -> PostResult:
        """Publish a python object as a cbor2 encoded object."""
        data = cbor2.dumps(object_)
        raw_data = RawData(data, content_type)
        return await self.publish(raw_data)

    async def publish_json(
        self,
        object_: object,
        content_type: ContentType = MIME_JSON,
    ) -> PostResult:
        """Publish a python object as a JSON encoded object."""
        data = json.dumps(object_)
        encoded_data = data.encode()
        raw_data = RawData(encoded_data, content_type)
        return await self.publish(raw_data)

    async def publish_yaml(
        self,
        object_: object,
        content_type: ContentType = MIME_YAML,
    ) -> PostResult:
        """Publish a python object as a JSON encoded object."""
        data = yaml.dump(object_)
        encoded_data = data.encode()
        raw_data = RawData(encoded_data, content_type)
        return await self.publish(raw_data)

    async def publish(self, obj0: RawData, /) -> PostResult:
        """Publish raw bytes."""
        try:
            object_transform_context = ObjectTransformContext(
                obj0,
                self._topic_name,
                self,
            )
            object_ = await self._transform(object_transform_context)
        except Exception as exception:
            message = f"Error while transforming {obj0}: {exception}"
            return TransformError(500, message)
        if isinstance(object_, TransformError):
            logger.exception(f"Error while transforming {obj0}: {object_}")
            return object_
        use_seq = self._seq
        self._seq += 1
        clocks = self.current_clocks()
        digest = self.blob_manager.save_blob_for_queue(
            object_.content,
            (self.name_for_blob_manager, use_seq),
        )
        time_inserted = time.time_ns()
        content_length = len(object_.content)
        data_saved = DataSaved(
            origin_node=self.topic_reference.origin_node,
            unique_id=self.topic_reference.unique_id,
            index=use_seq,
            time_inserted=time_inserted,
            digest=digest,
            content_type=object_.content_type,
            content_length=content_length,
            clocks=clocks,
        )
        self.stored.append(use_seq)
        self.saved[use_seq] = data_saved
        # TODO: Implement the semantics for others
        if self.bounds.max_size is not None:
            while len(self.stored) > self.bounds.max_size:
                x_old: int = self.stored.popleft()
                if x_old in self.saved:
                    ds_old = self.saved.pop(x_old)
                    self.blob_manager.release_blob(
                        ds_old.digest,
                        (self.name_for_blob_manager, x_old),
                    )
        inot = InsertNotification(data_saved, obj0)
        relative_url = self._topic_name.as_relative_url()
        key = Key(relative_url, K_INDEX)
        self._pub.publish(key, inot)
        return self.get_data_ready(
            data_saved,
            object_.content,
            inline_data=False,
        )

    def current_clocks(self) -> Clocks:
        """Return clocks."""
        clocks = Clocks.empty()
        if self._seq > 0:
            based_on = self._seq - 1
            clocks.logical[self.topic_reference.unique_id] = MinMax(
                min=based_on,
                max=based_on,
            )
        current_time = time.time_ns()
        clocks.wall[self.topic_reference.unique_id] = MinMax(
            min=current_time,
            max=current_time,
        )
        return clocks

    def last(self) -> DataSaved:
        """Return last data saved."""
        if self.stored:
            last = self.stored[-1]
            return self.saved[last]
        message = "No data in queue."
        raise KeyError(message)

    def last_data(self) -> RawData:
        """Return last raw data."""
        last = self.last()
        digest = last.digest
        data = self.blob_manager.get_blob(digest)
        return RawData(data, last.content_type)

    @asynccontextmanager
    async def subscribe_context(
        self,
        callback: Callable[
            ["ObjectQueue", InsertNotification],
            Awaitable[None],
        ],
        *,
        max_frequency: float | None = None,
    ) -> AsyncIterator[None]:
        """Subscribe context."""
        sub_id = self.subscribe(callback, max_frequency=max_frequency)
        try:
            yield
        finally:
            await self.unsubscribe(sub_id)

    def subscribe(
        self,
        callback: Callable[
            ["ObjectQueue", InsertNotification],
            Awaitable[None],
        ],
        *,
        max_frequency: float | None = None,
    ) -> SUB_ID:
        """Subscribe."""
        listener_id = cast(SUB_ID, self.number_of_listeners)
        self.number_of_listeners += 1
        wrap_callback = Wrapper(callback, self, listener_id)
        relative_url = self._topic_name.as_relative_url()
        key = Key(relative_url, K_INDEX)
        self._sub.add_async_listener(key, wrap_callback)
        self.listeners[listener_id] = ListenerData(
            key,
            wrap_callback,
            max_frequency,
        )
        return listener_id

    def get_listener_info(self) -> ListenerInfo:
        """Return listener information."""
        number_of_listeners = len(self.listeners)
        if number_of_listeners == 0:
            max_frequency = None
        else:
            listener_data = self.listeners.values()
            max_frequencies = [
                listener_datum.max_frequency
                for listener_datum in listener_data
            ]
            if any(max_frequency is None for max_frequency in max_frequencies):
                max_frequency = None
            else:
                non_none = [
                    max_frequency
                    for max_frequency in max_frequencies
                    if max_frequency is not None
                ]
                max_frequency = max(non_none)
        return ListenerInfo(number_of_listeners, max_frequency)

    async def aclose(self) -> None:
        """Close asynchronously."""
        self.aclosing = True
        while self.listeners:
            listener_list = list(self.listeners)
            sub_id = listener_list[0]
            await self.unsubscribe(sub_id, error_if_not_exists=False)

    async def unsubscribe(
        self,
        sub_id: SUB_ID,
        *,
        error_if_not_exists: bool = True,
    ) -> None:
        """Unsubscribe."""
        if sub_id not in self.listeners:
            logger.warning(
                "Subscription %s not found (closing = %s)",
                sub_id,
                self.aclosing,
            )
            return
        listener = self.listeners.pop(sub_id)
        try:
            await self._sub.remove_listener(listener.key, listener.wrapper)
        except Exception as exception:
            logger.exception("Could not unsubscribe %s: %s", sub_id, exception)

    def get_data_ready(
        self,
        data_saved: DataSaved,
        content: bytes,
        *,
        inline_data: bool,
    ) -> DataReady:
        """Return data ready."""
        available_interval = DEFAULT_DATA_AVAILABILITY_TIMEOUT
        available_until = time.time() + available_interval
        actual_url = self.blob_manager.get_use_once_link_store(
            data_saved.digest,
            content,
            data_saved.content_type,
            available_interval,
        )
        if inline_data:
            chunks_arriving = 1
            availability_ = []
        else:
            chunks_arriving = 0
            availability_ = [
                ResourceAvailability(actual_url, available_until),
            ]
        return DataReady(
            index=data_saved.index,
            time_inserted=data_saved.time_inserted,
            digest=data_saved.digest,
            content_type=data_saved.content_type,
            content_length=data_saved.content_length,
            availability=availability_,
            chunks_arriving=chunks_arriving,
            clocks=data_saved.clocks,
            unique_id=self.topic_reference.unique_id,
            origin_node=self.topic_reference.origin_node,
        )


class Wrapper:
    """Wrapper."""

    function: Callable
    listener_id: SUB_ID
    queue: ObjectQueue

    def __init__(
        self,
        function: Callable[[ObjectQueue, InsertNotification], Awaitable[None]],
        queue: ObjectQueue,
        listener_id: SUB_ID,
    ) -> None:
        """Initialize wrapper."""
        self.function = function
        self.queue = queue
        self.listener_id = listener_id

    def __str__(self) -> str:
        return f"Wrapper({self.listener_id})"

    def __repr__(self) -> str:
        return f"Wrapper({self.listener_id})"

    async def __call__(self, _: Key, message: InsertNotification) -> None:
        return await self.function(self.queue, message)
