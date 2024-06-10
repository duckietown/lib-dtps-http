import json
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, dataclass as original_dataclass
from typing import AsyncIterator, Awaitable, Callable, cast, Dict, Iterator, NewType, Optional, Union

import cbor2
import yaml
from aiopubsub import Hub, Key, Publisher, Subscriber  # type: ignore
from typing_extensions import Deque

from . import logger
from .blob_manager import BlobManager
from .constants import DEFAULT_DATA_AVAILABILITY_TIMEOUT, MIME_CBOR, MIME_JSON, MIME_TEXT, MIME_YAML
from .structures import (
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
from .types import ContentType, HTTPRequest, HTTPResponse, TopicNameV

__all__ = [
    "ObjectQueue",
    "ObjectServeContext",
    "ObjectServeFunction",
    "ObjectServeResult",
    "ObjectTransformContext",
    "ObjectTransformFunction",
    "ObjectTransformResult",
    "PostResult",
    "SUB_ID",
    "TransformError",
    "transform_identity",
]

SUB_ID = NewType("SUB_ID", int)
K_INDEX = "index"


@original_dataclass
class ObjectTransformContext:
    raw_data: RawData
    topic: TopicNameV
    queue: "ObjectQueue"


@original_dataclass
class TransformError:
    http_code: int
    message: str


ObjectTransformResult = Union[RawData, TransformError]
ObjectServeContext = HTTPRequest
ObjectServeResult = Union[RawData, HTTPResponse]


@dataclass
class SuccessPostResult:
    redirect_url: str


PostResult = Union[DataReady, TransformError]
GetResult = Union[DataReady, HTTPResponse]

# PublishResult = Union[DataSaved, TransformError]

ObjectTransformFunction = Callable[[ObjectTransformContext], Awaitable[ObjectTransformResult]]
ObjectServeFunction = Callable[[ObjectServeContext], Awaitable[ObjectServeResult]]


async def transform_identity(otc: ObjectTransformContext) -> RawData:
    return otc.raw_data


# tolerance for removal of blobs after they are not needed anymore
# TOLERANCE_REMOVAL = 0.0
from collections import deque


@dataclass
class ListenerData:
    key: Key
    wrapper: "Wrapper"
    max_frequency: Optional[float]


class ObjectQueue:
    stored: Deque[int]
    saved: Dict[int, DataSaved]
    # _data: Dict[str, RawData]
    _seq: int
    _name: TopicNameV
    _hub: Hub
    _pub: Publisher
    _sub: Subscriber
    tr: TopicRef
    bounds: Bounds
    transform: ObjectTransformFunction
    blob_manager: BlobManager
    serve: Optional[ObjectServeFunction]
    listeners: "Dict[SUB_ID,  ListenerData]"

    def __init__(
        self,
        hub: Hub,
        name: TopicNameV,
        tr: TopicRef,
        bounds: Bounds,
        blob_manager: BlobManager,
        transform: ObjectTransformFunction = transform_identity,
        serve: Optional[ObjectServeFunction] = None,
    ):
        self.bounds = bounds
        self._hub = hub
        self._pub = Publisher(self._hub, Key())
        self._sub = Subscriber(self._hub, name.as_relative_url())
        self._seq = 0
        # self._data = {}
        self._name = name
        self.tr = tr
        self.stored = deque()
        self.saved = {}
        self._transform = transform
        self.serve = serve
        self.listeners = {}
        self.nlisteners = 0
        self.blob_manager = blob_manager
        self.name_for_blob_manager = name.as_relative_url()

        self.request_counter = 0
        self.aclosing = False
        # self.subscribe_lock = asyncio.Lock()

    def get_channel_info(self) -> ChannelInfo:
        if not self.stored:
            newest = None
            oldest = None
        else:
            ds_oldest = self.saved[self.stored[0]]
            ds_newest = self.saved[self.stored[-1]]
            oldest = ChannelInfoDesc(sequence=ds_oldest.index, time_inserted=ds_oldest.time_inserted)
            newest = ChannelInfoDesc(sequence=ds_newest.index, time_inserted=ds_newest.time_inserted)

        ci = ChannelInfo(queue_created=self.tr.created, num_total=self._seq, newest=newest, oldest=oldest)
        return ci

    async def publish_text(self, text: str, content_type: ContentType = MIME_TEXT) -> PostResult:
        data = text.encode("utf-8")
        return await self.publish(RawData(content=data, content_type=content_type))

    async def publish_cbor(self, obj: object, content_type: ContentType = MIME_CBOR) -> PostResult:
        """Publish a python object as a cbor2 encoded object."""
        data = cbor2.dumps(obj)
        return await self.publish(RawData(content=data, content_type=content_type))

    async def publish_json(self, obj: object, content_type: ContentType = MIME_JSON) -> PostResult:
        """Publish a python object as a JSON encoded object."""
        data = json.dumps(obj)
        return await self.publish(RawData(content=data.encode(), content_type=content_type))

    async def publish_yaml(self, obj: object, content_type: ContentType = MIME_YAML) -> PostResult:
        """Publish a python object as a JSON encoded object."""
        data = yaml.dump(obj)
        return await self.publish(RawData(content=data.encode(), content_type=content_type))

    async def publish(self, obj0: RawData, /) -> PostResult:
        """
        Publish raw bytes.

        """

        try:
            obj = await self._transform(ObjectTransformContext(raw_data=obj0, topic=self._name, queue=self))
        except Exception as e:
            msg = f"Error while transforming {obj0}: {e}"
            return TransformError(500, msg)

        if isinstance(obj, TransformError):
            logger.error(f"Error while transforming {obj0}: {obj}")
            return obj

        use_seq = self._seq
        self._seq += 1
        # digest = obj.digest()
        clocks = self.current_clocks()
        digest = self.blob_manager.save_blob_for_queue(obj.content, (self.name_for_blob_manager, use_seq))
        ds = DataSaved(
            origin_node=self.tr.origin_node,
            unique_id=self.tr.unique_id,
            index=use_seq,
            time_inserted=time.time_ns(),
            digest=digest,
            content_type=obj.content_type,
            content_length=len(obj.content),
            clocks=clocks,
        )

        # self._data[digest] = obj
        self.stored.append(use_seq)
        self.saved[use_seq] = ds

        # logger.info(
        #    f'pushing, bounds = {self.bounds}  stored = {len(self.stored)}  saved = {len(self.saved)} '
        #    f'blobs={len(self.blob_manager.blobs)}')
        if self.bounds.max_size is not None:  # TODO: implement the semantics for others
            while len(self.stored) > self.bounds.max_size:
                x_old: int = self.stored.popleft()
                if x_old in self.saved:  # should always be true
                    ds_old = self.saved.pop(x_old)
                    # if TOLERANCE_REMOVAL is not None and TOLERANCE_REMOVAL > 0:
                    #     # extend deadline by an arbitrary 10 seconds
                    #     # (should not be needed, but just in case)
                    #     self.blob_manager.extend_deadline(ds_old.digest, TOLERANCE_REMOVAL)
                    self.blob_manager.release_blob(ds_old.digest, (self.name_for_blob_manager, x_old))

        inot = InsertNotification(ds, obj0)
        self._pub.publish(
            Key(self._name.as_relative_url(), K_INDEX), inot
        )  # logger.debug(f"published #{self._seq} {self._name}: {obj!r}")

        # reached_at = self._name.as_relative_url()
        data_ready = self.get_data_ready(ds, False)
        return data_ready

    def current_clocks(self) -> Clocks:
        clocks = Clocks.empty()
        if self._seq > 0:
            based_on = self._seq - 1
            clocks.logical[self.tr.unique_id] = MinMax(min=based_on, max=based_on)
        now = time.time_ns()
        clocks.wall[self.tr.unique_id] = MinMax(min=now, max=now)
        return clocks

    def last(self) -> DataSaved:
        if self.stored:
            last = self.stored[-1]
            return self.saved[last]
        else:
            raise KeyError("No data in queue")

    def last_data(self) -> RawData:
        last = self.last()
        data = self.blob_manager.get_blob(self.last().digest)
        return RawData(content=data, content_type=last.content_type)

    @asynccontextmanager
    async def subscribe_context(
        self,
        callback: "Callable[[ObjectQueue, InsertNotification], Awaitable[None]]",
        max_frequency: Optional[float] = None,
    ) -> AsyncIterator[None]:
        sub_id = self.subscribe(callback, max_frequency)
        try:
            yield
        finally:
            await self.unsubscribe(sub_id)

    def subscribe(
        self,
        callback: "Callable[[ObjectQueue, InsertNotification], Awaitable[None]]",
        max_frequency: Optional[float] = None,
    ) -> SUB_ID:
        listener_id = cast(SUB_ID, self.nlisteners)
        self.nlisteners += 1

        wrap_callback = Wrapper(callback, self, listener_id)

        key = Key(self._name.as_relative_url(), K_INDEX)

        self._sub.add_async_listener(key, wrap_callback)
        self.listeners[listener_id] = ListenerData(
            key=key, wrapper=wrap_callback, max_frequency=max_frequency
        )

        return listener_id

    def get_listener_info(self) -> ListenerInfo:
        nlisteners = len(self.listeners)
        if nlisteners == 0:
            max_frequency = None
        else:

            max_frequencies = [v.max_frequency for v in self.listeners.values()]
            if any(x is None for x in max_frequencies):
                max_frequency = None
            else:
                non_none = [x for x in max_frequencies if x is not None]
                max_frequency = max(non_none)

        return ListenerInfo(nlisteners, max_frequency)

    async def aclose(self) -> None:
        self.aclosing = True
        # async with self.subscribe_lock:

        while self.listeners:
            sub_id = list(self.listeners)[0]
            await self.unsubscribe(sub_id, error_if_not_exists=False)
        #
        # for sub_id in list(self.listeners):
        #     await self.unsubscribe(sub_id, error_if_not_exists=False)
        # await self._sub.remove_all_listeners()

    async def unsubscribe(self, sub_id: SUB_ID, error_if_not_exists: bool = True) -> None:
        if sub_id not in self.listeners:
            msg = f"Subscription {sub_id} not found (closing = {self.aclosing})"
            logger.warning(msg)
            return
        li = self.listeners.pop(sub_id)
        try:
            await self._sub.remove_listener(li.key, li.wrapper)
        except Exception as e:
            logger.error(f"Could not unsubscribe {sub_id}: {e}")

    def get_data_ready(self, ds: DataSaved, inline_data: bool) -> DataReady:
        available_interval = DEFAULT_DATA_AVAILABILITY_TIMEOUT
        available_until = time.time() + available_interval
        content = self.blob_manager.get_blob(ds.digest)
        actual_url = self.blob_manager.get_use_once_link_store(
            ds.digest, content, ds.content_type, available_interval
        )
        # available_until = self.blob_manager.extend_deadline(ds.digest, available_interval)

        # who = (self.name_for_blob_manager + '-request', self.request_counter)
        # self.request_counter += 1

        # actual_url = encode_url(digest=ds.digest, content_type=ds.content_type)
        # rel_url = get_relative_url(actual_url, presented_as)
        if inline_data:
            nchunks = 1
            availability_ = []
        else:
            nchunks = 0
            availability_ = [ResourceAvailability(url=actual_url, available_until=available_until)]

        data = DataReady(
            index=ds.index,
            time_inserted=ds.time_inserted,
            digest=ds.digest,
            content_type=ds.content_type,
            content_length=ds.content_length,
            availability=availability_,
            chunks_arriving=nchunks,
            clocks=ds.clocks,
            unique_id=self.tr.unique_id,
            origin_node=self.tr.origin_node,
        )
        return data


class Wrapper:
    def __init__(
        self,
        f: "Callable[[ObjectQueue, InsertNotification], Awaitable[None]]",
        oq: ObjectQueue,
        listener_id: SUB_ID,
    ):
        self.f = f
        self.oq = oq
        self.listener_id = listener_id

    def __str__(self) -> str:
        return f"Wrapper({self.listener_id})"

    def __repr__(self) -> str:
        return f"Wrapper({self.listener_id})"

    async def __call__(self, _key: Key, msg: InsertNotification) -> None:
        return await self.f(self.oq, msg)
