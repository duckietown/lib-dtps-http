import json
import time
from dataclasses import dataclass, dataclass as original_dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

import cbor2
import yaml
from aiopubsub import Hub, Key, Publisher, Subscriber

from . import logger
from .constants import (
    MIME_CBOR,
    MIME_JSON,
    MIME_TEXT,
    MIME_YAML,
)
from .structures import (
    ChannelInfo,
    ChannelInfoDesc,
    Clocks,
    DataReady,
    DataSaved,
    MinMax,
    RawData,
    ResourceAvailability,
    TopicRef,
)
from .types import ContentType, TopicNameV
from .urls import get_relative_url

__all__ = [
    "ObjectQueue",
    "ObjectTransformContext",
    "ObjectTransformFunction",
    "ObjectTransformResult",
    "TransformError",
]

SUB_ID = int
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


@dataclass
class SuccessPostResult:
    redirect_url: str


PostResult = Union[DataReady, TransformError]

# PublishResult = Union[DataSaved, TransformError]

ObjectTransformFunction = Callable[[ObjectTransformContext], Awaitable[ObjectTransformResult]]


async def transform_identity(otc: ObjectTransformContext) -> RawData:
    return otc.raw_data


class ObjectQueue:
    stored: List[int]
    saved: Dict[int, DataSaved]
    _data: Dict[str, RawData]
    _seq: int
    _name: TopicNameV
    _hub: Hub
    _pub: Publisher
    _sub: Subscriber
    tr: TopicRef
    max_history: Optional[int]
    transform: ObjectTransformFunction

    def __init__(
        self,
        hub: Hub,
        name: TopicNameV,
        tr: TopicRef,
        max_history: Optional[int],
        transform: ObjectTransformFunction = transform_identity,
    ):
        if max_history is not None and max_history < 1:
            msg = f"Invalid max_history: {max_history}"
            raise ValueError(msg)
        self._hub = hub
        self._pub = Publisher(self._hub, Key())
        self._sub = Subscriber(self._hub, name.as_relative_url())
        self._seq = 0
        self._data = {}
        self._name = name
        self.tr = tr
        self.max_history = max_history
        self.stored = []
        self.saved = {}
        self._transform = transform
        self.listeners = {}
        self.nlisteners = 0

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
        digest = obj.digest()
        clocks = self.current_clocks()
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
        self._data[digest] = obj
        self.stored.append(use_seq)
        self.saved[use_seq] = ds

        if self.max_history is not None:
            if len(self.stored) > self.max_history:
                x = self.stored.pop(0)
                ds_old = self.saved.pop(x, None)
                if ds_old is not None:
                    del self._data[ds_old.digest]

        self._pub.publish(
            Key(self._name.as_relative_url(), K_INDEX), use_seq
        )  # logger.debug(f"published #{self._seq} {self._name}: {obj!r}")

        reached_at = self._name.as_relative_url()
        data_ready = self.get_data_ready(ds, reached_at, False)
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
        return self.get(self.last().digest)

    def get(self, digest: str) -> RawData:
        return self._data[digest]

    def subscribe(self, callback: "Callable[[ObjectQueue, int], Awaitable[None]]") -> SUB_ID:
        listener_id = self.nlisteners
        self.nlisteners += 1

        wrap_callback = Wrapper(callback, self, listener_id)

        key = Key(self._name.as_relative_url(), K_INDEX)

        self._sub.add_async_listener(key, wrap_callback)
        # self._sub.remove_listener(key, wrap_callback)
        self.listeners[listener_id] = (key, wrap_callback)

        return listener_id

    async def aclose(self) -> None:
        for sub_id in list(self.listeners):
            await self.unsubscribe(sub_id)
        # await self._sub.remove_all_listeners()

    async def unsubscribe(self, sub_id: SUB_ID) -> None:
        if sub_id not in self.listeners:
            logger.warning(f"Subscription {sub_id} not found")
            return
        key, callback = self.listeners.pop(sub_id)
        try:
            await self._sub.remove_listener(key, callback)
        except Exception as e:
            logger.error(f"Could not unsubscribe {sub_id}: {e}")

    def get_data_ready(self, ds: DataSaved, presented_as: str, inline_data: bool) -> DataReady:
        actual_url = self._name.as_relative_url() + "data/" + ds.digest + "/"
        rel_url = get_relative_url(actual_url, presented_as)
        if inline_data:
            nchunks = 1
            availability_ = []
        else:
            nchunks = 0
            availability_ = [ResourceAvailability(url=rel_url, available_until=time.time() + 60)]

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
        self, f: "Callable[[ObjectQueue, int], Awaitable[None]]", oq: ObjectQueue, listener_id: SUB_ID
    ):
        self.f = f
        self.oq = oq
        self.listener_id = listener_id

    def __str__(self):
        return f"Wrapper({self.listener_id})"

    def __repr__(self):
        return f"Wrapper({self.listener_id})"

    async def __call__(self, _key: Key, msg: Any):
        return await self.f(self.oq, msg)
