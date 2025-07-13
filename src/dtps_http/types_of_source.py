"""Types of source."""

__all__ = [
    "AbstractSource",
    "ForwardedQueue",
    "Native",
    "NotAvailableYet",
    "NotFound",
    "OurQueue",
    "SourceComposition",
    "Transformed",
]

import copy
import time
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import asdict, dataclass, replace
from typing import (
    Any,
    cast,
)

import cbor2
import jsonpatch
import jsonpointer
from aiohttp import ClientResponse
from aiohttp.web import HTTPBadRequest
from aiohttp.web_response import Response
from jsonpatch import JsonPatch

from dtps_http import logger, my_raise_for_status
from dtps_http.client import DTPSClient
from dtps_http.constants import (
    CONTENT_TYPE_DTPS_INDEX_CBOR,
    CONTENT_TYPE_PATCH_CBOR,
    DEFAULT_DATA_AVAILABILITY_TIMEOUT,
)
from dtps_http.object_queue import PostResult, TransformError
from dtps_http.server import DTPSServer
from dtps_http.structures import (
    Bounds,
    ContentInfo,
    DataReady,
    DataSaved,
    LinkBenchmark,
    RawData,
    ResourceAvailability,
    TopicProperties,
    TopicReachability,
    TopicRef,
    TopicsIndex,
)
from dtps_http.types_ import (
    ContentType,
    HTTPRequest,
    NodeID,
    SourceID,
    TopicNameV,
)
from dtps_http.urls import URL, get_relative_url, join, parse_url_unescape
from dtps_http.utils import pydantic_parse


class AbstractSource(ABC):
    """Abstract source."""

    @abstractmethod
    async def call(
        self,
        presented_as: str,
        server: DTPSServer,
        raw_data: RawData,
    ) -> RawData | TransformError:
        """Call."""

    @abstractmethod
    async def delete(
        self,
        presented_as: str,
        server: DTPSServer,
    ) -> "TransformError | None":
        """Delete."""

    @abstractmethod
    def get_inside(self, s: str, /) -> "AbstractSource":
        """Return abstract source.

        AbstractSource / "a" / "b"
        """

    @abstractmethod
    def get_inside_after(self, s: str) -> "AbstractSource":
        """Return abstract source after."""

    @abstractmethod
    async def get_meta_info(
        self,
        presented_as: str,
        server: DTPSServer,
    ) -> TopicsIndex:
        """Return meta info."""
        message = f"AbstractSource.get_meta_info() for {self}."
        raise NotImplementedError(message)

    @abstractmethod
    def get_properties(self, server: DTPSServer) -> TopicProperties:
        """Return properties."""
        message = f"AbstractSource.get_properties() for {self}."
        raise NotImplementedError(message)

    @abstractmethod
    async def get_resolved_data(
        self,
        presented_as: str,
        server: DTPSServer,
        request: HTTPRequest | None,
    ) -> "ResolvedData":
        """Return resolved data."""

    @abstractmethod
    async def get_source_node_id(
        self,
        server: DTPSServer,
    ) -> NodeID | None:
        """Return source node identification."""

    @abstractmethod
    async def patch(
        self,
        presented_as: str,
        server: DTPSServer,
        patch: JsonPatch,
    ) -> PostResult:
        """Patch."""

    @abstractmethod
    async def publish(
        self,
        presented_as: str,
        server: DTPSServer,
        raw_data: RawData,
    ) -> PostResult:
        """Publish."""

    def resolve_extra(
        self,
        components: tuple[str, ...],
        extra: str | None,
    ) -> "AbstractSource":
        """Return resolve extra."""
        if not components:
            if extra is None:
                return self
            return self.get_inside_after(extra)
        first, *rest = components
        abstract_source = self.get_inside(first)
        components = tuple(rest)
        return abstract_source.resolve_extra(components, extra)


class AbstractTransform(ABC):
    """Abstract transform."""

    def get_transform_inside(self, s: str) -> "AbstractTransform":
        message = f"AbstractTransform.get_transform_inside() for {self}"
        raise NotImplementedError(message)

    @abstractmethod
    def transform(self, data: "ResolvedData") -> "ResolvedData": ...


@dataclass
class ForwardedQueue(AbstractSource):
    """Forwarded queue."""

    topic_name: TopicNameV

    async def get_source_node_id(self, server: DTPSServer) -> NodeID | None:
        """Return source node identification."""
        return server.forwarded[self.topic_name].origin_node

    async def get_meta_info(
        self,
        _: str,
        __: DTPSServer,
    ) -> TopicsIndex:
        """Return meta information."""
        message = f"get_meta_info() for {self}."
        raise NotImplementedError(message)

    def get_inside_after(self, s: str) -> AbstractSource:
        """Return source after."""
        message = f"get_inside_after({s!r}) not implemented for {self!r}."
        raise KeyError(message)

    def get_inside(self, s: str, /) -> AbstractSource:
        """Return source."""
        message = f"get_inside({s!r}) not implemented for {self!r}."
        raise KeyError(message)

    def get_properties(self, server: DTPSServer) -> TopicProperties:
        """Return properties."""
        forwarded_topic = server.forwarded[self.topic_name]
        return forwarded_topic.properties

    async def get_resolved_data(
        self,
        _: str,
        server: DTPSServer,
        __: HTTPRequest | None,
    ) -> "ResolvedData":
        """Return resolved data."""
        url_data = server.forwarded[self.topic_name].forward_url_data
        async with (
            server.client() as dtps_client,
            dtps_client.my_session(url_data) as (session2, use_url2),
            session2.get(use_url2) as resp_data,
        ):
            await my_raise_for_status(resp_data, url_data)
            content = await resp_data.read()
            content_type = ContentType(resp_data.content_type)
            return RawData(content, content_type)

    async def patch(
        self,
        _: str,
        server: DTPSServer,
        patch: JsonPatch,
    ) -> PostResult:
        """Patch."""
        url_post = server.forwarded[self.topic_name].forward_url_data
        async with server.client() as dtps_client:
            data = cbor2.dumps(patch.patch)
            async with dtps_client.my_session(url_post) as (
                session2,
                use_url2,
            ):
                headers = {
                    "content-type": CONTENT_TYPE_PATCH_CBOR,
                }
                async with session2.patch(
                    use_url2,
                    data=data,
                    headers=headers,
                ) as resp_data:
                    if not resp_data.ok:
                        content = await resp_data.read()
                        message = str(content)
                        return TransformError(resp_data.status, message)
                    return await load_datasaved_resp(
                        server,
                        url_post,
                        dtps_client,
                        resp_data,
                    )

    async def delete(
        self,
        _: str,
        server: DTPSServer,
    ) -> "TransformError | None":
        """Delete."""
        url_post = server.forwarded[self.topic_name].forward_url_data
        async with (
            server.client() as dtps_client,
            dtps_client.my_session(url_post) as (session2, use_url2),
            session2.delete(use_url2) as resp_data,
        ):
            if not resp_data.ok:
                content = await resp_data.read()
                message = str(content)
                return TransformError(resp_data.status, message)
        return None

    async def publish(
        self,
        _: str,
        server: DTPSServer,
        raw_data: RawData,
    ) -> PostResult:
        """Publish."""
        url_post = server.forwarded[self.topic_name].forward_url_data
        async with (
            server.client() as dtps_client,
            dtps_client.my_session(url_post) as (session2, use_url2),
        ):
            headers = {
                "content-type": raw_data.content_type,
            }
            async with session2.post(
                use_url2,
                data=raw_data.content,
                headers=headers,
            ) as resp_data:
                if not resp_data.ok:
                    content = await resp_data.read()
                    message = str(content)
                    return TransformError(resp_data.status, message)
                return await load_datasaved_resp(
                    server,
                    url_post,
                    dtps_client,
                    resp_data,
                )

    async def call(
        self,
        _: str,
        server: DTPSServer,
        raw_data: RawData,
    ) -> RawData | TransformError:
        """Call."""
        url_post = server.forwarded[self.topic_name].forward_url_data
        async with (
            server.client() as dtps_client,
            dtps_client.my_session(url_post) as (session2, use_url2),
        ):
            headers = {
                "content-type": raw_data.content_type,
            }
            async with session2.post(
                use_url2,
                data=raw_data.content,
                headers=headers,
            ) as resp_data:
                if not resp_data.ok:
                    content = await resp_data.read()
                    message = str(content)
                    return TransformError(resp_data.status, message)
                data_ready = await load_datasaved_resp(
                    server,
                    url_post,
                    dtps_client,
                    resp_data,
                )
                url = parse_url_unescape(data_ready.availability[0].url)
                return await dtps_client.get(url, data_ready.content_type)


@dataclass
class GetInside(AbstractTransform):
    components: tuple[str, ...]

    def __post_init__(self) -> None:
        if ":meta" in self.components:
            message = "Should have resolved :meta"
            raise ValueError(message)

    def apply(self, object_: object) -> object:
        return get_inside(object_, (), object_, self.components)

    def get_transform_inside(self, string: str) -> "AbstractTransform":
        return GetInside((*self.components, string))

    def transform(self, data: "ResolvedData") -> "ResolvedData":
        if isinstance(data, RawData):
            object_ = data.get_as_native_object()
            object_ = self.apply(object_)
            return Native(object_)
        if isinstance(data, Native):
            object_ = self.apply(object_)
            return Native(object_)
        if isinstance(data, NotAvailableYet):
            return data
        if not isinstance(data, NotFound):
            raise TypeError
        return data


@dataclass
class MetaInfo(AbstractSource):
    source: AbstractSource

    async def delete(
        self,
        _: str,
        __: DTPSServer,
    ) -> "TransformError | None":
        return TransformError(400, "Cannot delete MetaInfo.")

    async def get_source_node_id(self, server: DTPSServer) -> NodeID | None:
        return await self.source.get_source_node_id(server)

    async def get_meta_info(
        self,
        _: str,
        __: DTPSServer,
    ) -> TopicsIndex:
        message = "OurQueue.get_meta_info() for a meta info."
        raise KeyError(message)

    def get_properties(self, _: DTPSServer) -> TopicProperties:
        return TopicProperties.readonly()

    def get_inside_after(self, s: str) -> AbstractSource:
        message = f"get_inside_after({s!r}) not implemented for {self!r}."
        raise KeyError(message)

    def get_inside(self, s: str, /) -> AbstractSource:
        message = f"get_inside({s!r}) not implemented for {self!r}."
        raise KeyError(message)

    async def get_resolved_data(
        self,
        presented_as: str,
        server: DTPSServer,
        _: HTTPRequest | None,
    ) -> "ResolvedData":
        meta_info = await self.source.get_meta_info(presented_as, server)
        wire = meta_info.to_wire()
        wire_dictionary = asdict(wire)
        return RawData.cbor_from_native_object(wire_dictionary)

    async def patch(
        self,
        _: str,
        __: DTPSServer,
        ___: JsonPatch,
    ) -> PostResult:
        return TransformError(400, "Cannot PATCH MetaInfo.")

    async def publish(
        self,
        _: str,
        __: DTPSServer,
        ___: RawData,
    ) -> PostResult:
        return TransformError(400, "Cannot POST MetaInfo.")

    async def call(
        self,
        _: str,
        __: DTPSServer,
        ___: RawData,
    ) -> RawData | TransformError:
        return TransformError(400, "Cannot call MetaInfo.")


@dataclass
class Native:
    """Native."""

    object_: object


@dataclass
class NotAvailableYet:
    """Not available yet."""

    comment: str


@dataclass
class NotFound:
    """Not found."""

    comment: str


@dataclass
class OurQueue(AbstractSource):
    """Our queue."""

    topic_name: TopicNameV

    async def get_source_node_id(self, server: DTPSServer) -> NodeID | None:
        """Return source node identification."""
        return server.node_id

    async def get_meta_info(
        self,
        presented_as: str,
        server: DTPSServer,
    ) -> TopicsIndex:
        """Return meta information."""
        queue = server.get_object_queue(self.topic_name)
        topic_reference = queue.topic_reference
        url_supposed = self.topic_name.as_relative_url()
        url_relative = get_relative_url(url_supposed, presented_as)
        benchmark = LinkBenchmark.identity()
        reachability = TopicReachability(
            url=url_relative,
            answering=server.node_id,
            forwarders=[],
            benchmark=benchmark,
        )
        topic_reference = replace(topic_reference, reachability=[reachability])
        root = TopicNameV.root()
        topics = {
            root: topic_reference,
        }
        return TopicsIndex(topics)

    def get_properties(self, server: DTPSServer) -> TopicProperties:
        """Return properties."""
        queue = server.get_object_queue(self.topic_name)
        return queue.topic_reference.properties

    def get_inside_after(self, s: str) -> AbstractSource:
        """Return source after."""
        message = f"get_inside_after({s!r}) not implemented for {self!r}."
        raise KeyError(message)

    def get_inside(self, s: str, /) -> AbstractSource:
        """Return source."""
        if s == ":meta":
            return MetaInfo(self)
        transform = GetInside((s,))
        return Transformed(self, transform)

    async def get_resolved_data(
        self,
        _: str,
        server: DTPSServer,
        request: HTTPRequest | None,
    ) -> "ResolvedData":
        """Return resolved data."""
        queue = server.get_object_queue(self.topic_name)
        if queue.serve is not None and request is not None:
            return await queue.serve(request)
        if not queue.stored:
            dash_separated_topic_name = self.topic_name.as_dash_sep()
            message = f"No data yet for {dash_separated_topic_name}."
            return NotAvailableYet(message)
        return queue.last_data()

    async def publish(
        self,
        _: str,
        server: DTPSServer,
        raw_data: RawData,
    ) -> PostResult:
        """Publish."""
        queue = server.get_object_queue(self.topic_name)
        return await queue.publish(raw_data)

    async def call(
        self,
        _: str,
        server: DTPSServer,
        raw_data: RawData,
    ) -> RawData | TransformError:
        """Call."""
        queue = server.get_object_queue(self.topic_name)
        otr = await queue.publish(raw_data)
        if isinstance(otr, TransformError):
            return otr
        return queue.last_data()

    async def patch(
        self,
        _: str,
        server: DTPSServer,
        patch: JsonPatch,
    ) -> PostResult:
        """Patch."""
        queue = server.get_object_queue(self.topic_name)
        topic_reference = queue.topic_reference.properties
        if not topic_reference.patchable:
            dash_separated_topic_name = self.topic_name.as_dash_sep()
            message = f"Cannot patch {dash_separated_topic_name}."
            logger.exception(message)
            raise HTTPBadRequest(reason=message)
        last_data = queue.last_data()
        object_ = last_data.get_as_native_object()
        try:
            obj2 = patch.apply(object_)
        except (
            jsonpatch.JsonPatchException,
            jsonpointer.JsonPointerException,
        ) as error:
            message = f"Cannot apply patch {patch} to {object_}"
            logger.exception(message + f": {error}")
            raise HTTPBadRequest(reason=message) from error
        if not isinstance(obj2, dict):
            message = (
                f"Patch {patch} applied to {object_} gives {obj2} which is not"
                " a dictionary."
            )
            logger.exception(message)
            raise HTTPBadRequest(reason=message)
        obj2 = cast(dict[str, Any], obj2)
        raw_data = RawData.json_from_native_object(obj2)
        return await queue.publish(raw_data)

    async def delete(
        self,
        _: str,
        server: DTPSServer,
    ) -> "TransformError | None":
        """Delete."""
        queue = server.get_object_queue(self.topic_name)
        topic_reference = queue.topic_reference.properties
        if not topic_reference.droppable:
            dash_separated_topic_name = self.topic_name.as_dash_sep()
            message = f"Cannot patch {dash_separated_topic_name}."
            logger.exception(message)
            return TransformError(401, message)
        await server.remove_object_queue(self.topic_name)
        return None


@dataclass
class SourceComposition(AbstractSource):
    """Source composition."""

    topic_name: TopicNameV
    sources: dict[TopicNameV, AbstractSource]
    unique_id: SourceID
    origin_node: NodeID

    async def get_source_node_id(self, _: DTPSServer) -> NodeID | None:
        """Return source node identification."""
        return self.origin_node

    async def get_meta_info(
        self,
        presented_as: str,
        server: DTPSServer,
    ) -> TopicsIndex:
        """Return meta information."""
        topics: dict[TopicNameV, TopicRef] = {}
        for prefix, source in self.sources.items():
            meta_info = await source.get_meta_info(presented_as, server)
            for topic_name, topic in meta_info.topics.items():
                topics[prefix + topic_name] = topic
        supposed = self.topic_name.as_relative_url()
        url_relative = get_relative_url(supposed, presented_as)
        benchmark = LinkBenchmark.identity()
        reachability = TopicReachability(
            url_relative,
            server.node_id,
            [],
            benchmark,
        )
        content_info = ContentInfo.simple(CONTENT_TYPE_DTPS_INDEX_CBOR)
        root = TopicNameV.root()
        # TODO: Warn?
        bounds = Bounds.unbounded()
        properties = self.get_properties(server)
        topics[root] = TopicRef(
            self.unique_id,
            self.origin_node,
            {},
            [reachability],
            0,
            properties,
            content_info,
            bounds,
        )
        return TopicsIndex(topics)

    async def delete(self, _: str, __: DTPSServer) -> None:
        """Delete."""
        message = f"delete() for {self}."
        raise NotImplementedError(message)

    def get_properties(self, server: DTPSServer) -> TopicProperties:
        """Return properties."""
        streamable = False
        pushable = False
        readable = True
        immutable = True
        for source in self.sources.values():
            properties = source.get_properties(server)
            streamable = streamable or properties.streamable
            readable = readable and properties.readable
            immutable = immutable and properties.immutable
        return TopicProperties(
            streamable,
            pushable,
            readable,
            immutable,
            has_history=False,
            patchable=False,
            droppable=False,
        )

    def get_inside_after(self, s: str) -> AbstractSource:
        """Return source after."""
        message = f"get_inside_after({s!r}) not implemented for {self!r}."
        raise KeyError(message)

    def get_inside(self, s: str, /) -> AbstractSource:
        """Return source."""
        message = f"get_inside({s!r}) not implemented for {self!r}."
        raise KeyError(message)

    async def get_resolved_data(
        self,
        presented_as: str,
        server: DTPSServer,
        _: HTTPRequest | None,
    ) -> "ResolvedData":
        """Return resolved data."""
        data = await self.get_meta_info(presented_as, server)
        wire = data.to_wire()
        wire_dictionary = asdict(wire)
        as_cbor = cbor2.dumps(wire_dictionary)
        return RawData(as_cbor, CONTENT_TYPE_DTPS_INDEX_CBOR)

    async def patch(
        self,
        _: str,
        __: DTPSServer,
        ___: JsonPatch,
    ) -> PostResult:
        """Patch."""
        # TODO: This can be done in principle
        return TransformError(400, "Cannot patch SourceComposition.")

    async def publish(
        self,
        _: str,
        __: DTPSServer,
        ___: RawData,
    ) -> PostResult:
        """Publish."""
        return TransformError(400, "Cannot post to SourceComposition.")

    async def call(
        self,
        _: str,
        __: DTPSServer,
        ___: RawData,
    ) -> RawData | TransformError:
        """Call."""
        return TransformError(400, "Cannot call SourceComposition.")


@dataclass
class Transformed(AbstractSource):
    """Transformed."""

    source: AbstractSource
    transform: AbstractTransform

    async def delete(
        self,
        _: str,
        __: DTPSServer,
    ) -> None:
        """Delete."""
        message = f"delete() for {self}."
        raise NotImplementedError(message)

    async def get_source_node_id(self, server: DTPSServer) -> NodeID | None:
        """Return source node identification."""
        return await self.source.get_source_node_id(server)

    async def get_meta_info(
        self,
        _: str,
        __: DTPSServer,
    ) -> TopicsIndex:
        """Return meta information."""
        message = f"OurQueue.get_meta_info() for {self}."
        raise NotImplementedError(message)

    def get_inside_after(self, s: str) -> "Transformed":
        """Return transformed after."""
        message = f"get_inside_after({s!r}) not implemented for {self!r}."
        raise KeyError(message)

    def get_inside(self, s: str, /) -> "Transformed":
        """Return transformed."""
        transform = self.transform.get_transform_inside(s)
        return Transformed(self.source, transform)

    async def get_resolved_data(
        self,
        presented_as: str,
        server: DTPSServer,
        request: HTTPRequest | None,
    ) -> "ResolvedData":
        """Return resolved data."""
        data = await self.source.get_resolved_data(
            presented_as,
            server,
            request,
        )
        return self.transform.transform(data)

    def get_properties(self, server: DTPSServer) -> TopicProperties:
        """Return properties."""
        return self.source.get_properties(server)

    async def patch(
        self,
        presented_as: str,
        server: DTPSServer,
        patch: JsonPatch,
    ) -> PostResult:
        """Patch."""
        if isinstance(self.transform, GetInside):
            patch2 = add_prefix_to_patch(self.transform.components, patch)
            return await self.source.patch(presented_as, server, patch2)
        message = f"patch() for {self}"
        raise NotImplementedError(message)

    async def publish(
        self,
        presented_as: str,
        server: DTPSServer,
        raw_data: RawData,
    ) -> PostResult:
        """Publish."""
        if isinstance(self.transform, GetInside):
            native = raw_data.get_as_native_object()
            path = "".join(
                "/" + component for component in self.transform.components
            )
            ops = [
                {
                    "op": "replace",
                    "path": path,
                    "value": native,
                },
            ]
            patch = JsonPatch(ops)
            return await self.source.patch(presented_as, server, patch)
        message = f"patch() for {self}."
        raise NotImplementedError(message)

    async def call(
        self,
        _: str,
        __: DTPSServer,
        ___: RawData,
    ) -> RawData | TransformError:
        """Call."""
        return TransformError(400, "Cannot call Transformed.")


def add_prefix_to_patch(
    prefix: tuple[str, ...],
    patch: JsonPatch,
) -> JsonPatch:
    patch2 = copy.deepcopy(patch.patch)
    pref = "".join("/" + o for o in prefix)
    for op in patch2:
        if "path" in op:
            op["path"] = pref + op["path"]
    return JsonPatch(patch2)


def get_inside(
    original_ob: object,
    context: tuple[int | str, ...],
    object_: object,
    components: Sequence[str],
) -> object:
    if not components:
        return object_
    first, *rest = components
    if isinstance(object_, dict):
        object_ = cast(dict[str, Any], object_)
        if first not in object_:
            keys = object_.keys()
            key_list = list(keys)
            message = (
                f"Cannot get_inside({components!r}) of dict with keys "
                f"{key_list!r} \ncontext: {context!r}\noriginal:\n"
                f"{original_ob!r}."
            )
            raise KeyError(message)
        v: Any = object_[first]
        return get_inside(original_ob, (*context, first), v, rest)
    if isinstance(object_, list | tuple):
        object_ = cast(list[Any] | tuple[Any, ...], object_)
        try:
            i = int(first)
        except ValueError as error:
            message = (
                f"Cannot get_inside({components!r}) of {object_!r} in "
                f"{context!r} in {original_ob!r}."
            )
            raise KeyError(message) from error
        if i < 0 or i >= len(object_):
            message = "Index out of range."
            raise KeyError(message)
        v = object_[i]
        return get_inside(original_ob, (*context, i), v, rest)
    message = (
        f"Cannot get_inside({components!r}) of {object_!r} in {context!r} in "
        f"{original_ob!r}."
    )
    raise KeyError(message)


async def load_datasaved_resp(
    server: DTPSServer,
    base_url: URL,
    client: DTPSClient,
    resp_data: ClientResponse,
) -> DataReady:
    content = await resp_data.read()
    content_type = ContentType(resp_data.content_type)
    data = RawData(content_type=content_type, content=content)
    data_object = data.get_as_native_object()
    data_saved = pydantic_parse(DataSaved, data_object)
    locations = resp_data.headers.getall("location")
    data_ready = DataReady.from_data_saved(data_saved)
    for location in locations:
        url = join(base_url, location)
        raw_data = await client.get(url, data_saved.content_type)
        available_until = time.time() + DEFAULT_DATA_AVAILABILITY_TIMEOUT
        url_string = server.blob_manager.get_use_once_link_store(
            data_ready.digest,
            raw_data.content,
            data_ready.content_type,
            DEFAULT_DATA_AVAILABILITY_TIMEOUT,
        )
        data_ready.availability.append(
            ResourceAvailability(url_string, available_until),
        )
        break
    else:
        # TODO: How to deal with failure?
        message = f"No location in {locations}."
        raise ValueError(message)
    return data_ready


ResolvedData = RawData | Native | NotAvailableYet | NotFound | Response
