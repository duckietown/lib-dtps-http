import copy
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, replace
from typing import Any, Dict, List, Optional, Sequence, Tuple, TYPE_CHECKING, Union

import cbor2
import jsonpatch
from aiohttp import web
from jsonpatch import JsonPatch

from . import logger
from .constants import CONTENT_TYPE_DTPS_INDEX_CBOR
from .object_queue import ObjectTransformResult, TransformError
from .structures import (
    ContentInfo,
    LinkBenchmark,
    RawData,
    TopicProperties,
    TopicReachability,
    TopicRef,
    TopicsIndex,
)
from .types import ContentType, NodeID, SourceID, TopicNameV
from .urls import get_relative_url, parse_url_unescape, relative_url

__all__ = [
    "ForwardedQueue",
    "Native",
    "NotAvailableYet",
    "NotFound",
    "OurQueue",
    "Source",
    "SourceComposition",
    "Transformed",
    "TypeOfSource",
]


@dataclass
class Native:
    ob: object


@dataclass
class NotAvailableYet:
    comment: str


@dataclass
class NotFound:
    comment: str


ResolvedData = Union[RawData, Native, NotAvailableYet, NotFound]

if TYPE_CHECKING:
    from .server import DTPSServer


class Source(ABC):
    def resolve_extra(self, components: Tuple[str, ...], extra: Optional[str]) -> "Source":
        if not components:
            if extra is None:
                return self
            else:
                return self.get_inside_after(extra)
        else:
            first, *rest = components
            return self.get_inside(first).resolve_extra(tuple(rest), extra)

    @abstractmethod
    def get_properties(self, server: "DTPSServer") -> TopicProperties:
        raise NotImplementedError(f"Source.get_properties() for {self}")

    @abstractmethod
    def get_inside_after(self, s: str) -> "Source":
        ...

    @abstractmethod
    def get_inside(self, s: str, /) -> "Source":
        ...

    @abstractmethod
    async def get_resolved_data(
        self, request: web.Request, presented_as: str, server: "DTPSServer"
    ) -> "ResolvedData":
        ...

    @abstractmethod
    async def get_meta_info(self, presented_as: str, server: "DTPSServer") -> "TopicsIndex":
        raise NotImplementedError(f"Source.get_meta_info() for {self}")

    @abstractmethod
    async def patch(
        self, request: web.Request, presented_as: str, server: "DTPSServer", patch: JsonPatch
    ) -> "ObjectTransformResult":
        ...

    @abstractmethod
    async def post(
        self, request: web.Request, presented_as: str, server: "DTPSServer", rd: RawData
    ) -> "ObjectTransformResult":
        ...


class Transform(ABC):
    @abstractmethod
    def transform(self, data: "ResolvedData") -> "ResolvedData":
        ...

    def get_transform_inside(self, s: str) -> "Transform":
        raise NotImplementedError(f"Transform.get_transform_inside() for {self}")


@dataclass
class GetInside(Transform):
    components: Tuple[str, ...]

    def get_transform_inside(self, s: str) -> "Transform":
        return GetInside(self.components + (s,))

    def transform(self, data: "ResolvedData") -> "ResolvedData":
        if isinstance(data, RawData):
            ob = data.get_as_native_object()
            return Native(self.apply(ob))
        elif isinstance(data, Native):
            return Native(self.apply(data.ob))
        elif isinstance(data, NotAvailableYet):
            return data
        else:
            assert isinstance(data, NotFound)
            return data

        # raise NotImplementedError(f"Transform.transform() for {self}")

    def apply(self, ob: object) -> object:
        return get_inside(ob, (), ob, self.components)


def get_inside(
    original_ob: object, context: Tuple[Union[int, str], ...], ob: object, components: Sequence[str]
) -> object:
    if not components:
        return ob

    first, *rest = components

    if isinstance(ob, dict):
        if first not in ob:
            keys: List[Any] = list(ob.keys())
            raise KeyError(
                f"cannot get_inside({components!r}) of dict with keys {keys!r}\ncontext: "
                f"{context!r}\noriginal:\n{original_ob!r}"
            )
        v: Any = ob[first]
        return get_inside(original_ob, context + (first,), v, rest)
    elif isinstance(ob, (list, tuple)):
        try:
            i = int(first)
        except ValueError:
            raise KeyError(f"cannot get_inside({components!r}) of {ob!r} in {context!r} in {original_ob!r}")
        if i < 0 or i >= len(ob):
            raise KeyError(f"index out of range")

        v = ob[i]
        return get_inside(original_ob, context + (i,), v, rest)

    else:
        raise KeyError(f"cannot get_inside({components!r}) of {ob!r} in {context!r} in {original_ob!r}")


@dataclass
class OurQueue(Source):
    topic_name: TopicNameV

    async def get_meta_info(self, presented_as: str, server: "DTPSServer") -> "TopicsIndex":
        oq = server.get_oq(self.topic_name)
        tr = oq.tr

        url_supposed = self.topic_name.as_relative_url()
        url_relative = get_relative_url(url_supposed, presented_as)

        reachability = TopicReachability(
            url=url_relative, answering=server.node_id, forwarders=[], benchmark=LinkBenchmark.identity()
        )

        tr = replace(tr, reachability=[reachability])
        return TopicsIndex(topics={TopicNameV.root(): tr})

    def get_properties(self, server: "DTPSServer") -> TopicProperties:
        oq = server.get_oq(self.topic_name)
        return oq.tr.properties

    def get_inside_after(self, s: str) -> "Source":
        raise KeyError(f"get_inside_after({s!r}) not implemented for {self!r}")

    def get_inside(self, s: str, /) -> "Source":
        return Transformed(self, GetInside((s,)))

    async def get_resolved_data(
        self, request: web.Request, presented_as: str, server: "DTPSServer"
    ) -> "ResolvedData":
        oq = server.get_oq(self.topic_name)
        if not oq.stored:
            return NotAvailableYet(f"no data yet for {self.topic_name.as_dash_sep()}")
        return oq.last_data()

    async def post(
        self, request: web.Request, presented_as: str, server: "DTPSServer", rd: RawData
    ) -> "ObjectTransformResult":
        oq = server.get_oq(self.topic_name)

        otr = await oq.publish(rd)
        return otr

    async def patch(
        self, request: web.Request, presented_as: str, server: "DTPSServer", patch: JsonPatch
    ) -> "ObjectTransformResult":
        oq = server.get_oq(self.topic_name)
        last_data = oq.last_data()
        ob = last_data.get_as_native_object()
        try:
            # noinspection PyTypeChecker
            ob2 = patch.apply(ob)
        except jsonpatch.JsonPatchException as e:
            msg = f"Cannot apply patch {patch} to {ob}"
            logger.error(msg + f": {e}")
            raise web.HTTPBadRequest(reason=msg) from e

        rd = RawData.json_from_native_object(ob2)
        otr = await oq.publish(rd)
        return otr


@dataclass
class ForwardedQueue(Source):
    topic_name: TopicNameV

    async def get_meta_info(self, presented_as: str, server: "DTPSServer") -> "TopicsIndex":
        raise NotImplementedError(f"OurQueue.get_meta_info() for {self}")

    def get_inside_after(self, s: str) -> "Source":
        raise KeyError(f"get_inside_after({s!r}) not implemented for {self!r}")

    def get_inside(self, s: str, /) -> "Source":
        raise KeyError(f"get_inside({s!r}) not implemented for {self!r}")

    def get_properties(self, server: "DTPSServer") -> TopicProperties:
        fd = server._forwarded[self.topic_name]
        return fd.properties

    async def get_resolved_data(
        self, request: web.Request, presented_as: str, server: "DTPSServer"
    ) -> "ResolvedData":
        url_data = server._forwarded[self.topic_name].forward_url_data
        from dtps_http import DTPSClient, my_raise_for_status

        async with DTPSClient.create() as dtpsclient:
            async with dtpsclient.my_session(url_data) as (session2, use_url2):
                async with session2.get(use_url2) as resp_data:
                    await my_raise_for_status(resp_data, url_data)
                    data = await resp_data.read()
                    content_type = ContentType(resp_data.content_type)
                    data = RawData(content_type=content_type, content=data)
                    return data

    async def patch(
        self, request: web.Request, presented_as: str, server: "DTPSServer", patch: JsonPatch
    ) -> "ObjectTransformResult":
        raise NotImplementedError(f"patch() for {self}")  # TODO: DTSW-4787: patch forwarded queue

    async def post(
        self, request: web.Request, presented_as: str, server: "DTPSServer", rd: RawData
    ) -> "ObjectTransformResult":
        raise NotImplementedError(f"post() for {self}")  # TODO: DTSW-4780: post forwarded queue


@dataclass
class SourceComposition(Source):
    topic_name: TopicNameV
    sources: Dict[TopicNameV, Source]
    unique_id: SourceID
    origin_node: NodeID

    async def get_meta_info(self, presented_as: str, server: "DTPSServer") -> "TopicsIndex":
        topics = {}
        for prefix, source in self.sources.items():
            x = await source.get_meta_info(presented_as, server)

            for a, b in x.topics.items():
                topics[prefix + a] = b

        supposed = self.topic_name.as_relative_url()
        url_relative = get_relative_url(supposed, presented_as)
        reachability = TopicReachability(
            url=url_relative, answering=server.node_id, forwarders=[], benchmark=LinkBenchmark.identity()
        )

        content_info = ContentInfo.simple(CONTENT_TYPE_DTPS_INDEX_CBOR)
        topics[TopicNameV.root()] = TopicRef(
            unique_id=self.unique_id,
            origin_node=self.origin_node,
            app_data={},
            reachability=[reachability],
            created=0,
            properties=self.get_properties(server),
            content_info=content_info,
        )
        return TopicsIndex(topics=topics)

    def get_properties(self, server: "DTPSServer") -> TopicProperties:
        immutable = True
        streamable = False
        pushable = False
        readable = True

        for _k, v in self.sources.items():
            p = v.get_properties(server)

            immutable = immutable and p.immutable
            streamable = streamable or p.streamable
            readable = readable and p.readable

        return TopicProperties(
            streamable=streamable,
            pushable=pushable,
            readable=readable,
            immutable=immutable,
            has_history=False,
            patchable=False,
        )

    def get_inside_after(self, s: str) -> "Source":
        raise KeyError(f"get_inside_after({s!r}) not implemented for {self!r}")

    def get_inside(self, s: str, /) -> "Source":
        raise KeyError(f"get_inside({s!r}) not implemented for {self!r}")

    async def get_resolved_data(
        self, request: web.Request, presented_as: str, server: "DTPSServer"
    ) -> "ResolvedData":
        data = await self.get_meta_info(presented_as, server)
        as_cbor = cbor2.dumps(asdict(data.to_wire()))
        return RawData(content_type=CONTENT_TYPE_DTPS_INDEX_CBOR, content=as_cbor)

    async def patch(
        self, request: web.Request, presented_as: str, server: "DTPSServer", patch: JsonPatch
    ) -> "ObjectTransformResult":
        return TransformError(400, "Cannot patch SourceComposition")

    async def post(
        self, request: web.Request, presented_as: str, server: "DTPSServer", rd: RawData
    ) -> "ObjectTransformResult":
        return TransformError(400, "Cannot post to SourceComposition")


@dataclass
class Transformed(Source):
    source: Source
    transform: Transform

    async def get_meta_info(self, presented_as: str, server: "DTPSServer") -> "TopicsIndex":
        raise NotImplementedError(f"OurQueue.get_meta_info() for {self}")

    def get_inside_after(self, s: str) -> "Source":
        raise KeyError(f"get_inside_after({s!r}) not implemented for {self!r}")

    def get_inside(self, s: str, /) -> "Source":
        return Transformed(self.source, self.transform.get_transform_inside(s))

    async def get_resolved_data(
        self, request: web.Request, presented_as: str, server: "DTPSServer"
    ) -> "ResolvedData":
        data = await self.source.get_resolved_data(request, presented_as, server)
        return self.transform.transform(data)

    def get_properties(self, server: "DTPSServer") -> TopicProperties:
        return self.source.get_properties(server)

    async def patch(
        self, request: web.Request, presented_as: str, server: "DTPSServer", patch: JsonPatch
    ) -> "ObjectTransformResult":
        # logger.debug(f"Transformed.patch() {self} {patch}")
        if isinstance(self.transform, GetInside):
            patch2 = add_prefix_to_patch(self.transform.components, patch)
            return await self.source.patch(request, presented_as, server, patch2)
        else:
            raise NotImplementedError(f"patch() for {self}")

        # raise NotImplementedError(f"patch() for {self}")

    async def post(
        self, request: web.Request, presented_as: str, server: "DTPSServer", rd: RawData
    ) -> "ObjectTransformResult":
        if isinstance(self.transform, GetInside):
            native = rd.get_as_native_object()
            path = "".join("/" + o for o in self.transform.components)
            ops = [{"op": "replace", "path": path, "value": native}]
            patch = JsonPatch(ops)

            return await self.source.patch(request, presented_as, server, patch)
        else:
            raise NotImplementedError(f"patch() for {self}")


def add_prefix_to_patch(prefix: Tuple[str, ...], patch: JsonPatch) -> JsonPatch:
    patch2 = copy.deepcopy(patch.patch)
    pref = "".join("/" + o for o in prefix)
    for op in patch2:
        if "path" in op:
            op["path"] = pref + op["path"]

    return JsonPatch(patch2)


@dataclass
class MetaInfo(Source):
    source: Source

    async def get_meta_info(self, presented_as: str, server: "DTPSServer") -> "TopicsIndex":
        raise NotImplementedError(f"OurQueue.get_meta_info() for {self}")  # TODO: DTSW-4789

    def get_properties(self, server: "DTPSServer") -> TopicProperties:
        return TopicProperties.readonly()

    def get_inside_after(self, s: str) -> "Source":
        raise KeyError(f"get_inside_after({s!r}) not implemented for {self!r}")

    def get_inside(self, s: str, /) -> "Source":
        raise KeyError(f"get_inside({s!r}) not implemented for {self!r}")

    async def get_resolved_data(
        self, request: web.Request, presented_as: str, server: "DTPSServer"
    ) -> "ResolvedData":
        raise NotImplementedError("MetaInfo.get_resolved_data()")  # TODO: DTSW-4789

    async def patch(
        self, request: web.Request, presented_as: str, server: "DTPSServer", patch: JsonPatch
    ) -> "ObjectTransformResult":
        return TransformError(400, "Cannot PATCH MetaInfo")

    async def post(
        self, request: web.Request, presented_as: str, server: "DTPSServer", rd: RawData
    ) -> "ObjectTransformResult":
        return TransformError(400, "Cannot POST MetaInfo")


TypeOfSource = Union[OurQueue, ForwardedQueue, SourceComposition, Transformed]
