import json
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, replace
from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING, Union

from aiohttp import web

from .constants import CONTENT_TYPE_DTPS_INDEX_CBOR
from .structures import (
    ContentInfo,
    LinkBenchmark,
    RawData,
    TopicProperties,
    TopicReachability,
    TopicRef,
    TopicsIndex,
)
from .types import NodeID, SourceID, TopicNameV

__all__ = [
    "ForwardedQueue",
    "OurQueue",
    "Source",
    "SourceComposition",
    "Transformed",
    "TypeOfSource",
]

from .urls import get_relative_url


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
    def resolve_extra(self, components: Tuple[str, ...], extra: Optional[str]):
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
        elif isinstance(data, NotFound):
            return data
        raise NotImplementedError(f"Transform.transform() for {self}")

    def apply(self, ob: object) -> object:
        return get_inside(ob, (), ob, self.components)


def get_inside(
    original_ob: object, context: Tuple[Union[int, str], ...], ob: object, components: Tuple[str, ...]
) -> object:
    if not components:
        return ob

    first, *rest = components

    if isinstance(ob, dict):
        if first not in ob:
            raise KeyError(
                f"cannot get_inside({components!r}) of dict with keys {list(ob)!r}\ncontext: "
                f"{context!r}\noriginal:\n{original_ob!r}"
            )
        v = ob[first]
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

        # raise NotImplementedError(f"OurQueue.get_meta_info() for {self}")

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
        # resp = await server.serve_get_proxied(request, server._forwarded[self.topic_name])
        url_data = server._forwarded[self.topic_name].forward_url_data
        from dtps_http import DTPSClient

        async with DTPSClient.create() as dtpsclient:
            async with dtpsclient.my_session(url_data) as (session2, use_url2):
                async with session2.get(use_url2) as resp_data:
                    resp_data.raise_for_status()
                    data = await resp_data.read()
                    content_type = resp_data.content_type
                    data = RawData(content_type=content_type, content=data)
                    return data


import cbor2, yaml


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
            topics.update()

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

    async def get_resolved_data0(
        self, request: web.Request, presented_as: str, server: "DTPSServer"
    ) -> "ResolvedData":
        res: Dict[str, Any] = {}
        for k0, v in self.sources.items():
            k = k0.as_dash_sep()
            data = await v.get_resolved_data(request, presented_as, server)
            if isinstance(data, RawData):
                if "cbor" in data.content_type:
                    res[k] = cbor2.loads(data.content)
                elif "json" in data.content_type:
                    res[k] = json.loads(data.content)
                elif "yaml" in data.content_type:
                    res[k] = yaml.safe_load(data.content)
                else:
                    res[k] = {"content": data.content, "content_type": data.content_type}
            elif isinstance(data, Native):
                res[k] = data.ob

            elif isinstance(data, NotAvailableYet):
                res[k] = None
                pass
            elif isinstance(data, NotFound):
                res[k] = None
                pass
        return Native(res)

    async def get_resolved_data2(
        self, request: web.Request, presented_as: str, server: "DTPSServer"
    ) -> "ResolvedData":
        res: Dict[str, Any] = {}
        for k0, v in self.sources.items():
            k = k0.as_dash_sep()
            data = await v.get_resolved_data(request, presented_as, server)
            if isinstance(data, RawData):
                if "cbor" in data.content_type:
                    res[k] = cbor2.loads(data.content)
                elif "json" in data.content_type:
                    res[k] = json.loads(data.content)
                elif "yaml" in data.content_type:
                    res[k] = yaml.safe_load(data.content)
                else:
                    res[k] = {"content": data.content, "content_type": data.content_type}
            elif isinstance(data, Native):
                res[k] = data.ob
            elif isinstance(data, NotAvailableYet):
                res[k] = None
                pass
            elif isinstance(data, NotFound):
                res[k] = None
                pass
            else:
                raise AssertionError
        return Native(res)


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


@dataclass
class MetaInfo(Source):
    source: Source

    async def get_meta_info(self, presented_as: str, server: "DTPSServer") -> "TopicsIndex":
        raise NotImplementedError(f"OurQueue.get_meta_info() for {self}")

    def get_properties(self, server: "DTPSServer") -> TopicProperties:
        return self.source.get_properties(server)  # XXX

    def get_inside_after(self, s: str) -> "Source":
        raise KeyError(f"get_inside_after({s!r}) not implemented for {self!r}")

    def get_inside(self, s: str, /) -> "Source":
        raise KeyError(f"get_inside({s!r}) not implemented for {self!r}")

    async def get_resolved_data(
        self, request: web.Request, presented_as: str, server: "DTPSServer"
    ) -> "ResolvedData":
        raise NotImplementedError("MetaInfo.get_resolved_data()")


TypeOfSource = Union[OurQueue, ForwardedQueue, SourceComposition, Transformed]
