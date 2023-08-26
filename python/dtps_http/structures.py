import json
from dataclasses import asdict
from typing import Optional, Sequence

import cbor2
from multidict import CIMultiDict
from pydantic import parse_obj_as
from pydantic.dataclasses import dataclass

from .constants import HEADER_LINK_BENCHMARK
from .types import NodeID, SourceID, TopicNameS, TopicNameV, URLString

__all__ = [
    "ChannelInfo",
    "ChannelInfoDesc",
    "Chunk",
    "Clocks",
    "ContentInfo",
    "DataReady",
    "ForwardingStep",
    "LinkBenchmark",
    "Metadata",
    "MinMax",
    "RawData",
    "ResourceAvailability",
    "TopicProperties",
    "TopicReachability",
    "TopicRef",
    "TopicsIndex",
    "TransportData",
    "channel_msgs_parse",
]


@dataclass
class LinkBenchmark:
    complexity: int  # 0 for local, 1 for using named, +2 for each network hop
    bandwidth: int  # B/s
    latency: float  # seconds
    reliability: float  # 0..1
    hops: int

    @classmethod
    def identity(cls) -> "LinkBenchmark":
        return LinkBenchmark(complexity=0, bandwidth=1_000_000_000, latency=0.0, reliability=1.0, hops=1)

    def __or__(self, other: "LinkBenchmark") -> "LinkBenchmark":
        complexity = self.complexity + other.complexity
        bandwidth = min(self.bandwidth, other.bandwidth)
        latency = self.latency + other.latency
        reliability = self.reliability * other.reliability
        hops = self.hops + other.hops
        return LinkBenchmark(complexity, bandwidth, latency, reliability, hops)

    def fill_headers(self, headers: CIMultiDict[str]) -> None:
        # RTT = 2 * latency - in mseconds
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/RTT
        rtt_ms = (self.latency * 2) * 1000
        headers["RTT"] = f"{rtt_ms:.2f}"
        # Downlink = bandwidth in Mbits/s
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Downlink
        mbs = self.bandwidth / (1024.0 * 1024.0)
        headers["Downlink"] = f"{mbs:.3f}"
        # everything as a json object
        headers[HEADER_LINK_BENCHMARK] = json.dumps(asdict(self))


@dataclass
class ForwardingStep:
    forwarding_node: NodeID
    forwarding_node_connects_to: URLString

    performance: LinkBenchmark


@dataclass
class TopicReachability:
    url: URLString
    answering: NodeID

    # mostly for debugging
    forwarders: list[ForwardingStep]

    benchmark: LinkBenchmark


@dataclass
class TopicProperties:
    streamable: bool
    pushable: bool
    readable: bool
    immutable: bool

    @classmethod
    def streamable_readonly(cls) -> "TopicProperties":
        return TopicProperties(streamable=True, pushable=False, readable=True, immutable=False)


ContentType = str


@dataclass
class RawData:
    content: bytes
    content_type: str

    @classmethod
    def simple_string(cls, s: str) -> "RawData":
        return cls(s.encode("utf-8"), "text/plain")

    def digest(self) -> str:
        import hashlib

        s = hashlib.sha256(self.content).hexdigest()
        return f"sha256:{s}"


@dataclass
class ContentInfo:
    accept_content_type: list[ContentType]
    storage_content_type: list[ContentType]
    produces_content_type: list[ContentType]
    jschema: Optional[object]
    examples: list[RawData]

    @classmethod
    def simple(cls, ct: ContentType, jschema: Optional[object] = None, examples: Sequence[RawData] = ()):
        return ContentInfo(
            accept_content_type=[ct],
            storage_content_type=[ct],
            produces_content_type=[ct],
            jschema=jschema,
            examples=list(examples),
        )


@dataclass
class TopicRef:
    unique_id: SourceID  # unique id for the stream
    origin_node: NodeID  # unique id of the node that created the stream
    app_data: dict[str, bytes]
    reachability: list[TopicReachability]
    created: int
    properties: TopicProperties
    content_info: ContentInfo


@dataclass
class TopicsIndex:
    topics: dict[TopicNameV, TopicRef]

    @classmethod
    def from_json(cls, s: object) -> "TopicsIndex":
        wire = TopicsIndexWire.from_json(s)
        return wire.to_topics_index()

    def to_wire(self) -> "TopicsIndexWire":
        res = {}
        for k, v in self.topics.items():
            res[k.as_dash_sep()] = v
        return TopicsIndexWire(res)


@dataclass
class TopicsIndexWire:
    topics: dict[TopicNameS, TopicRef]

    @classmethod
    def from_json(cls, s: object) -> "TopicsIndexWire":
        return parse_obj_as(TopicsIndexWire, s)

    def to_topics_index(self) -> "TopicsIndex":
        topics: dict[TopicNameV, TopicRef] = {}
        for k, v in self.topics.items():
            topics[TopicNameV.from_dash_sep(k)] = v
        return TopicsIndex(topics)


# used in websockets


@dataclass
class ResourceAvailability:
    url: URLString
    available_until: float  # timestamp


@dataclass
class ChannelInfoDesc:
    sequence: int
    time_inserted: int


@dataclass
class ChannelInfo:
    queue_created: int
    num_total: int
    newest: Optional[ChannelInfoDesc]
    oldest: Optional[ChannelInfoDesc]

    @classmethod
    def from_cbor(cls, s: bytes) -> "ChannelInfo":
        struct = cbor2.loads(s)
        return parse_obj_as(ChannelInfo, struct)


@dataclass
class Chunk:
    digest: str
    i: int
    n: int
    index: int
    data: bytes

    @classmethod
    def from_cbor(cls, s: bytes) -> "Chunk":
        struct = cbor2.loads(s)
        return parse_obj_as(Chunk, struct)


@dataclass
class MinMax:
    min: int
    max: int


@dataclass
class Clocks:
    logical: dict[str, MinMax]
    wall: dict[str, MinMax]

    @classmethod
    def empty(cls) -> "Clocks":
        return Clocks(logical={}, wall={})


@dataclass
class DataReady:
    origin_node: NodeID
    unique_id: SourceID
    sequence: int
    time_inserted: int
    digest: str
    content_type: str
    content_length: int
    clocks: Clocks
    availability: list[ResourceAvailability]
    chunks_arriving: int

    @classmethod
    def from_json_string(cls, s: str) -> "DataReady":
        return parse_obj_as(DataReady, json.loads(s))

    @classmethod
    def from_cbor(cls, s: bytes) -> "DataReady":
        struct = cbor2.loads(s)
        return parse_obj_as(DataReady, struct)


def channel_msgs_parse(d: bytes) -> ChannelInfo | DataReady | Chunk:
    struct = cbor2.loads(d)
    if not isinstance(struct, dict):
        msg = "Expected a dictionary here"
        raise ValueError(f"{msg}: {d}\n{struct}")
    if DataReady.__name__ in struct:
        dr = parse_obj_as(DataReady, struct["DataReady"])
        return dr
    elif ChannelInfo.__name__ in struct:
        dr = parse_obj_as(ChannelInfo, struct["ChannelInfo"])
        return dr
    elif Chunk.__name__ in struct:
        dr = parse_obj_as(Chunk, struct["Chunk"])
        return dr
    else:
        raise ValueError(f"unexpected value {struct}")


@dataclass
class TransportData:
    canonical_url: str
    alternative_urls: list[str]


@dataclass
class Metadata:
    sequence: int
    generated_ns: int
