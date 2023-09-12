import json
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Sequence, Union

import cbor2
from multidict import CIMultiDict
from pydantic import TypeAdapter
from pydantic.dataclasses import dataclass
from .constants import HEADER_LINK_BENCHMARK, MIME_TEXT
from .types import ContentType, NodeID, SourceID, TopicNameS, TopicNameV, URLString
from .urls import URL, URLIndexer

__all__ = [
    "ChannelInfo",
    "ChannelInfoDesc",
    "Chunk",
    "Clocks",
    "ContentInfo",
    "DataFromChannel",
    "DataReady",
    "DataSaved",
    "ForwardingStep",
    "History",
    "LinkBenchmark",
    "ListenURLEvents",
    "Metadata",
    "MinMax",
    "RawData",
    "Registration",
    "ResourceAvailability",
    "TopicProperties",
    "TopicReachability",
    "TopicRef",
    "TopicsIndex",
    "TransportData",
    "channel_msgs_parse",
    "is_structure",
]


@dataclass
class LinkBenchmark:
    complexity: int  # 0 for local, 1 for using named, +2 for each network hop
    bandwidth: int  # B/s
    latency_ns: int  # seconds
    reliability_percent: int  # 0..100
    hops: int

    @classmethod
    def identity(cls) -> "LinkBenchmark":
        return LinkBenchmark(
            complexity=0, bandwidth=1_000_000_000, latency_ns=0, reliability_percent=100, hops=1
        )

    def __or__(self, other: "LinkBenchmark") -> "LinkBenchmark":
        complexity = self.complexity + other.complexity
        bandwidth = min(self.bandwidth, other.bandwidth)
        latency = self.latency_ns + other.latency_ns
        reliability = int(self.reliability_percent * other.reliability_percent / (100 * 100))
        hops = self.hops + other.hops
        return LinkBenchmark(complexity, bandwidth, latency, reliability, hops)

    def fill_headers(self, headers: CIMultiDict[str]) -> None:
        # RTT = 2 * latency - in mseconds
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/RTT
        rtt_ns = self.latency_ns * 2
        rtt_ms = rtt_ns / 1_000_000.0
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
    forwarders: List[ForwardingStep]

    benchmark: LinkBenchmark


@dataclass
class TopicProperties:
    streamable: bool
    pushable: bool
    readable: bool
    immutable: bool
    has_history: bool

    patchable: bool

    @classmethod
    def streamable_readonly(cls) -> "TopicProperties":
        return TopicProperties(
            streamable=True, pushable=False, readable=True, immutable=False, has_history=True, patchable=False
        )


@dataclass
class RawData:
    content: bytes
    content_type: ContentType

    @classmethod
    def simple_string(cls, s: str) -> "RawData":
        return cls(s.encode("utf-8"), MIME_TEXT)

    def digest(self) -> str:
        import hashlib

        s = hashlib.sha256(self.content).hexdigest()
        return f"sha256:{s}"

    def get_as_yaml(self) -> str:
        ob = self.get_as_native_object()
        import yaml

        return yaml.safe_dump(ob)

    def get_as_native_object(self) -> object:
        if not is_structure(self.content_type):
            msg = (
                f"Cannot convert non-structure content to native object (content_type={self.content_type!r})"
            )
            raise ValueError(msg)
        if is_yaml(self.content_type):
            import yaml

            return yaml.safe_load(self.content)
        if is_json(self.content_type):
            import json

            return json.loads(self.content)
        if is_cbor(self.content_type):
            import cbor2

            return cbor2.loads(self.content)
        raise ValueError(f"cannot convert {self.content_type!r} to native object")


def is_structure(content_type: str) -> bool:
    return is_yaml(content_type) or is_json(content_type) or is_cbor(content_type)


def is_yaml(content_type: str) -> bool:
    return "yaml" in content_type


def is_json(content_type: str) -> bool:
    return "json" in content_type


def is_cbor(content_type: str) -> bool:
    return "cbor" in content_type


@dataclass
class DataDesc:
    content_type: ContentType
    jschema: Optional[object]
    examples: List[RawData]


@dataclass
class DataSaved:
    index: int
    time_inserted: int
    digest: str
    content_type: ContentType
    content_length: int
    clocks: "Clocks"


@dataclass
class ContentInfo:
    accept: Dict[str, DataDesc]
    storage: DataDesc
    produces_content_type: List[ContentType]

    @classmethod
    def simple(cls, ct: ContentType, jschema: Optional[object] = None, examples: Sequence[RawData] = ()):
        dd = DataDesc(content_type=ct, jschema=jschema, examples=list(examples))

        return ContentInfo(
            accept={"": dd},
            storage=dd,
            produces_content_type=[ct],
        )


@dataclass
class TopicRef:
    unique_id: SourceID  # unique id for the stream
    origin_node: NodeID  # unique id of the node that created the stream
    app_data: Dict[str, bytes]
    reachability: List[TopicReachability]
    created: int
    properties: TopicProperties
    content_info: ContentInfo


@dataclass
class TopicsIndex:
    topics: Dict[TopicNameV, TopicRef]

    def __post_init__(self):
        for k, v in self.topics.items():
            if not v.reachability:
                msg = f"Topic {k.as_dash_sep()!r} has no reachability"
                raise AssertionError(msg)

    @classmethod
    def from_json(cls, s: Any) -> "TopicsIndex":
        wire = TopicsIndexWire.from_json(s)
        return wire.to_topics_index()

    def to_wire(self) -> "TopicsIndexWire":
        res = {}
        for k, v in self.topics.items():
            res[k.as_dash_sep()] = v
        return TopicsIndexWire(res)


@dataclass
class TopicsIndexWire:
    topics: Dict[TopicNameS, TopicRef]

    @classmethod
    def from_json(cls, s: Any) -> "TopicsIndexWire":
        return TypeAdapter(cls).validate_python(s)

    def to_topics_index(self) -> "TopicsIndex":
        topics: Dict[TopicNameV, TopicRef] = {}
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
        return TypeAdapter(cls).validate_python(struct)


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
        return TypeAdapter(cls).validate_python(struct)


@dataclass
class MinMax:
    min: int
    max: int


@dataclass
class Clocks:
    logical: Dict[str, MinMax]
    wall: Dict[str, MinMax]

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
    content_type: ContentType
    content_length: int
    clocks: Clocks
    availability: List[ResourceAvailability]
    chunks_arriving: int

    @classmethod
    def from_json_string(cls, s: str) -> "DataReady":
        struct = json.loads(s)
        return TypeAdapter(cls).validate_python(struct)

    @classmethod
    def from_cbor(cls, s: bytes) -> "DataReady":
        struct = cbor2.loads(s)
        return TypeAdapter(cls).validate_python(struct)


@dataclass
class DataFromChannel:
    data_ready: DataReady
    raw_data: RawData


# @dataclass
# class History:
#     available: Dict[int, DataReady]

History = Dict[int, DataReady]


@dataclass
class WarningMsg:
    comment: str


@dataclass
class ErrorMsg:
    comment: str


@dataclass
class FinishedMsg:
    comment: str


def channel_msgs_parse(d: bytes) -> "ChannelMsgs":
    struct = cbor2.loads(d)
    if not isinstance(struct, dict):
        msg = "Expected a dictionary here"
        raise ValueError(f"{msg}: {d}\n{struct}")

    for T in (ChannelInfo, DataReady, Chunk, FinishedMsg, ErrorMsg, WarningMsg):
        if T.__name__ in struct:
            return TypeAdapter(T).validate_python(struct[T.__name__])

    raise ValueError(f"unexpected value {struct}")


@dataclass
class TransportData:
    canonical_url: str
    alternative_urls: List[str]


@dataclass
class Metadata:
    sequence: int
    generated_ns: int


@dataclass
class Registration:
    switchboard_url: "URLIndexer"
    topic: TopicNameV
    namespace: TopicNameV


ChannelMsgs = Union[ChannelInfo, DataReady, Chunk, FinishedMsg, ErrorMsg, WarningMsg]
ListenURLEvents = Union[DataFromChannel, WarningMsg, ErrorMsg, FinishedMsg]
