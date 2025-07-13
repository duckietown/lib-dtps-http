"""Structures."""

__all__ = [
    "CHANNEL_MESSAGE_TYPES",
    "Bounds",
    "ChannelInfo",
    "ChannelInfoDesc",
    "ChannelMessages",
    "Chunk",
    "Clocks",
    "ConnectionEstablishedMessage",
    "ConnectionJob",
    "ContentInfo",
    "DataDesc",
    "DataReady",
    "DataSaved",
    "Digest",
    "ErrorMessage",
    "FinishedMessage",
    "ForwardingStep",
    "History",
    "InsertNotification",
    "LinkBenchmark",
    "ListenURLEvents",
    "ListenerInfo",
    "Metadata",
    "MinMax",
    "ProxyJob",
    "PushResult",
    "RawData",
    "Registration",
    "ResourceAvailability",
    "SilenceMessage",
    "TopicProperties",
    "TopicReachability",
    "TopicRef",
    "TopicRefAdd",
    "TopicsIndex",
    "TopicsIndexWire",
    "TransportData",
    "WarningMessage",
    "get_digest",
    "is_image",
    "is_structure",
]

import hashlib
import itertools
import json
from collections.abc import Sequence
from dataclasses import asdict
from typing import (
    Any,
    Literal,
    NewType,
    cast,
)

import cbor2
import xxhash
import yaml
from multidict import CIMultiDict
from pydantic.dataclasses import dataclass

from dtps_http.constants import (
    DEFAULT_MAX_HISTORY,
    HEADER_LINK_BENCHMARK,
    MIME_CBOR,
    MIME_JSON,
    MIME_TEXT,
    MIME_YAML,
)
from dtps_http.types_ import (
    ContentType,
    NodeID,
    SourceID,
    TopicNameS,
    TopicNameV,
    URLString,
)
from dtps_http.urls import (
    URL,
    URLIndexer,
    join,
    parse_url_unescape,
    url_to_string,
)
from dtps_http.utils import pydantic_parse

Digest = NewType("Digest", str)
ServiceMode = Literal["BestEffort", "AllMessages", "AllMessagesSinceStart"]


@dataclass(frozen=True)
class Bounds:
    """Bounds."""

    min_size: int
    min_time_ms: int
    min_bytes: int
    max_size: int | None
    max_time_ms: int | None
    max_bytes: int | None

    @classmethod
    def unbounded(cls) -> "Bounds":
        """Return unbounded."""
        return cls(
            min_size=0,
            min_time_ms=0,
            max_size=None,
            max_time_ms=None,
            min_bytes=0,
            max_bytes=None,
        )

    @classmethod
    def max_length(cls, n: int) -> "Bounds":
        """Return maximum length."""
        return cls(
            min_size=0,
            min_time_ms=0,
            max_size=n,
            max_time_ms=None,
            min_bytes=0,
            max_bytes=None,
        )

    @classmethod
    def default(cls) -> "Bounds":
        """Return default."""
        return cls.max_length(DEFAULT_MAX_HISTORY)


@dataclass
class ChannelInfo:
    """Channel information."""

    queue_created: int
    num_total: int
    newest: "ChannelInfoDesc | None"
    oldest: "ChannelInfoDesc | None"

    @classmethod
    def from_cbor(cls, s: bytes) -> "ChannelInfo":
        """Return from CBOR."""
        struct = cbor2.loads(s)
        return pydantic_parse(cls, struct)


@dataclass
class ChannelInfoDesc:
    """Channel information description."""

    sequence: int
    time_inserted: int


@dataclass
class Chunk:
    """Chunck."""

    digest: str
    i: int
    n: int
    index: int
    data: bytes

    @classmethod
    def from_cbor(cls, s: bytes) -> "Chunk":
        """Return from CBOR."""
        struct = cbor2.loads(s)
        return pydantic_parse(cls, struct)


@dataclass
class Clocks:
    """Clocks."""

    logical: dict[str, "MinMax"]
    wall: dict[str, "MinMax"]

    @classmethod
    def empty(cls) -> "Clocks":
        """Return empty."""
        return Clocks(logical={}, wall={})


@dataclass
class ConnectionEstablishedMessage:
    """Connection established message."""

    comment: str


@dataclass
class ConnectionJob:
    """Connection job."""

    source: TopicNameV
    target: TopicNameV
    service_mode: ServiceMode

    def to_wire(self) -> "ConnectionJobWire":
        """Return connection job wire."""
        dash_separated_source = self.source.as_dash_sep()
        dash_separated_target = self.target.as_dash_sep()
        return ConnectionJobWire(
            dash_separated_source,
            dash_separated_target,
            self.service_mode,
        )


@dataclass
class ConnectionJobWire:
    """Connection job wire."""

    source: TopicNameS
    target: TopicNameS
    service_mode: ServiceMode


@dataclass
class ContentInfo:
    """Content information."""

    accept: dict[str, "DataDesc"]
    storage: "DataDesc"
    produces_content_type: list[ContentType]

    @classmethod
    def simple(
        cls,
        ct: ContentType,
        jschema: object | None = None,
        examples: Sequence["RawData"] = (),
    ) -> "ContentInfo":
        """Return simple."""
        examples_list = list(examples)
        dd = DataDesc(ct, jschema, examples_list)
        return ContentInfo(
            {
                "": dd,
            },
            dd,
            [ct],
        )


@dataclass
class DataDesc:
    """Data description."""

    content_type: ContentType
    jschema: object | None
    examples: list["RawData"]


@dataclass
class DataReady:
    """Data ready."""

    origin_node: NodeID
    unique_id: SourceID
    index: int
    time_inserted: int
    digest: Digest
    content_type: ContentType
    content_length: int
    clocks: Clocks
    availability: list["ResourceAvailability"]
    chunks_arriving: int

    @classmethod
    def from_json_string(cls, s: str) -> "DataReady":
        """Return from JSON string."""
        struct = json.loads(s)
        return pydantic_parse(cls, struct)

    @classmethod
    def from_cbor(cls, s: bytes) -> "DataReady":
        """Return from CBOR."""
        struct = cbor2.loads(s)
        if not isinstance(struct, dict):
            message = f"Expected a dictionary here: {s!r}\n{struct}"
            raise TypeError(message)
        return pydantic_parse(cls, struct)

    def as_data_saved(self) -> "DataSaved":
        """Return data saved."""
        return DataSaved(
            origin_node=self.origin_node,
            unique_id=self.unique_id,
            index=self.index,
            time_inserted=self.time_inserted,
            digest=self.digest,
            content_type=self.content_type,
            content_length=self.content_length,
            clocks=self.clocks,
        )

    @classmethod
    def from_data_saved(cls, ds: "DataSaved") -> "DataReady":
        """Return from data saved."""
        return DataReady(
            origin_node=ds.origin_node,
            unique_id=ds.unique_id,
            index=ds.index,
            time_inserted=ds.time_inserted,
            digest=ds.digest,
            content_type=ds.content_type,
            content_length=ds.content_length,
            clocks=ds.clocks,
            availability=[],
            chunks_arriving=0,
        )


@dataclass
class DataSaved:
    """Data saved."""

    origin_node: NodeID
    unique_id: SourceID
    index: int
    time_inserted: int
    digest: Digest
    content_type: ContentType
    content_length: int
    clocks: "Clocks"


@dataclass
class ErrorMessage:
    """Error message."""

    comment: str


@dataclass
class FinishedMessage:
    """Finished message."""

    comment: str


@dataclass
class ForwardingStep:
    """Forwarding step."""

    forwarding_node: NodeID
    forwarding_node_connects_to: URLString
    performance: "LinkBenchmark"


@dataclass
class InsertNotification:
    """Insert notification."""

    data_saved: DataSaved
    raw_data: "RawData"


@dataclass
class LinkBenchmark:
    """Link benchmark."""

    complexity: int  # 0: local; 1: named unix socket; 2: each network hop
    bandwidth: int  # B/s
    latency_ns: int  # s
    reliability_percent: int  # 0..100
    hops: int

    @classmethod
    def identity(cls) -> "LinkBenchmark":
        """Return link benchmark."""
        return LinkBenchmark(0, 1_000_000_000, 0, 100, 1)

    def __or__(self, other: "LinkBenchmark") -> "LinkBenchmark":
        """Or."""
        complexity = self.complexity + other.complexity
        bandwidth = min(self.bandwidth, other.bandwidth)
        latency = self.latency_ns + other.latency_ns
        reliability = int(
            self.reliability_percent * other.reliability_percent / (100 * 100),
        )
        hops = self.hops + other.hops
        return LinkBenchmark(complexity, bandwidth, latency, reliability, hops)

    def fill_headers(self, headers: CIMultiDict[str]) -> None:
        """Fill headers."""
        # RTT = 2 * latency - in mseconds
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/RTT
        rtt_ns = self.latency_ns * 2
        rtt_ms = rtt_ns / 1_000_000
        headers["RTT"] = f"{rtt_ms:.2f}"
        # Note that Downlink = bandwidth in Mbits/s
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Downlink
        mbs = self.bandwidth / (1024 * 1024)
        headers["Downlink"] = f"{mbs:.3f}"
        # Everything as a JSON object
        self_dictionary = asdict(self)
        headers[HEADER_LINK_BENCHMARK] = json.dumps(self_dictionary)


@dataclass
class ListenerInfo:
    """Listener information."""

    number_of_listeners: int
    max_frequency: float | None


@dataclass
class Metadata:
    """Metadata."""

    sequence: int
    generated_ns: int


@dataclass
class MinMax:
    """Min-max."""

    min: int
    max: int


@dataclass
class ProxyJob:
    """Proxy job."""

    node_id: NodeID | None
    urls: list[URLString]
    mask_origin: bool

    @classmethod
    def from_json(cls, s: Any) -> "ProxyJob":
        """Return proxy job."""
        return pydantic_parse(cls, s)

    def __post_init__(self) -> None:
        """Post proxy job initilization."""
        if not self.urls:
            message = "Empty URLs."
            raise ValueError(message)
        for url in self.urls:
            parse_url_unescape(url)


@dataclass(frozen=True)
class PushResult:
    """Push result."""

    result: bool
    message: str

    @classmethod
    def from_json(cls, s: Any) -> "PushResult":
        """Return push result."""
        return pydantic_parse(cls, s)


@dataclass
class RawData:
    """Raw data."""

    content: bytes
    content_type: ContentType

    def short_description(self) -> str:
        """Return short description."""
        content_length = len(self.content)
        return f"RawData({self.content_type}; {content_length} bytes)"

    @classmethod
    def simple_string(cls, string: str) -> "RawData":
        """Return simple string."""
        content = string.encode("utf-8")
        return cls(content=content, content_type=MIME_TEXT)

    @classmethod
    def cbor_from_native_object(cls, object_: object) -> "RawData":
        """Return CBOR from native object."""
        content = cbor2.dumps(object_)
        return cls(content=content, content_type=MIME_CBOR)

    @classmethod
    def json_from_native_object(cls, object_: object) -> "RawData":
        """Return JSON from native object."""
        json_string = json.dumps(object_)
        encoded_json_string = json_string.encode()
        return cls(encoded_json_string, MIME_JSON)

    @classmethod
    def yaml_from_native_object(cls, object_: object) -> "RawData":
        """Return YAML from native object."""
        data = yaml.safe_dump(object_)
        encoded_data = data.encode()
        return cls(encoded_data, MIME_YAML)

    def digest(self) -> Digest:
        """Digest."""
        return get_digest(self.content)

    def get_as_yaml(self) -> str:
        """Return as YAML."""
        object_ = self.get_as_native_object()
        return yaml.safe_dump(object_)

    def get_as_native_object(self) -> object:
        """Return as native object."""
        if is_plain_text(self.content_type):
            return self.content.decode("utf-8")

        if not is_structure(self.content_type):
            message = (
                f"Cannot convert non-structure content to native object "
                f"(content_type={self.content_type!r})\ndata={self.content!r}"
            )
            raise ValueError(message)
        if is_yaml(self.content_type):
            return yaml.safe_load(self.content)
        if is_json(self.content_type):
            return json.loads(self.content)
        if is_cbor(self.content_type):
            return cbor2.loads(self.content)
        message = f"Cannot convert {self.content_type!r} to native object."
        raise ValueError(message)

    def as_cbor(self) -> "RawData":
        """Return as CBOR."""
        native_object = self.get_as_native_object()
        return RawData.cbor_from_native_object(native_object)

    def as_json(self) -> "RawData":
        """Return as JSON."""
        native_object = self.get_as_native_object()
        return RawData.json_from_native_object(native_object)

    def as_yaml(self) -> "RawData":
        """Return as YAML."""
        native_object = self.get_as_native_object()
        return RawData.yaml_from_native_object(native_object)

    def get_as(self, content_type: str) -> "RawData":
        """Return as `content_type`."""
        split_content_types = []
        for content_type_ in content_type.split(";"):
            split_content_type_ = content_type_.split(",")
            split_content_types.append(split_content_type_)
        chain = itertools.chain.from_iterable(split_content_types)
        content_types = list(chain)
        if MIME_JSON in content_types:
            return self.as_json()
        if MIME_CBOR in content_types:
            return self.as_cbor()
        if MIME_YAML in content_types:
            return self.as_yaml()
        # Must leave most general case to the end
        if "*/*" in content_types:
            return self
        message = f"Cannot convert to {content_type!r}."
        raise ValueError(message)


@dataclass
class Registration:
    """Registration."""

    switchboard_url: URLIndexer
    topic: TopicNameV
    namespace: TopicNameV


@dataclass
class ResourceAvailability:
    """Resource availability."""

    url: URLString
    available_until: float  # Timestamp


@dataclass
class SilenceMessage:
    """Silence message."""

    dt: float
    comment: str


@dataclass
class TopicsIndex:
    """Topics index."""

    topics: dict[TopicNameV, "TopicRef"]

    def __post_init__(self) -> None:
        """Run post topics index initialization."""
        for topic_name, topic in self.topics.items():
            if not topic.reachability:
                dash_separated_topic_name = topic_name.as_dash_sep()
                message = (
                    f"Topic {dash_separated_topic_name!r} has no reachability."
                )
                raise AssertionError(message)

    def to_wire(self) -> "TopicsIndexWire":
        """Return topics index wire."""
        topics = {}
        for topic_name, topic_reference in self.topics.items():
            dash_separated_topic_name = topic_name.as_dash_sep()
            topics[dash_separated_topic_name] = topic_reference.to_wire()
        return TopicsIndexWire(topics)


@dataclass
class TopicsIndexWire:
    """Topics index wire."""

    topics: dict[TopicNameS, "TopicRefWire"]

    @classmethod
    def from_json(cls, s: Any) -> "TopicsIndexWire":
        """Return from JSON."""
        return pydantic_parse(cls, s)

    def to_internal(self, where_this_available: list[URL]) -> "TopicsIndex":
        """Return topics index."""
        topics = {}
        for (
            dash_separated_topic_name,
            topic_reference_wire,
        ) in self.topics.items():
            topic_name = TopicNameV.from_dash_sep(dash_separated_topic_name)
            topics[topic_name] = topic_reference_wire.to_internal(
                where_this_available,
            )
        return TopicsIndex(topics)


@dataclass
class TopicProperties:
    """Topic properties."""

    streamable: bool
    pushable: bool
    readable: bool
    immutable: bool
    has_history: bool
    patchable: bool
    droppable: bool

    @classmethod
    def streamable_readonly(cls) -> "TopicProperties":
        """Return streamable read-only topic properties."""
        return TopicProperties(
            streamable=True,
            pushable=False,
            readable=True,
            immutable=False,
            has_history=True,
            patchable=False,
            droppable=False,
        )

    @classmethod
    def default(cls) -> "TopicProperties":
        """Return default."""
        return TopicProperties.rw_pushable()

    @classmethod
    def readonly(cls) -> "TopicProperties":
        """Return read-only."""
        return TopicProperties(
            streamable=False,
            pushable=False,
            readable=True,
            immutable=False,
            has_history=False,
            patchable=False,
            droppable=False,
        )

    @classmethod
    def rw_pushable(cls) -> "TopicProperties":
        """Return read-write pushable."""
        return TopicProperties(
            streamable=True,
            pushable=True,
            readable=True,
            immutable=False,
            has_history=True,
            patchable=True,
            droppable=True,
        )

    @classmethod
    def patchable_only(cls) -> "TopicProperties":
        """Return patchable only."""
        return TopicProperties(
            streamable=True,
            pushable=False,
            readable=True,
            immutable=False,
            has_history=True,
            patchable=True,
            droppable=False,
        )


@dataclass
class TopicReachability:
    """Topic reachability."""

    url: URLString
    answering: NodeID
    # Mostly for debugging
    forwarders: list[ForwardingStep]
    benchmark: LinkBenchmark

    def __post_init__(self) -> None:
        """Run post topic reachability initialization."""
        if "///" in self.url:
            message = f"Invalid URL: {self.url!r}"
            raise ValueError(message)

    def to_wire(self) -> "TopicReachabilityWire":
        """Return wire."""
        return TopicReachabilityWire(
            url=self.url,  # Should I really do this?
            answering=self.answering,
            forwarders=self.forwarders,
            benchmark=self.benchmark,
        )


@dataclass
class TopicReachabilityWire:
    """Topic reachability wire."""

    url: URLString
    answering: NodeID
    # Mostly for debugging
    forwarders: list[ForwardingStep]
    benchmark: LinkBenchmark

    def to_internal(self, urlbase: URL) -> "TopicReachability":
        url = join(urlbase, self.url)
        url_string = url_to_string(url)
        return TopicReachability(
            url=url_string,
            answering=self.answering,
            forwarders=self.forwarders,
            benchmark=self.benchmark,
        )


@dataclass
class TopicRef:
    """Topic reference."""

    unique_id: SourceID  # Unique ID for the stream
    origin_node: NodeID  # Unique ID of the node that created the stream
    app_data: dict[str, bytes]
    reachability: list[TopicReachability]
    created: int
    properties: TopicProperties
    content_info: ContentInfo
    bounds: Bounds

    def to_wire(self) -> "TopicRefWire":
        """Return wire."""
        reachability = []
        for topic_reachability in self.reachability:
            wire = topic_reachability.to_wire()
            reachability.append(wire)
        return TopicRefWire(
            unique_id=self.unique_id,
            origin_node=self.origin_node,
            app_data=self.app_data,
            reachability=reachability,
            created=self.created,
            properties=self.properties,
            content_info=self.content_info,
            bounds=self.bounds,
        )


@dataclass
class TopicRefAdd:
    """Add topic reference."""

    app_data: dict[str, bytes]
    properties: TopicProperties
    content_info: ContentInfo
    bounds: Bounds

    @classmethod
    def from_json(cls, s: Any) -> "TopicRefAdd":
        """Return from JSON."""
        return pydantic_parse(cls, s)


@dataclass
class TopicRefWire:
    """Topic reference wire."""

    unique_id: SourceID  # Unique ID for the stream
    origin_node: NodeID  # Unique ID of the node that created the stream
    app_data: dict[str, bytes]
    reachability: list[TopicReachabilityWire]
    created: int
    properties: TopicProperties
    content_info: ContentInfo
    bounds: Bounds

    def to_internal(self, where_available: list[URL], /) -> "TopicRef":
        reachability = []
        for topic_reachability_wire in self.reachability:
            for url in where_available:
                topic_reachability = topic_reachability_wire.to_internal(url)
                reachability.append(topic_reachability)
        return TopicRef(
            unique_id=self.unique_id,
            origin_node=self.origin_node,
            app_data=self.app_data,
            reachability=reachability,
            created=self.created,
            properties=self.properties,
            content_info=self.content_info,
            bounds=self.bounds,
        )


@dataclass
class TransportData:
    """Transport data."""

    canonical_url: str
    alternative_urls: list[str]


@dataclass
class WarningMessage:
    """Warning message."""

    comment: str


def get_digest_xxh128(s: bytes) -> Digest:
    xxh128 = xxhash.xxh128()
    xxh128.update(s)
    d = xxh128.hexdigest()
    return cast(Digest, f"xxh128:{d}")


def get_digest_xxh64(s: bytes) -> Digest:
    xxh64 = xxhash.xxh64()
    xxh64.update(s)
    d = xxh64.hexdigest()
    return cast(Digest, f"xxh64:{d}")


def get_digest_xxh32(s: bytes) -> Digest:
    xxh32 = xxhash.xxh32()
    xxh32.update(s)
    d = xxh32.hexdigest()
    return cast(Digest, f"xxh32:{d}")


def get_digest_sha256(s: bytes) -> Digest:
    sha256 = hashlib.sha256(s)
    d = sha256.hexdigest()
    return cast(Digest, f"sha256:{d}")


def get_digest_sha1(s: bytes) -> Digest:
    sha1 = hashlib.sha1(s)
    d = sha1.hexdigest()
    return cast(Digest, f"sha1:{d}")


def get_digest_blake2b(s: bytes) -> Digest:
    """Return blake2b digest."""
    blake2b = hashlib.blake2b(s)
    d = blake2b.hexdigest()
    return cast(Digest, f"blake2b:{d}")


def get_digest_blake2s(s: bytes) -> Digest:
    """Return blake2s digest."""
    blake2s = hashlib.blake2s(s)
    d = blake2s.hexdigest()
    return cast(Digest, f"blake2s:{d}")


def get_digest_md5(s: bytes) -> Digest:
    """Return md5 digest."""
    md5 = hashlib.md5(s)
    d = md5.hexdigest()
    return cast(Digest, f"md5:{d}")


def get_digest(s: bytes) -> Digest:
    """Return digest."""
    return get_digest_xxh128(s)


def is_structure(content_type: str) -> bool:
    """Return `True` if structure, `False` otherwise."""
    return (
        is_cbor(content_type) or is_json(content_type) or is_yaml(content_type)
    )


def is_image(content_type: str) -> bool:
    """Return `True` if image, `False` otherwise."""
    return "image" in content_type


def is_yaml(content_type: str) -> bool:
    return "yaml" in content_type


def is_plain_text(content_type: str) -> bool:
    return "text/plain" in content_type


def is_json(content_type: str) -> bool:
    return "json" in content_type


def is_cbor(content_type: str) -> bool:
    return "cbor" in content_type


CHANNEL_MESSAGE_TYPES = (
    ChannelInfo,
    DataReady,
    Chunk,
    FinishedMessage,
    ErrorMessage,
    WarningMessage,
    SilenceMessage,
)

ChannelMessages = (
    ChannelInfo
    | Chunk
    | DataReady
    | ErrorMessage
    | FinishedMessage
    | SilenceMessage
    | WarningMessage
)
History = dict[int, DataReady]
ListenURLEvents = (
    ConnectionEstablishedMessage
    | ErrorMessage
    | FinishedMessage
    | InsertNotification
    | SilenceMessage
    | WarningMessage
)
