import json
from dataclasses import asdict

import cbor2
from multidict import CIMultiDict
from pydantic import parse_obj_as
from pydantic.dataclasses import dataclass

from .constants import HEADER_LINK_BENCHMARK
from .types import NodeID, SourceID, TopicName, URLString

__all__ = ["DataReady", "ForwardingStep", "LinkBenchmark", "RawData", "ResourceAvailability",
    "TopicReachability", "channel_msgs_parse", "TopicRef", "TopicsIndex", ]


@dataclass
class LinkBenchmark:
    complexity: int  # 0 for local, 1 for using named, +2 for each network hop
    bandwidth: float  # B/s
    latency: float  # seconds
    reliability: float  # 0..1
    hops: int

    @classmethod
    def identity(cls) -> "LinkBenchmark":
        return cls(complexity=0, bandwidth=float(1_000_000_000), latency=0.0, reliability=1.0, hops=0)

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
class TopicRef:
    unique_id: SourceID  # unique id for the stream
    origin_node: NodeID  # unique id of the node that created the stream
    app_data: dict[str, bytes]
    reachability: list[TopicReachability]
    


@dataclass
class TopicsIndex:
    node_id: NodeID
    node_started: int
    node_app_data: dict[str, bytes]

    topics: dict[TopicName, TopicRef]

    @classmethod
    def from_json(cls, s: object) -> "TopicsIndex":
        return parse_obj_as(TopicsIndex, s)


# used in websockets


@dataclass
class ResourceAvailability:
    url: URLString
    available_until: float  # timestamp


@dataclass
class ChannelInfo:
    last_sequence: int
    last_timestamp: int
    oldest_available_sequence: int
    newest_available_timestamp: int


    @classmethod
    def from_cbor(cls, s: bytes) -> "DataReady":
        struct = cbor2.loads(s)
        return parse_obj_as(DataReady, struct)

@dataclass
class DataReady:
    sequence: int
    time_inserted: int
    content_type: str
    content_length: int
    digest: str
    availability: list[ResourceAvailability]
    chunks_arriving: int

    @classmethod
    def from_json_string(cls, s: str) -> "DataReady":
        return parse_obj_as(DataReady, json.loads(s))

    @classmethod
    def from_cbor(cls, s: bytes) -> "DataReady":
        struct = cbor2.loads(s)
        return parse_obj_as(DataReady, struct)

def channel_msgs_parse(d: bytes) -> ChannelInfo | DataReady:
    struct = cbor2.loads(d)
    if 'DataReady' in struct:
        dr = parse_obj_as(DataReady, struct['DataReady'])
        return dr
    elif 'ChannelInfo' in struct:
        dr = parse_obj_as(ChannelInfo, struct['ChannelInfo'])
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
