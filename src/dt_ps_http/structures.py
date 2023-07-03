import json
from typing import NewType, Optional

from pydantic import parse_obj_as
from pydantic.dataclasses import dataclass

from .types import URLString

__all__ = [
    "DataReady",
    "RawData",
    "TopicName",
    "TopicReachability",
    "TopicRef",
    "TopicsIndex",
    "as_TopicName",
]

TopicName = NewType("TopicName", str)


def as_TopicName(s: str) -> TopicName:
    # TODO: Check that s is a valid topic name
    return TopicName(s)


NodeID = NewType("NodeID", str)
SourceID = NewType("SourceID", str)


@dataclass
class ForwardingStep:
    forwarding_node: NodeID
    forwarding_node_connects_to: URLString
    estimated_latency: float
    estimated_bandwidth: float
    complexity: int


@dataclass
class TopicReachability:
    url: URLString
    answering: NodeID

    forwarders: list[ForwardingStep]

    def get_total_complexity(self) -> int:
        return sum(_.complexity for _ in self.forwarders)

    def get_total_latency(self) -> float:
        return sum(fs.estimated_latency for fs in self.forwarders)

    def get_bandwidth(self) -> float:
        return min(fs.estimated_bandwidth for fs in self.forwarders)


@dataclass
class TopicRef:
    unique_id: SourceID  # unique id for the stream
    origin_node: NodeID  # unique id of the node that created the stream
    app_static_data: Optional[dict[str, object]]

    reachability: list[TopicReachability]
    #
    # forwarders: list[str]
    # urls: list[URLString]
    #


@dataclass
class TopicsIndex:
    node_id: NodeID

    topics: dict[TopicName, TopicRef]

    @classmethod
    def from_json(cls, s: object) -> "TopicsIndex":
        return parse_obj_as(TopicsIndex, s)


# used in websockets
@dataclass
class DataReady:
    sequence: int

    digest: str
    urls: list[URLString]

    @classmethod
    def from_json_string(cls, s: str) -> "DataReady":
        return parse_obj_as(DataReady, json.loads(s))


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

        return hashlib.sha256(self.content).hexdigest()
