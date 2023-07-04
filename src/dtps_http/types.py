from typing import NewType

__all__ = [
    "NodeID",
    "SourceID",
    "TopicName",
    "URLString",
    "as_TopicName",
]

TopicName = NewType("TopicName", str)

URLString = NewType("URLString", str)
NodeID = NewType("NodeID", str)
SourceID = NewType("SourceID", str)


def as_TopicName(s: str) -> TopicName:
    # TODO: Check that s is a valid topic name
    return TopicName(s)
