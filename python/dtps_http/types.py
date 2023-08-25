from dataclasses import dataclass
from typing import NewType, Self

__all__ = [
    "NodeID",
    "SourceID",
    "TopicNameS",
    "TopicNameV",
    "URLString",
]

# TopicName = NewType("TopicName", str)

URLString = NewType("URLString", str)
NodeID = NewType("NodeID", str)
SourceID = NewType("SourceID", str)


TopicNameS = NewType("TopicNameS", str)


@dataclass(frozen=True)
class TopicNameV:
    components: tuple[str, ...]

    def as_relative_url(self) -> TopicNameS:
        return TopicNameS("/".join(self.components))

    def __str__(self) -> str:
        raise AssertionError("use as_relative_url()")

    @classmethod
    def root(cls) -> "TopicNameV":
        return cls(())

    @classmethod
    def from_relative_url(cls, s: str) -> "TopicNameV":
        if s.endswith("/"):
            s = s[:-1]
        if s.startswith("/"):
            s = s[1:]

        if not s or s == "/":
            components = ()
        else:
            components = tuple(s.split("/"))

        return cls(components)

    def __add__(self, other: Self) -> Self:
        return TopicNameV(self.components + other.components)
