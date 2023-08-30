from dataclasses import dataclass
from typing import NewType, Optional, Self, Sequence

__all__ = [
    "ContentType",
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


ContentType = NewType("ContentType", str)


@dataclass(frozen=True)
class TopicNameV:
    components: tuple[str, ...]

    def as_relative_url(self) -> TopicNameS:
        """returns either "" or a/b/c/ (with ending /)"""
        if not self.components:
            return TopicNameS("")
        else:
            return TopicNameS("/".join(self.components) + "/")

    def as_dash_sep(self) -> TopicNameS:
        """returns either "" or a/b/c (without ending /)"""
        if not self.components:
            return TopicNameS("")
        else:
            return TopicNameS("/".join(self.components))

    def __str__(self) -> str:
        raise AssertionError("use as_relative_url()")

    @classmethod
    def root(cls) -> "TopicNameV":
        return cls(())

    def is_root(self) -> bool:
        return not self.components

    @classmethod
    def from_dash_sep(cls, s: str) -> "TopicNameV":
        if s.endswith("/"):
            raise ValueError(f"{s!r} ends with /")
        return cls(tuple(s.split("/")))

    @classmethod
    def from_components(cls, c: Sequence[str], /) -> "TopicNameV":
        return cls(tuple(c))

    @classmethod
    def from_relative_url(cls, s: str) -> "TopicNameV":
        """s is either "" or a/b/c/ (with ending /)"""
        if not s or s == "/":
            return cls.root()

        if s.startswith("/"):
            raise ValueError(f"{s!r} starts with /")

        if not s.endswith("/"):
            msg = f"{s!r} does not end with /"
            raise ValueError(msg)

        s = s[:-1]

        components = tuple(s.split("/"))

        return cls(components)

    def is_prefix_of(self, other: Self) -> Optional[tuple[tuple[str, ...], tuple[str, ...]]]:
        """returns (prefix, rest)"""
        if len(self.components) > len(other.components):
            return None

        for i in range(len(self.components)):
            if self.components[i] != other.components[i]:
                return None

        return self.components, other.components[len(self.components) :]

    def __add__(self, other: Self) -> Self:
        return TopicNameV(self.components + other.components)

    def nontrivial_prefixes(self) -> Sequence[Self]:
        return [TopicNameV(self.components[:i]) for i in range(1, len(self.components))]
