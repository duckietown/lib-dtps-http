from dataclasses import dataclass
from typing import NewType, Optional, Sequence, Tuple

from typing_extensions import Self

__all__ = [
    "ContentType",
    "NodeID",
    "SourceID",
    "TopicNameS",
    "TopicNameV",
    "URLString",
]

URLString = NewType("URLString", str)
NodeID = NewType("NodeID", str)
SourceID = NewType("SourceID", str)

TopicNameS = NewType("TopicNameS", str)

# ContentType = NewType("ContentType", str)
ContentType = str


@dataclass(frozen=True)
class TopicNameV:
    components: Tuple[str, ...]

    def __post_init__(self):
        for c in self.components:
            if "/" in c:
                raise ValueError(f"Invalid component {c!r} in {self!r}")

    def as_relative_url(self) -> URLString:
        """returns either "" or a/b/c/ (with ending /)"""
        if not self.components:
            return URLString("")
        else:
            return URLString("/".join(self.components) + "/")

    def as_dash_sep(self) -> TopicNameS:
        """returns either "" or a/b/c (without ending /)"""
        if not self.components:
            return TopicNameS("")
        else:
            return TopicNameS("/".join(self.components))

    def __str__(self) -> str:
        return f"Topic({self.as_dash_sep()!r})"
        # raise AssertionError("use as_relative_url()")

    @classmethod
    def root(cls) -> "TopicNameV":
        return cls(())

    def is_root(self) -> bool:
        return not self.components

    @classmethod
    def from_dash_sep(cls, s: str) -> "TopicNameV":
        if not s:
            return cls.root()
        if s.endswith("/"):
            raise ValueError(f"{s!r} ends with /")
        return cls(tuple(s.split("/")))

    @classmethod
    def from_dash_sep_or_none(cls, s: Optional[str]) -> "TopicNameV":
        """Like from_dash_sep, but it treats None as root."""
        if s is None:
            return cls.root()
        else:
            return cls.from_dash_sep(s)

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

    def is_prefix_of(self, other: Self) -> Optional[Tuple[Tuple[str, ...], Tuple[str, ...]]]:
        """returns (prefix, rest)"""
        if len(self.components) > len(other.components):
            return None

        for i in range(len(self.components)):
            if self.components[i] != other.components[i]:
                return None

        return self.components, other.components[len(self.components) :]

    def __add__(self, other: Self) -> "TopicNameV":
        return TopicNameV(self.components + other.components)

    def nontrivial_prefixes(self) -> "Sequence[TopicNameV]":
        return [TopicNameV(self.components[:i]) for i in range(1, len(self.components))]

    def __lt__(self, other):
        return self.components < other.components

    def __le__(self, other):
        return self.components <= other.components

    def __gt__(self, other):
        return self.components > other.components

    def __ge__(self, other):
        return self.components >= other.components
