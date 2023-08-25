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

    @classmethod
    def from_dash_sep(cls, s: str) -> "TopicNameV":
        if s.endswith("/"):
            raise ValueError(f"{s!r} ends with /")
        return cls(tuple(s.split("/")))

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

    def __add__(self, other: Self) -> Self:
        return TopicNameV(self.components + other.components)
