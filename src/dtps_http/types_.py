"""Types."""

__all__ = [
    "ContentType",
    "HTTPRequest",
    "HTTPResponse",
    "NodeID",
    "SourceID",
    "TopicNameS",
    "TopicNameV",
    "URLString",
]

from collections.abc import Sequence
from dataclasses import dataclass
from typing import NewType, Self

from aiohttp import web

ContentType = str
HTTPRequest = web.Request
HTTPResponse = web.Response
NodeID = NewType("NodeID", str)
SourceID = NewType("SourceID", str)
TopicNameS = NewType("TopicNameS", str)
URLString = NewType("URLString", str)


@dataclass(frozen=True)
class TopicNameV:
    """Topic name."""

    components: tuple[str, ...]

    def __add__(self, other: Self) -> "TopicNameV":
        """Addition."""
        return TopicNameV(self.components + other.components)

    def __ge__(self, other: "TopicNameV") -> bool:
        """Greater than or equal to."""
        return self.components >= other.components

    def __gt__(self, other: "TopicNameV") -> bool:
        """Greater than."""
        return self.components > other.components

    def __le__(self, other: "TopicNameV") -> bool:
        """Less than or equal to."""
        return self.components <= other.components

    def __lt__(self, other: "TopicNameV") -> bool:
        """Less than."""
        return self.components < other.components

    def __post_init__(self) -> None:
        """Run post topic name initialization."""
        for component in self.components:
            if "/" in component:
                message = f"Invalid component {component!r} in {self!r}."
                raise ValueError(message)

    def __str__(self) -> str:
        """Return string representation of topic name."""
        dash_separated_self = self.as_dash_sep()
        return f"Topic({dash_separated_self!r})"

    def as_dash_sep(self) -> TopicNameS:
        """Return either "" or `a/b/c` (without ending `/`)."""
        if not self.components:
            return TopicNameS("")
        dash_separated_topic_name = "/".join(self.components)
        return TopicNameS(dash_separated_topic_name)

    def as_relative_url(self) -> URLString:
        """Return either "" or `a/b/c/` (with ending `/`)."""
        if not self.components:
            return URLString("")
        relative_url = "/".join(self.components) + "/"
        return URLString(relative_url)

    @classmethod
    def from_components(cls, components: Sequence[str], /) -> "TopicNameV":
        """Return topic name from components."""
        components_tuple = tuple(components)
        return cls(components_tuple)

    @classmethod
    def from_dash_sep(cls, dash_separated_topic_name: str) -> "TopicNameV":
        """Return topic name from dash separated topic name."""
        if not dash_separated_topic_name:
            return cls.root()
        if dash_separated_topic_name.endswith("/"):
            message = f"{dash_separated_topic_name!r} ends with '/'."
            raise ValueError(message)
        split_dash_separated_topic_name = dash_separated_topic_name.split("/")
        split_dash_separated_topic_name_tuple = tuple(
            split_dash_separated_topic_name,
        )
        return cls(split_dash_separated_topic_name_tuple)

    @classmethod
    def from_dash_sep_or_none(cls, s: str | None) -> "TopicNameV":
        """Like from_dash_sep, but it treats None as root."""
        if s is None:
            return cls.root()
        return cls.from_dash_sep(s)

    @classmethod
    def from_relative_url(cls, relative_url: str) -> "TopicNameV":
        """Return topic name from relative URL.

        `relative_url` is either "" or `a/b/c/` (with ending `/`).
        """
        if not relative_url or relative_url == "/":
            return cls.root()
        if relative_url.startswith("/"):
            message = f"{relative_url!r} starts with '/'."
            raise ValueError(message)
        if not relative_url.endswith("/"):
            message = f"{relative_url!r} does not end with '/'."
            raise ValueError(message)
        relative_url = relative_url[:-1]
        split_relative_url = relative_url.split("/")
        components = tuple(split_relative_url)
        return cls(components)

    def is_prefix_of(
        self,
        other: Self,
    ) -> tuple[tuple[str, ...], tuple[str, ...]] | None:
        """Return (prefix, rest)."""
        components_length = len(self.components)
        if components_length > len(other.components):
            return None
        for i in range(components_length):
            if self.components[i] != other.components[i]:
                return None
        return self.components, other.components[components_length:]

    def is_root(self) -> bool:
        """Return `True` if `self` is `root`, `False` otherwise."""
        return not self.components

    def nontrivial_prefixes(self) -> "Sequence[TopicNameV]":
        """Return nontrivial prefixes."""
        components_length = len(self.components)
        topic_names = []
        for i in range(1, components_length):
            topic_name = TopicNameV(self.components[:i])
            topic_names.append(topic_name)
        return topic_names

    @classmethod
    def root(cls) -> "TopicNameV":
        """Return `root`."""
        return cls(())
