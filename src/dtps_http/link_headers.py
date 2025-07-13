"""Link headers."""

__all__ = ["LinkHeader", "get_link_headers", "put_link_header"]

from dataclasses import field

from multidict import CIMultiDict, CIMultiDictProxy
from pydantic.dataclasses import dataclass


@dataclass
class LinkHeader:
    """Link header."""

    url: str
    rel: str
    attributes: dict[str, str] = field(default_factory=dict)

    def to_header(self) -> str:
        """Convert to header."""
        header = f"<{self.url}>; rel={self.rel}"
        for key, value in self.attributes.items():
            header += f"; {key}={value}"
        return header

    @classmethod
    def parse(cls, header: str) -> "LinkHeader":
        """Parse."""
        pairs = header.split(";")
        if not pairs:
            raise ValueError
        first = pairs[0]
        if not first.startswith("<") or not first.endswith(">"):
            raise ValueError
        url = first[1:-1]
        attributes: dict[str, str] = {}
        for pair in pairs[1:]:
            stripped_pair = pair.strip()
            key, _, value = stripped_pair.partition("=")
            key = key.strip()
            value = value.strip()
            attributes[key] = value
        rel = attributes.pop("rel", "")
        return cls(url, rel, attributes)


def get_link_headers(
    headers: CIMultiDict[str] | CIMultiDictProxy[str],
) -> dict[str, LinkHeader]:
    """Return link headers."""
    link_headers: dict[str, LinkHeader] = {}
    default: list[str] = []
    for header in headers.getall("Link", default):
        link_header = LinkHeader.parse(header)
        link_headers[link_header.rel] = link_header
    return link_headers


def put_link_header(
    headers: CIMultiDict[str],
    url: str,
    rel: str,
    content_type: str | None,
) -> None:
    """Put link header."""
    link_header = LinkHeader(url, rel)
    if content_type is not None:
        link_header.attributes["type"] = content_type
    header = link_header.to_header()
    headers.add("Link", header)
