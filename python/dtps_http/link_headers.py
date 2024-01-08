from dataclasses import field
from typing import Dict, List, Optional, Union

from multidict import CIMultiDict, CIMultiDictProxy
from pydantic.dataclasses import dataclass

__all__ = [
    "LinkHeader",
    "get_link_headers",
    "put_link_header",
]


@dataclass
class LinkHeader:
    url: str
    rel: str
    attributes: Dict[str, str] = field(default_factory=dict)

    def to_header(self) -> str:
        s = f"<{self.url}>; rel={self.rel}"
        for k, v in self.attributes.items():
            s += f"; {k}={v}"
        return s

    @classmethod
    def parse(cls, header: str) -> "LinkHeader":
        pairs = header.split(";")
        if not pairs:
            raise ValueError
        first = pairs[0]
        if not first.startswith("<") or not first.endswith(">"):
            raise ValueError
        url = first[1:-1]

        attributes: Dict[str, str] = {}
        for p in pairs[1:]:
            p = p.strip()
            k, _, v = p.partition("=")
            k = k.strip()
            v = v.strip()
            attributes[k] = v

        rel = attributes.pop("rel", "")
        return cls(url=url, rel=rel, attributes=attributes)


def get_link_headers(h: Union[CIMultiDict[str], CIMultiDictProxy[str]]) -> Dict[str, LinkHeader]:
    res: Dict[str, LinkHeader] = {}
    default: List[str] = []
    for l in h.getall("Link", default):
        lh = LinkHeader.parse(l)
        res[lh.rel] = lh
    return res


def put_link_header(h: CIMultiDict[str], url: str, rel: str, content_type: Optional[str]):
    l = LinkHeader(url=url, rel=rel)
    if content_type is not None:
        l.attributes["type"] = content_type

    h.add("Link", l.to_header())
