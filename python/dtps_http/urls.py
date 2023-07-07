import os
from typing import cast, NamedTuple, NewType, Optional, TYPE_CHECKING
from urllib.parse import unquote

from urllib3.util import parse_url, Url

from .types import URLString
from . import logger

__all__ = [
    "URL",
    "URLIndexer",
    "URLTopic",
    "URLWS",
    "URLWSInline",
    "URLWSOffline",
    "join",
    "parse_url_unescape",
    "url_to_string",
]

if TYPE_CHECKING:

    class URL(NamedTuple):
        scheme: str
        auth: Optional[str]
        host: str
        port: Optional[int]
        path: Optional[str]
        query: Optional[str]
        fragment: Optional[str]

else:
    from urllib3.util import Url as URL

URLIndexer = NewType("URLIndexer", URL)
URLTopic = NewType("URLTopic", URL)
URLWS = NewType("URL_WEBSOCKETS", URL)
URLWSInline = NewType("URLWSInline", URLWS)
URLWSOffline = NewType("URLWSOffline", URLWS)


def quote(s: str) -> str:
    return s.replace("/", "%2F")


def parse_url_unescape(s: URLString) -> URL:
    parsed = parse_url(s)
    if parsed.path is None:
        logger.warning(f"parse_url_unescape: path is None: {s!r}")
        path = "/"
    else:
        path = parsed.path
    res = Url(
        scheme=parsed.scheme,
        host=unquote(parsed.host) if parsed.host is not None else None,
        port=parsed.port,
        path=path,  # unquote(parsed.path) if parsed.path is not None else None,
        query=parsed.query,
    )

    return res


def url_to_string(url: URL) -> URLString:
    # noinspection PyProtectedMember
    url2 = url._replace(host=quote(url.host) if url.host is not None else None)
    return cast(URLString, str(url2))


def join(url: URL, path0: str) -> URL:
    if "?" in path0:
        path0, _, query = path0.partition("?")
    else:
        query = None

    if "://" in path0:
        return parse_url_unescape(cast(URLString, path0))
    if url.path is None:
        path = path0
    else:
        path = os.path.normpath(os.path.join(url.path, path0))
    if path0.endswith("/"):
        path += "/"
    # noinspection PyProtectedMember
    res = url._replace(path=path, query=query)
    # print(f'join {url!r} {path0!r} -> {res!r}')
    return res
