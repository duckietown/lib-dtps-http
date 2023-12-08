import functools
import os
import posixpath
from typing import cast, List, NamedTuple, NewType, Optional, TYPE_CHECKING
from urllib.parse import unquote

from urllib3.util import parse_url, Url

from .types import URLString

__all__ = [
    "URL",
    "URLIndexer",
    "URLTopic",
    "URLWS",
    "URLWSInline",
    "URLWSOffline",
    "get_relative_url",
    "join",
    "make_http_unix_url",
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
# URLTopic = NewType("URLTopic", URL)
URLTopic = URL
URLWS = NewType("URLWS", URL)
URLWSInline = NewType("URLWSInline", URLWS)
URLWSOffline = NewType("URLWSOffline", URLWS)


def quote(s: str) -> str:
    return s.replace("/", "%2F")


def make_http_unix_url(socket_path: str, url_path: Optional[str] = None) -> URL:
    if url_path is None:
        url_path = "/"
    return URL(
        scheme="http+unix",
        host=socket_path,
        port=None,
        path=url_path,
        query=None,
        auth=None,
        fragment=None,
    )


def parse_url_unescape(s: URLString) -> URL:
    parsed = parse_url(s)
    if parsed.path is None:
        # logger.warning(f"parse_url_unescape: path is None: {s!r}")
        path = "/"
    else:
        path = parsed.path

    res = Url(  # type: ignore
        scheme=parsed.scheme,
        host=unquote(parsed.host) if parsed.host is not None else None,
        port=parsed.port,
        path=path,  # unquote(parsed.path) if parsed.path is not None else None,
        query=parsed.query,
    )

    if res.scheme == "http+unix" and res.host is None:
        msg = f"This url seems invalid: Expected to have a non-null host:\n  {s!r}"
        raise ValueError(msg)

    return cast(URL, res)


def url_to_string(url: URL) -> URLString:
    # noinspection PyProtectedMember
    assert isinstance(url, URL), type(url)
    url2 = url._replace(host=quote(url.host) if url.host is not None else None)

    if not url2.scheme and not url2.host and url2.port is None:
        s = url2.path or "/"
        if url2.query is not None:
            s += "?" + url2.query
        return URLString(s)

    res = cast(URLString, str(url2))
    parse_url_unescape(res)
    return res


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


@functools.lru_cache(maxsize=None)
def _norm_parts(path: str) -> List[str]:
    if not path.startswith("/"):
        path = "/" + path
    path = posixpath.normpath(path)[1:]
    return path.split("/") if path else []


def get_relative_url(url: str, other: str) -> URLString:
    """
    Return given url relative to other.

    Both are operated as slash-separated paths, similarly to the 'path' part of a URL.
    The last component of `other` is skipped if it contains a dot (considered a file).
    Actual URLs (with schemas etc.) aren't supported. The leading slash is ignored.
    Paths are normalized ('..' works as parent directory), but going higher than the
    root has no effect ('foo/../../bar' ends up just as 'bar').
    """
    # Remove filename from other url if it has one.
    dirname, _, basename = other.rpartition("/")
    if "." in basename:
        other = dirname

    other_parts = _norm_parts(other)
    dest_parts = _norm_parts(url)
    common = 0
    for a, b in zip(other_parts, dest_parts):
        if a != b:
            break
        common += 1

    rel_parts = [".."] * (len(other_parts) - common) + dest_parts[common:]
    relurl = "/".join(rel_parts) or "."
    res = relurl + "/" if url.endswith("/") else relurl
    if res == "./":
        res = ""
    return URLString(res)
