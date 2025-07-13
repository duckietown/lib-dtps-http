"""URLs."""

__all__ = [
    "URL",
    "URLWS",
    "URLIndexer",
    "URLTopic",
    "URLWSInline",
    "URLWSOffline",
    "get_relative_url",
    "join",
    "make_http_unix_url",
    "parse_url_unescape",
    "url_to_string",
]

import functools
import os
import posixpath
from pathlib import Path
from typing import NamedTuple, NewType, cast
from urllib.parse import unquote

from urllib3.util import Url, parse_url

from dtps_http.types_ import URLString


class URL(NamedTuple):
    """URL."""

    scheme: str
    auth: str | None
    host: str
    port: int | None
    path: str | None
    query: str | None
    fragment: str | None


def quote(string: str) -> str:
    return string.replace("/", "%2F")


def make_http_unix_url(socket_path: str, url_path: str | None = None) -> URL:
    """Return http+unix URL."""
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
    """Parse URL."""
    parsed = parse_url(s)
    path = "/" if parsed.path is None else parsed.path
    host = unquote(parsed.host) if parsed.host is not None else None
    res = Url(
        scheme=parsed.scheme,
        host=host,
        port=parsed.port,
        path=path,
        query=parsed.query,
    )
    if res.scheme == "http+unix" and res.host is None:
        message = (
            f"This url seems invalid: Expected to have a non-null host:\n{s!r}"
        )
        raise ValueError(message)
    return cast(URL, res)


def url_to_string(url: URL) -> URLString:
    """Return URL as string."""
    if not isinstance(url, URL):
        raise TypeError
    host = quote(url.host) if url.host is not None else None
    url2 = url._replace(host=host)
    if not url2.scheme and not url2.host and url2.port is None:
        url_path = url2.path or "/"
        if url2.query is not None:
            url_path += "?" + url2.query
        return URLString(url_path)
    url2_string = str(url2)
    res = cast(URLString, url2_string)
    parse_url_unescape(res)
    return res


def join(url: URL, path0: str) -> URL:
    """Join."""
    if "?" in path0:
        path0, _, query = path0.partition("?")
    else:
        query = None
    if "://" in path0:
        path_string = cast(URLString, path0)
        return parse_url_unescape(path_string)
    if url.path is None:
        path = path0
    else:
        url_path = Path(url.path) / path0
        path = url_path.as_posix()
        path = os.path.normpath(path)
    if path0.endswith("/"):
        path += "/"
    return url._replace(path=path, query=query)


@functools.lru_cache(maxsize=128)
def _norm_parts(path: str) -> list[str]:
    if not path.startswith("/"):
        path = "/" + path
    path = posixpath.normpath(path)
    path = path[1:]
    return path.split("/") if path else []


def get_relative_url(url: str, other: str) -> URLString:
    """Return URL relative to the other.

    Both are operated as slash-separated paths, similar to the "path"
    part of a URL. The last component of `other` is skipped if it
    contains a dot (considered a file). Actual URLs (with schemas etc.)
    are not supported. The leading slash is ignored. Paths are
    normalized (`..` works as the parent directory), but going higher
    than the root has no effect (`foo/../../bar` ends up just as `bar`).
    """
    # Remove filename from other url if it has one.
    dirname, _, basename = other.rpartition("/")
    if "." in basename:
        other = dirname
    other_parts = _norm_parts(other)
    dest_parts = _norm_parts(url)
    common = 0
    for a, b in zip(other_parts, dest_parts, strict=False):
        if a != b:
            break
        common += 1
    other_parts_length = len(other_parts)
    rel_parts = [".."] * (other_parts_length - common) + dest_parts[common:]
    relurl = "/".join(rel_parts) or "."
    res = relurl + "/" if url.endswith("/") else relurl
    if res == "./":
        res = ""
    return URLString(res)


URLIndexer = NewType("URLIndexer", URL)
URLTopic = URL
URLWS = NewType("URLWS", URL)
URLWSInline = NewType("URLWSInline", URLWS)
URLWSOffline = NewType("URLWSOffline", URLWS)
