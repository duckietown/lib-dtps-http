from typing import cast

from mkdocs.utils import get_relative_url
from urllib3.util import parse_url

from dtps_http import join, parse_url_unescape, url_to_string, URLString

url1 = cast(URLString, "http+unix://%2Ftmp%2Fmine/topics/clock3/data/3?debug=1")


def test_parse_urls1() -> None:
    """The standard library's urllib.parse.urlparse() does not unescape the host part of the URL."""
    parsed = parse_url(url1)
    print(repr(parsed))
    assert parsed.scheme == "http+unix"
    assert parsed.host == "%2Ftmp%2Fmine"
    assert parsed.port is None
    assert parsed.path == "/topics/clock3/data/3"
    assert parsed.query == "debug=1"


def test_parse_url2() -> None:
    """We have a custom function that does unescape the host part of the URL."""
    parsed = parse_url_unescape(url1)
    print(repr(parsed))
    assert parsed.scheme == "http+unix"
    assert parsed.host == "/tmp/mine"
    assert parsed.port is None
    assert parsed.path == "/topics/clock3/data/3"
    assert parsed.query == "debug=1"


def test_parse_url3() -> None:
    """We have a custom function that does unescape the host part of the URL."""
    url3 = URLString("http://localhost/")
    parsed = parse_url_unescape(url3)
    joined = join(parsed, "/topic?debug=1")
    print(repr(joined))
    assert joined.scheme == "http"
    assert joined.host == "localhost"
    assert joined.port is None
    assert joined.path == "/topic"
    assert joined.query == "debug=1"


def test_parse_url4() -> None:
    """We have a custom function that does unescape the host part of the URL."""
    url3 = URLString("")
    parsed = parse_url_unescape(url3)
    print(repr(parsed))
    print(f"{url_to_string(parsed)=!r}")


def test_relative_urls1() -> None:
    assert get_relative_url("a/b/", "a/") == "b/"


def test_relative_urls2() -> None:
    assert get_relative_url("a/", "a/b/") == "../"
