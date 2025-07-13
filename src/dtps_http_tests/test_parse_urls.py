"""Parse URL tests."""

from typing import cast

from urllib3.util import parse_url

from dtps_http import URLString, get_relative_url, join, parse_url_unescape
from dtps_http_tests import logger

url1 = cast(
    URLString,
    "http+unix://%2Ftmp%2Fmine/topics/clock3/data/3?debug=1",
)


def test_parse_urls1() -> None:
    """Run first parse URLs test."""
    parsed = parse_url(url1)
    if parsed.scheme != "http+unix":
        raise AssertionError
    if parsed.host != "%2Ftmp%2Fmine":
        raise AssertionError
    if parsed.port is not None:
        raise AssertionError
    if parsed.path != "/topics/clock3/data/3":
        raise AssertionError
    if parsed.query != "debug=1":
        raise AssertionError


def test_parse_url2() -> None:
    """Run second parse URLs test."""
    parsed = parse_url_unescape(url1)
    if parsed.scheme != "http+unix":
        raise AssertionError
    if parsed.host != "/tmp/mine":
        raise AssertionError
    if parsed.port is not None:
        raise AssertionError
    if parsed.path != "/topics/clock3/data/3":
        raise AssertionError
    if parsed.query != "debug=1":
        raise AssertionError


def test_parse_url3() -> None:
    """Run third parse URLs test."""
    url3 = URLString("http://localhost/")
    parsed = parse_url_unescape(url3)
    joined = join(parsed, "/topic?debug=1")
    if joined.scheme != "http":
        raise AssertionError
    if joined.host != "localhost":
        raise AssertionError
    if joined.port is not None:
        raise AssertionError
    if joined.path != "/topic":
        raise AssertionError
    if joined.query != "debug=1":
        raise AssertionError


def test_parse_url4() -> None:
    """Run fourth parse URLs test."""
    url3 = URLString("")
    parsed = parse_url_unescape(url3)
    parsed_string_representation = repr(parsed)
    logger.info(parsed_string_representation)


def test_relative_urls1() -> None:
    """Run first relative URLs test."""
    if get_relative_url("a/b/", "a/") != "b/":
        raise AssertionError


def test_relative_urls2() -> None:
    """Run second relative URLs test."""
    if get_relative_url("a/", "a/b/") != "../":
        raise AssertionError
