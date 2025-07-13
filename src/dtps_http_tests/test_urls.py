"""URL tests."""

from dtps_http.urls import (
    join,
    make_http_unix_url,
    parse_url_unescape,
    url_to_string,
)
from dtps_http_tests import logger


def test_join_url() -> None:
    """Run join URL test."""
    fn = "/tmp/sockets/name"
    path = "/path/to/resource"
    url = make_http_unix_url(fn, path)
    urls = url_to_string(url)
    url2 = parse_url_unescape(urls)
    if url != url2:
        message = f"\nurl = {url!r} !=\nurl2= {url2!r}"
        raise AssertionError(message)
    rel = "a/b/c"
    url_composed = join(url, rel)
    url_composed_string = url_to_string(url_composed)
    logger.info(url_composed_string)
