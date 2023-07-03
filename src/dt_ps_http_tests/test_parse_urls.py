from urllib3.util import parse_url

from dt_ps_http import parse_url_unescape

url1 = "http+unix://%2Ftmp%2Fmine/topics/clock3/data/3"


def test_parse_urls1() -> None:
    parsed = parse_url(url1)
    print(repr(parsed))
    assert parsed.scheme == "http+unix"
    assert parsed.host == "%2Ftmp%2Fmine"
    assert parsed.port is None
    assert parsed.path == "/topics/clock3/data/3"
    assert parsed.query is None


def test_parse_url2() -> None:
    parsed = parse_url_unescape(url1)
    print(repr(parsed))
    assert parsed.scheme == "http+unix"
    assert parsed.host == "/tmp/mine"
    assert parsed.port is None
    assert parsed.path == "/topics/clock3/data/3"
    assert parsed.query is None
