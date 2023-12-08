from dtps_http.urls import join, make_http_unix_url, parse_url_unescape, url_to_string


def test_join_url1() -> None:
    fn = "/tmp/sockets/name"
    path = "/path/to/resource"

    url = make_http_unix_url(fn, path)
    print(f"url: {url!r}")
    urls = url_to_string(url)
    print(f"urls: {urls!r}")
    url2 = parse_url_unescape(urls)
    print(f"urls: {url2!r}")
    if url != url2:
        raise AssertionError(f"\nurl = {url!r} !=\nurl2= {url2!r}")

    rel = "a/b/c"
    url_composed = join(url, rel)
    print(f"url_composed: {url_composed!r}")
    url_composed_s = url_to_string(url_composed)
    print(f"url_composed_s: {url_composed_s}")
