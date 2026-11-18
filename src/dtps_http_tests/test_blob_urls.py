from dtps_http.blob_manager import decode_content_type_segment, encode_url2


def _segment(url: str, digest: str) -> str:
    """Return what the ``{content_type_base64:.*}`` route capture would contain."""
    return url.split(f":blobs/{digest}/", 1)[1]


def test_blob_url_roundtrip_with_token() -> None:
    digest = "xxh128:e7f7dfec5d1adfba3253703e92be2fb6"
    url = encode_url2(digest, "application/json", "fe11df94-1003-4810-9c46-0b2c5d1e2f3a")  # type: ignore
    segment = _segment(url, digest)
    assert "/" in segment, segment
    assert decode_content_type_segment(segment) == "application/json"


def test_blob_url_roundtrip_without_token() -> None:
    assert decode_content_type_segment("YXBwbGljYXRpb24vanNvbg==") == "application/json"


def test_blob_url_roundtrip_content_type_with_plus() -> None:
    digest = "xxh128:0"
    content_type = "application/vnd.dt.dtps-index+cbor"
    url = encode_url2(digest, content_type, "token")  # type: ignore
    assert decode_content_type_segment(_segment(url, digest)) == content_type
