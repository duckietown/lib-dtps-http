"""Publisher-subscriber subscriber."""

import argparse
import asyncio

from utils import read_continuous

from dtps_http import (
    URLString,
    parse_url_unescape,
)


def subscribe_main() -> None:
    """Subscribe."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="Topic URL")
    parsed = parser.parse_args()
    # Use `parse_url_unescape` to handle special unix socket URLs with
    # escaped slashes.
    url_string = URLString(parsed.url)
    url = parse_url_unescape(url_string)
    coroutine = read_continuous(url)
    asyncio.run(coroutine)


if __name__ == "__main__":
    subscribe_main()
