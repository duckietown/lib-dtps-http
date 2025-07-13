"""Hashes test."""

import time

from dtps_http.structures import (
    get_digest_blake2b,
    get_digest_blake2s,
    get_digest_md5,
    get_digest_sha1,
    get_digest_sha256,
    get_digest_xxh32,
    get_digest_xxh64,
    get_digest_xxh128,
)
from python_benchmark_scripts import logger
from python_benchmark_scripts.utils import generate_random_string

HASH_FUNCTIONS = (
    get_digest_xxh128,
    get_digest_xxh64,
    get_digest_xxh32,
    get_digest_sha1,
    get_digest_sha256,
    get_digest_blake2b,
    get_digest_blake2s,
    get_digest_md5,
)
NUMBERS = (100, 10000, 1000000, 10000000)
REPEAT_NUMBERS = (100000, 10000, 1000, 100)


def compare_hash_speeds() -> None:
    """Compare hash speeds."""
    data = []
    for number in NUMBERS:
        random_string = generate_random_string(number)
        encoded_random_string = random_string.encode()
        data.append(encoded_random_string)
    for number, datum, repeat_number in zip(
        NUMBERS,
        data,
        REPEAT_NUMBERS,
        strict=False,
    ):
        logger.info(
            "input length: %s, repeating %s times.",
            number,
            repeat_number,
        )
        for hash_function in HASH_FUNCTIONS:
            start_time = time.monotonic()
            start_time_ns = time.time_ns()
            number_of_bytes = 0
            digest = hash_function(datum)
            _, _, digest_string = digest.partition(":")
            digest_data = bytes.fromhex(digest_string)
            hash_length = len(digest_data) * 8
            for _ in range(repeat_number):
                number_of_bytes += len(datum)
                hash_function(datum)
            delta_time = time.monotonic() - start_time
            delta_time_ns = time.time_ns() - start_time_ns
            per_byte = delta_time_ns / (number_of_bytes)
            left_justified_hash_name = hash_function.__name__.ljust(20)
            hash_name = left_justified_hash_name.replace("get_digest_", "")
            message = (
                "  %s digest length %s bits |  time %10.5f s total    %10.3f "
                "ns/byte"
            )
            logger.info(message, hash_name, hash_length, delta_time, per_byte)


if __name__ == "__main__":
    compare_hash_speeds()
