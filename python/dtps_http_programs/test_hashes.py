import random
import string
import time

from dtps_http.structures import (
    get_digest_blake2b,
    get_digest_blake2s,
    get_digest_md5,
    get_digest_sha1,
    get_digest_sha256,
    get_digest_xxh128,
    get_digest_xxh32,
    get_digest_xxh64,
)


def generate_random_string(n):
    """Generate a random string of length n."""
    # Choose characters from uppercase, lowercase letters, and digits
    characters = string.ascii_letters + string.digits
    # Generate the random string
    random_string = "".join(random.choice(characters) for _ in range(n))
    return random_string


def compare_hashes_speed() -> None:
    hf = [
        get_digest_xxh128,
        get_digest_xxh64,
        get_digest_xxh32,
        get_digest_sha1,
        get_digest_sha256,
        get_digest_blake2b,
        get_digest_blake2s,
        get_digest_md5,
    ]
    ns = [100, 10000, 1000000, 10000000]
    repeats = [100000, 10000, 1000, 100]
    datas = [generate_random_string(n).encode() for n in ns]
    for n, data, repeat in zip(ns, datas, repeats):
        print()
        print(f"input length: {n}, repeating {repeat} times")
        for h in hf:
            t0 = time.monotonic()
            t0_ns = time.time_ns()
            nbytes = 0
            adigest = h(data)
            _, _, adigest = adigest.partition(":")

            hash_length = len(bytes.fromhex(adigest)) * 8
            for _ in range(repeat):
                nbytes += len(data)
                h(data)
            dt = time.monotonic() - t0
            dt_ns = time.time_ns() - t0_ns
            per_byte = dt_ns / (nbytes)
            hash_name = h.__name__.ljust(20).replace("get_digest_", "")
            print(
                f"  {hash_name} digest length {hash_length:4} bits |  time {dt:10.5f} s total    {per_byte:10.3} ns/byte"
            )


if __name__ == "__main__":
    compare_hashes_speed()
