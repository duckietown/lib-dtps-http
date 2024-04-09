import asyncio
import math
import random
import string
import time

from dtps import context_cleanup
from dtps_http import (
    Bounds,
    MIME_OCTET,
    RawData,
)
from dtps_http.utils_every_once_in_a_while import EveryOnceInAWhile


def generate_random_string(n):
    """Generate a random string of length n."""
    # Choose characters from uppercase, lowercase letters, and digits
    characters = string.ascii_letters + string.digits
    # Generate the random string
    random_string = "".join(random.choice(characters) for _ in range(n))
    return random_string


async def async_main():
    every_once = EveryOnceInAWhile(2)
    nbytes = 0
    nmessages = 0
    publish_period = 0.01

    length = 1024 * 1024
    # length = 256

    random_string0 = generate_random_string(length).encode("utf-8")
    # cargo run --bin dtps-http-rs-server --release -- --tcp-port 8000 --unix-path /tmp/dashboard
    # cargo run --bin dtps-http-rs-server --release -- --tcp-port 8001 --unix-path /tmp/node
    environment = {
        # f"DTPS_BASE_SWITCHBOARD": f"http://localhost:8000",
        f"DTPS_BASE_SWITCHBOARD": f"http+unix://%2Ftmp%2Fdashboard/",
        f"DTPS_BASE_NODE": f"http+unix://%2Ftmp%2Fnode/",
    }
    nmax = 100000
    dts = []
    async with context_cleanup("node", environment) as node:
        async with context_cleanup("switchboard", environment) as context:
            bounds = Bounds.max_length(1)
            topic_orig = await (node / "topic1").queue_create(bounds=bounds)

            topic = await (context / "topic2").expose(topic_orig, mask_origin=True)

            nreceived = 0
            received_bytes = 0

            async def on_data(rd2: RawData) -> None:
                nonlocal nreceived, received_bytes
                nreceived += 1
                received_bytes += len(rd2.content)

                pass

            sub = await topic.subscribe(on_data)

            async with topic.publisher_context() as publisher:
                while True:
                    await asyncio.sleep(publish_period)

                    if nmessages <= nmax:
                        # create random string and publish it
                        random_string = random_string0 + f"{nmessages:10d}".encode("utf-8")
                        nbytes += len(random_string)
                        rd = RawData(content=random_string, content_type=MIME_OCTET)
                        t0 = time.monotonic()
                        await publisher.publish(rd)
                        dt = time.monotonic() - t0
                        dts.append(dt)
                        nmessages += 1

                    if every_once.now():
                        print(f"Pushed {nmessages} with {nbytes / 1024 / 1024:.1f} MB")

                        if len(dts) > 10:
                            last_dts = dts[-10:]
                            average = math.fsum(last_dts) / len(last_dts)
                            print(
                                f"average push delay: {average * 1000:.3f}ms",
                            )
                        print(f"Received {nreceived} messages with {received_bytes / 1024 / 1024:.1f} MB")


def server_main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    server_main()
