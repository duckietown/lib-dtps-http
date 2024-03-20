import asyncio
import gc
import random
import string

from dtps_http import (
    ContentInfo,
    DTPSServer,
    interpret_command_line_and_start,
    MIME_OCTET,
    RawData,
    TopicNameV,
)
from dtps_http.structures import Bounds
from dtps_http.utils_every_once_in_a_while import EveryOnceInAWhile


def generate_random_string(n):
    """Generate a random string of length n."""
    # Choose characters from uppercase, lowercase letters, and digits
    characters = string.ascii_letters + string.digits
    # Generate the random string
    random_string = "".join(random.choice(characters) for _ in range(n))
    return random_string


async def async_main():
    s = DTPSServer.create(on_startup=[], enable_clock=False)
    args = ["--unix-path", "/tmp/test_memory.sock"]
    t = asyncio.create_task(interpret_command_line_and_start(s, args))
    await asyncio.sleep(1)

    oq = await s.create_oq(
        TopicNameV.from_dash_sep("topic1"),
        content_info=ContentInfo.simple(MIME_OCTET),
        tp=None,
        bounds=Bounds.max_length(2),
    )

    from pympler import muppy, summary

    every_once = EveryOnceInAWhile(10)
    nbytes = 0
    nmessages = 0
    bm = s.blob_manager
    publish_period = 0.001
    # all_objects = muppy.get_objects()
    from pympler import tracker

    tr = tracker.SummaryTracker()
    random_string0 = generate_random_string(1024 * 1024).encode("utf-8")

    while True:
        await asyncio.sleep(publish_period)
        # create random string and publish it
        if True:
            random_string = random_string0 + f"{nmessages:10d}".encode("utf-8")
            nbytes += len(random_string)
            rd = RawData(content=random_string, content_type=MIME_OCTET)
            await oq.publish(rd)
            nmessages += 1

        if every_once.now():
            bm.cleanup_blobs()

            obs = gc.get_objects()
            for o in obs:
                if o is obs:
                    continue
                if isinstance(o, (type, bytes)):
                    continue
                try:
                    l = len(o)
                except:
                    pass
                else:
                    if l > 500:
                        print(f"found {type(o)} of length {l}")
                        if l > 50000:
                            print("first element", o[0])
                            print("last element", o[-1])

                if isinstance(o, dict):
                    if len(o) > 1500:
                        first_keys = list(o)[:4]
                        last_keys = list(o)[-4:]
                        print(
                            f"found dict of length {len(o)}; first_keys={first_keys}; last_keys={last_keys}"
                        )

                        r2 = gc.get_referrers(o)
                        r2 = [_ for _ in r2 if _ is not obs and _ is not o]
                        r2_Typed = [type(_) for _ in r2]
                        print(f"referrences to dict: {r2_Typed}")
            obs = None

            print(f"Pushed {nmessages} with {nbytes / 1024 / 1024:.1f} MB")

            print(f"{len(oq.saved)=}")
            print(f"{len(oq.stored)=}")
            print(f"{len(bm.blobs)=}")
            print(f"{len(bm.blobs_forgotten)=}")

            if True:
                # queue_sizes = s.hub.get_all_subscriber_queue_sizes()
                # print(f"{queue_sizes=}")
                #
                # while True:
                #     without_outcome = {k: v for k, v in Global.active.items() if v.co.outcome is None}
                #
                #     logger.info(
                #         "task stats ",
                #         created=len(Global.created),
                #         nactive=len(Global.active),
                #         nnotfinished=len(without_outcome),
                #     )
                #     logger.info(joinlines("/".join(_) for _ in without_outcome))
                #     logger.info("checking memory")
                print("New objects since last time")
                tr.print_diff()

                all_objects_now = muppy.get_objects()

                sum1 = summary.summarize(all_objects_now)
                print("Current summary")
                summary.print_(sum1, limit=50)
                sum1 = None
                all_objects_now = None
                #
            # diff = summary.get_diff(all_objects, all_objects_now)
            # summary.print_(diff)
            # n = gc.collect()

            # all_objects = all_objects_now

            # print(bm.blobs)


def server_main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    server_main()
