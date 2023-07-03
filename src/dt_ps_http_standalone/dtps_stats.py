import argparse
import asyncio
import functools
import json
import sys
from dataclasses import asdict

from dt_ps_http import DTPSClient, RawData, URLString
from . import logger

__all__ = [
    "dtps_stats_main",
]


async def listen_to_all_topics(urlbase: str) -> None:
    def new_observation(topic_name: str, data: RawData) -> None:
        logger.info(f"new_observation {topic_name=} {data=}")

    never = asyncio.Event()
    async with DTPSClient.create() as dtpsclient:
        available = await dtpsclient.ask_topics(URLString(urlbase))

        for name, desc in available.items():
            # list_urls = "".join(f"\t{u} \n" for u in desc.urls)
            logger.info(
                f"Found topic {name!r}:\n"
                + json.dumps(asdict(desc), indent=2)
                + "\n"
                # + f"unique_id: {desc.unique_id}\n"
                # + f"origin_node: {desc.origin_node}\n"
                # + f"forwarders: {desc.forwarders}\n"
            )

            url = await dtpsclient.choose_best(desc.reachability)
            await dtpsclient.listen_url(url, functools.partial(new_observation, name))
            # for i in range(3):
            #     await dtpsclient.publish(name, RawData.simple_string(f"{name} {i}"))

        await never.wait()


def dtps_stats_main(args: list[str] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Connects to DTPS server and listens and subscribes to all topics"
    )

    args, rest = parser.parse_known_args(args)
    if len(rest) != 1:
        msg = f"Expected exactly one argument.\nObtained: {args!r}\n"
        logger.error(msg)
        sys.exit(2)

    urlbase = rest[0]

    f = listen_to_all_topics(urlbase)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(f)
