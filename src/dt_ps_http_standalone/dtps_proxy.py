import argparse
import asyncio
import json
from dataclasses import asdict, replace

from pydantic.dataclasses import dataclass

from dt_ps_http import (
    async_error_catcher,
    DTPSClient,
    DTPSServer,
    interpret_command_line_and_start,
    join,
    join_topic_names,
    parse_url_unescape,
    RawData,
    TOPIC_LIST,
    TopicName,
    TopicReachability,
    URL,
    URLString,
)
from . import logger

__all__ = [
    "dtps_proxy_main",
]


@dataclass
class ProxyNamed:
    index_url: URLString
    topic_name: TopicName


@dataclass
class ProxyConfig:
    proxied: dict[TopicName, ProxyNamed]


@async_error_catcher
async def go_proxy(args: list[str] = None) -> None:
    parser = argparse.ArgumentParser()

    # default_hostname = socket.gethostname()

    #
    # print(f"addresses}")
    # print(f"ipv6s={ipv6s!r}")
    parser.add_argument("--add-prefix", type=str, default="proxied", required=False)
    parser.add_argument("--url", required=True)

    parsed, args = parser.parse_known_args(args)

    urlbase = parsed.url
    #
    #
    # proxied = {
    #     "robot1.clock": ProxyNamed(URLString("http://localhost:8081/"), as_TopicName("timekeeper.clock11")),
    #     "robot2.clock": ProxyNamed(URLString("http://localhost:8081/"), as_TopicName("timekeeper.clock5")),
    #     "robot3.clock": ProxyNamed(URLString("http://localhost:8081/"), as_TopicName("timekeeper.clock7")),
    # }

    dtps_server = DTPSServer(topics_prefix=(), on_startup=[])

    t = interpret_command_line_and_start(dtps_server, args)
    server_task = asyncio.create_task(t)

    never = asyncio.Event()
    previously_seen: set[TopicName] = set()
    async with DTPSClient.create() as dtpsclient:

        def new_observation(oq_, d: RawData) -> None:
            oq_.publish(d)

        search_queue = asyncio.Queue()

        def topic_list_changed(d: RawData) -> None:
            search_queue.put_nowait(f"topic list changed: {d}")

        publish_alternate: bool = False

        @async_error_catcher
        async def ask_for_topics() -> None:
            available = await dtpsclient.ask_topics(parse_url_unescape(urlbase))
            nonlocal previously_seen
            added = set(available) - previously_seen
            removed = previously_seen - set(available)
            previously_seen = set(available)
            if added or removed:
                logger.info(f"added={added!r} removed={removed!r}")
            else:
                logger.info(f"no change")
                return

            for topic_name in removed:
                new_topic = join_topic_names(parsed.add_prefix, topic_name)
                if new_topic in dtps_server._oqs:
                    logger.info("removing topic %s", new_topic)
                    await dtps_server.remove_oq(new_topic)
                    dtps_server.forward_events.pop(new_topic, None)

            for topic_name in added:
                tr = available[topic_name]
                new_topic = join_topic_names(parsed.add_prefix, topic_name)

                if new_topic in dtps_server._oqs:
                    logger.info("already have topic %s", new_topic)
                    continue

                possible: list[tuple[URL, TopicReachability]] = []
                for reachability in tr.reachability:
                    urlhere = URLString(f"topics/{new_topic}/")

                    more_urls = await dtpsclient.get_alternates(reachability.url)
                    for m in more_urls:
                        reach_with_me = await dtpsclient.compute_with_hop(
                            dtps_server.node_id,
                            urlhere,
                            connects_to=m,
                            expects_answer_from=reachability.answering,
                            forwarders=reachability.forwarders,
                        )

                        if reach_with_me is not None:
                            possible.append((parse_url_unescape(reachability.url), reach_with_me))

                if not possible:
                    logger.error(f"Topic {topic_name} not available at {urlbase}: {available}")
                    raise ValueError(f"Topic {topic_name} not available at {urlbase}: {available}")

                def choose_key(x: TopicReachability) -> tuple[int, float, float]:
                    return (x.get_total_complexity(), x.get_total_latency(), -x.get_bandwidth())

                possible.sort(key=lambda _: choose_key(_[1]))
                url_to_use, r = possible[0]

                if publish_alternate:
                    tr2 = replace(tr, reachability=tr.reachability + [r])
                else:
                    tr2 = replace(tr, reachability=[r])

                logger.info(
                    f"Proxying {new_topic!r} as  {urlbase} {topic_name} ->  \n"
                    f" available at\n: {json.dumps(asdict(tr), indent=2)} \n"
                    f" proxied at\n: {json.dumps(asdict(tr2), indent=2)} \n"
                )
                if topic_name == TOPIC_LIST:
                    await dtpsclient.listen_url(url_to_use, topic_list_changed)
                else:
                    oq = await dtps_server.get_oq(new_topic, tr2)
                    logger.info(f"adding topic {new_topic} -> {url_to_use}")
                    dtps_server.forward_events[new_topic] = join(url_to_use, URLString("events/"))
                    dtps_server.forward_data[new_topic] = url_to_use

                    # await dtpsclient.listen_url(url_to_use, functools.partial(new_observation, oq))

        @async_error_catcher
        async def ask_for_topics_continuous() -> None:
            while True:
                delay = 10
                try:
                    msg = await asyncio.wait_for(search_queue.get(), delay)
                except TimeoutError:
                    msg = f"timeout after {delay}"
                logger.info(f"performing search: {msg}")
                try:
                    await ask_for_topics()
                except:
                    pass

        t = asyncio.create_task(ask_for_topics_continuous())
        search_queue.put_nowait("initial")
        await never.wait()


def dtps_proxy_main(args: list[str] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Connects to DTPS server and listens and subscribes to all topics"
    )

    args, rest = parser.parse_known_args(args)
    # if len(rest) != 1:
    #     msg = f"Expected exactly one argument.\nObtained: {args!r}\n"
    #     logger.error(msg)
    #     sys.exit(2)

    # urlbase = rest[0]

    f = go_proxy()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(f)
