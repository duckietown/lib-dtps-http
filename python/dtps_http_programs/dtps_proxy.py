import argparse
import asyncio
import json
from dataclasses import asdict, replace
from typing import cast, Dict, List, Optional, Set, Tuple

from pydantic.dataclasses import dataclass

from dtps_http import (
    async_error_catcher,
    DTPSClient,
    DTPSServer,
    ForwardedTopic,
    interpret_command_line_and_start,
    parse_url_unescape,
    RawData,
    TOPIC_LIST,
    TopicNameV,
    TopicReachability,
    URL,
    URLIndexer,
    URLString,
)
from . import logger

__all__ = [
    "dtps_proxy_main",
]


@dataclass
class ProxyNamed:
    index_url: URLString
    topic_name: TopicNameV


@dataclass
class ProxyConfig:
    proxied: Dict[TopicNameV, ProxyNamed]


@async_error_catcher
async def go_proxy(args: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument("--add-prefix", type=str, default="proxied", required=False)
    parser.add_argument("--url", required=True)
    parser.add_argument("--mask-origin", default=False, action="store_true")

    parsed, args = parser.parse_known_args(args)

    urlbase = parsed.url
    mask_origin = parsed.mask_origin
    dtps_server = DTPSServer.create()

    t = interpret_command_line_and_start(dtps_server, args)
    server_task = asyncio.create_task(t)

    url0 = cast(URLIndexer, parse_url_unescape(urlbase))
    previously_seen: Set[TopicNameV] = set()
    use_prefix = TopicNameV.from_dash_sep(parsed.add_prefix)
    async with DTPSClient.create() as dtpsclient:
        search_queue: "asyncio.Queue[str]" = asyncio.Queue()

        def topic_list_changed(d: RawData) -> None:
            search_queue.put_nowait(f"topic list changed: {d}")

        @async_error_catcher
        async def ask_for_topics() -> None:
            available = await dtpsclient.ask_topics(url0)
            available.pop(TOPIC_LIST, None)
            nonlocal previously_seen
            added = set(available) - previously_seen

            removed = previously_seen - set(available)
            previously_seen = set(available)
            if added or removed:
                logger.debug(f"added={added!r} removed={removed!r}")
            else:
                logger.debug(f"no change in topics")
                return

            for topic_name in removed:
                new_topic = use_prefix + topic_name
                if dtps_server.has_forwarded(new_topic):
                    logger.debug("removing topic %s", new_topic)
                    await dtps_server.remove_forward(new_topic)

            for topic_name in added:
                tr = available[topic_name]
                new_topic = use_prefix + topic_name

                if dtps_server.has_forwarded(new_topic):
                    logger.debug("already have topic %s", new_topic)
                    continue

                possible: List[Tuple[URL, TopicReachability]] = []
                for reachability in tr.reachability:
                    urlhere = URLString(new_topic.as_relative_url())
                    rurl = parse_url_unescape(reachability.url)
                    metadata = await dtpsclient.get_metadata(rurl)
                    for m in metadata.alternative_urls + [rurl]:
                        reach_with_me = await dtpsclient.compute_with_hop(
                            dtps_server.node_id,
                            urlhere,
                            connects_to=m,
                            expects_answer_from=reachability.answering,
                            forwarders=reachability.forwarders,
                        )

                        if reach_with_me is not None:
                            try:
                                xx = parse_url_unescape(reach_with_me.url)
                            except Exception:
                                logger.error(f"Could not parse {reach_with_me.url!r}")
                                continue
                            else:
                                possible.append((xx, reach_with_me))
                        else:
                            pass  # logger.info(f"Could not proxy {new_topic!r} as {urlbase} {topic_name}
                            # -> {m}")

                if not possible:
                    logger.error(f"Topic {topic_name} cannot be reached")
                    raise ValueError(f"Topic {topic_name} not available at {urlbase}")

                def choose_key(x: TopicReachability) -> Tuple[int, float, float]:
                    return x.benchmark.complexity, x.benchmark.latency_ns, -x.benchmark.bandwidth

                possible.sort(key=lambda _: choose_key(_[1]))
                url_to_use, r = possible[0]

                if mask_origin:
                    tr2 = replace(tr, reachability=[r])
                else:
                    tr2 = replace(tr, reachability=tr.reachability + [r])

                logger.info(f"adding topic {new_topic} -> {url_to_use}")

                metadata_to_use = await dtpsclient.get_metadata(url_to_use)
                fd = ForwardedTopic(
                    unique_id=tr.unique_id,
                    origin_node=tr.origin_node,
                    app_data=tr.app_data,
                    reachability=tr2.reachability,
                    forward_url_data=url_to_use,
                    forward_url_events=metadata_to_use.events_url,
                    forward_url_events_inline_data=metadata_to_use.events_data_inline_url,
                    content_info=tr2.content_info,  # FIXME: content info
                    properties=tr2.properties,
                )

                logger.info(
                    f"Proxying {new_topic!r} as  {urlbase} {topic_name} ->  \n"
                    f" available at\n: {json.dumps(asdict(tr), indent=2)} \n"
                    f" proxied at\n: {json.dumps(asdict(fd), indent=2)} \n"
                )

                await dtps_server.add_forwarded(new_topic, fd)

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

        t2 = asyncio.create_task(
            dtpsclient.listen_topic(
                url0,
                TOPIC_LIST,
                topic_list_changed,
                inline_data=True,
                raise_on_error=False,
            )
        )
        # task_listen_to_all_topics = asyncio.ensure_future(asyncio.create_task(xt))

        await asyncio.gather(server_task, t, t2)


def dtps_proxy_main(args: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Connects to DTPS server and listens and subscribes to all topics"
    )

    parsed, rest = parser.parse_known_args(args)

    f = go_proxy(rest)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(f)
