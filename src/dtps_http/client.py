import asyncio
import os
from contextlib import asynccontextmanager, AsyncExitStack
from dataclasses import dataclass, replace
from typing import Any, AsyncContextManager, AsyncIterator, Callable, cast, Optional, TYPE_CHECKING

import aiohttp
import methodtools as methodtools
from aiohttp import ClientConnectorError, TCPConnector, UnixConnector, WSMessage

from . import logger
from .constants import HEADER_NODE_ID, HEADER_SEE_EVENTS
from .exceptions import EventListeningNotAvailable
from .structures import (
    DataReady,
    ForwardingStep,
    NodeID,
    RawData,
    TopicName,
    TopicReachability,
    TopicRef,
    TopicsIndex,
)
from .types import URLString
from .urls import join, parse_url_unescape, URL, url_to_string
from .utils import async_error_catcher

__all__ = [
    "DTPSClient",
]


# @dataclass
# class AvailableTopic:
#     urls: list[str | URL]
#     description: Optional[str]


@dataclass
class LinkBenchmark:
    complexity: int  # 0 for local, 1 for using named, +2 for each network hop
    bandwidth: float  # B/s
    latency: float  # seconds


@dataclass
class AskTopicResult:
    alternative_urls: list[URLString]
    available: dict[TopicName, TopicRef]


class DTPSClient:
    if TYPE_CHECKING:

        @classmethod
        def create(cls) -> "AsyncContextManager[DTPSClient]":
            ...

    else:

        @classmethod
        @asynccontextmanager
        async def create(cls) -> "AsyncIterator[DTPSClient]":
            ob = cls()
            await ob.init()
            try:
                yield ob
            finally:
                await ob.aclose()

    # urlbase: URL

    def __init__(self) -> None:
        # assert isinstance(urlbase, (str, URL)), urlbase
        # if isinstance(urlbase, str):
        #     urlbase = parse_url_unescape(urlbase)
        # self.urlbase = urlbase
        self.S = AsyncExitStack()
        # self.available = {}
        self.tasks = []
        self.sessions = {}
        self.preferred_cache = {}
        self.blacklist_protocol_host_port = set()
        self.obtained_answer = {}

    blacklist_protocol_host_port: set[tuple[str, str, int]]
    obtained_answer: dict[tuple[str, str, int], Optional[NodeID]]

    preferred_cache: dict[URL, URL]
    # available: dict[TopicName, AvailableTopic]
    sessions: dict[str, aiohttp.ClientSession]

    async def init(self) -> None:
        pass
        # x = aiohttp.ClientSession()
        # self.session = await self.S.enter_async_context(x)

    async def aclose(self) -> None:
        for t in self.tasks:
            t.cancel()
        await self.S.aclose()

    #
    # async def list_topics(self) -> list[TopicName]:
    #     # if self.available is None:
    #     #     await self.ask_topics(url)
    #     return list(self.available.keys())

    # async def ask_topics(self, url: URL | str, add_prefix: str ='') -> None:
    #     use_url = self._look_cache(url)
    #     return await self._ask_topics(use_url, add_prefix)
    #
    # async def get_topic_url(self, index_url: URL | URLString, topic_name: TopicName) -> URLString:
    #     use_url = self._look_cache(index_url)
    #     available = await self.ask_topics(use_url)
    #     if topic_name not in available:
    #         raise NoSuchTopic(topic_name)
    #     return available[topic_name].urls[0]

    async def ask_topics(self, url: URL) -> dict[TopicName, TopicRef]:
        url = self._look_cache(url)
        async with self._session(url) as (session, use_url):
            async with session.get(use_url) as resp:
                resp.raise_for_status()
                answering = resp.headers.get(HEADER_NODE_ID)
                # logger.debug(f"ask topics {resp.headers}")
                if (preferred := await self.prefer_alternative(url, resp)) is not None:
                    logger.info(f"Using preferred alternative to {url} -> {repr(preferred)}")
                    return await self.ask_topics(preferred)
                assert resp.status == 200, resp.status
                res = await resp.json()

            alternatives0 = cast(list[URLString], resp.headers.getall("Content-Location", []))
            where_this_available = [url] + [parse_url_unescape(_) for _ in alternatives0]

            s = TopicsIndex.from_json(res)
            available = {}
            for k, tr0 in s.topics.items():
                reachability = []
                for r in tr0.reachability:
                    if "://" in r.url:
                        reachability.append(r)
                        # r2 = replace(r, url=url2)
                        # reachability.append(r2)
                    else:
                        for w in where_this_available:
                            url2 = url_to_string(join(w, r.url))

                            r2 = replace(r, url=url2)
                            reachability.append(r2)

                # urls: list[URLString] = [url_to_string(join(url, _)) for _ in tr0.urls]

                tr2 = replace(tr0, reachability=reachability)
                available[k] = tr2
            return available

    # def _get_topic_url(self, name: TopicName) -> URL:
    #     url0 = self.available[name].urls[0]
    #     return self._look_cache(url0)

    def _look_cache(self, url0: URL | URLString) -> URL:
        if not isinstance(url0, URL):
            url0 = parse_url_unescape(url0)
        return self.preferred_cache.get(url0, url0)

    async def publish(self, url0: URL | URLString, rd: RawData) -> None:
        url = self._look_cache(url0)
        #
        # if name not in self.available:
        #     raise NoSuchTopic(name)
        # name = TopicName(name)
        args = {}
        content_type = rd.content_type  # XXX: how to use this?
        # url = self.available[name].url
        # url = self._get_topic_url(name)
        async with self._session(url) as (session, use_url):
            async with session.post(use_url, data=rd.content, **args) as resp:
                resp.raise_for_status()
                assert resp.status == 200, resp
                await self.prefer_alternative(url, resp)  # just memorize

                # logger.info(f"posted {content_type} to {url}")

    #
    # async def listen(self, url0:  URL | str, cb: Callable[[RawData], Any]) -> None:
    #     if name not in self.available:
    #         raise NoSuchTopic(name)
    #     name = TopicName(name)
    #     url = self._get_topic_url(name)
    #     await self.listen_url(url, cb)

    async def prefer_alternative(self, current: URL, resp: aiohttp.ClientResponse) -> Optional[URL]:
        assert isinstance(current, URL), current
        if current in self.preferred_cache:
            return self.preferred_cache[current]

        alternatives0 = cast(list[URLString], resp.headers.getall("Content-Location", []))

        if not alternatives0:
            return None
        alternatives = [current] + [parse_url_unescape(a) for a in alternatives0]
        answering = resp.headers.get(HEADER_NODE_ID)

        # noinspection PyTypeChecker
        best = await self.find_best_alternative([(_, answering) for _ in alternatives])

        if best != current:
            self.preferred_cache[current] = best
            return best
        return None

    async def compute_with_hop(
        self,
        this_node_id: NodeID,
        this_url: URLString,
        connects_to: URL,
        expects_answer_from: NodeID,
        forwarders: list[ForwardingStep],
    ) -> Optional[TopicReachability]:
        if (benchmark := await self.can_use_url(connects_to, expects_answer_from)) is None:
            return None

        me = ForwardingStep(
            this_node_id,
            complexity=benchmark.complexity,
            estimated_latency=benchmark.latency,
            estimated_bandwidth=benchmark.bandwidth,
            forwarding_node_connects_to=url_to_string(connects_to),
        )
        tr2 = TopicReachability(this_url, this_node_id, forwarders=forwarders + [me])
        return tr2

    # async def score_alternatives(self, trs: list[TopicReachability]) -> list[TopicReachability]:
    #     possible = []
    #     for tr in trs:
    #         if (benchmark := self.can_use_url(parse_url_unescape(tr.url))) is not None:
    #
    #
    #             possible.append((benchmark, tr.get_total_latency() + benchmark., len(tr.forwarders), tr))
    #
    #     possible.sort(key=lambda x: (x[0], x[1], x[2]))
    #
    #     return [_[-1] for _ in possible]

    async def find_best_alternative(self, us: list[tuple[URL, NodeID]]) -> Optional[URL]:
        results = []
        possible = []
        for a, expects_answer_from in us:
            if (score := await self.can_use_url(a, expects_answer_from)) is not None:
                possible.append((score.complexity, score.latency, -score.bandwidth, a))
                results.append(f"✓ {str(a):<60} -> {score}")
            else:
                results.append(f"✗ {a} ")

        possible.sort(key=lambda x: (x[0], x[1]))
        if not possible:
            return None
        best = possible[0][-1]

        results.append(f"best: {best}")
        logger.debug("\n".join(results))

        return best

    @methodtools.lru_cache()
    def measure_latency(self, host: str, port: int) -> Optional[float]:
        from tcp_latency import measure_latency

        logger.debug(f"computing latency to {host}:{port}...")
        res = measure_latency(host, port, runs=5, wait=0.01, timeout=0.5)

        if not res:
            logger.debug(f"latency to {host}:{port} -> unreachable")
            return None

        latency_seconds = (sum(res) / len(res)) / 1000.0

        logger.debug(f"latency to {host}:{port} is  {latency_seconds}s  [{res}]")
        return latency_seconds

    async def can_use_url(
        self,
        url: URL,
        expects_answer_from: NodeID,
        measure_latency: bool = True,
        check_right_node: bool = True,
    ) -> Optional[LinkBenchmark]:
        """Returns None or a score for the url."""
        blacklist_key = (url.scheme, url.host, url.port)
        if blacklist_key in self.blacklist_protocol_host_port:
            return None

        if url.scheme in ("http", "https"):
            complexity = 2
            bandwidth = 100_000_000

            if url.port is None:
                port = 80 if url.scheme == "http" else 443
            else:
                port = url.port

            if measure_latency:
                latency = self.measure_latency(url.host, port)
                if latency is None:
                    self.blacklist_protocol_host_port.add(blacklist_key)
                    return None
            else:
                latency = 0.1

            if check_right_node:
                who_answers = await self.get_who_answers(url)

                if who_answers != expects_answer_from:
                    # msg = f"wrong {who_answers=} header in {url}, expected {expects_answer_from}"
                    # logger.error(msg)
                    #
                    # self.obtained_answer[blacklist_key] = resp.headers[HEADER_NODE_ID]
                    #
                    # self.blacklist_protocol_host_port.add(blacklist_key)
                    return None

            return LinkBenchmark(complexity, bandwidth, latency)
        if url.scheme == "http+unix":
            complexity = 1
            bandwidth = 100_000_000
            latency = 0.001
            path = url.host
            if path is None:
                raise ValueError(f"no host in {repr(url)}")
            if os.path.exists(path):
                return LinkBenchmark(complexity, bandwidth, latency)
            else:
                logger.warning(f"unix socket {path!r} does not exist")
                self.blacklist_protocol_host_port.add(blacklist_key)
                return None
        if url.scheme == "http+ether":
            # path = url.host
            # if os.path.exists(path):
            #     return 2
            # else:
            #     logger.warning(f"unix socket {path!r} does not exist")
            return None

        logger.warning(f"unknown scheme {url.scheme!r} for {url}")
        return None

    async def get_who_answers(self, url: URL) -> Optional[NodeID]:
        key = (url.scheme, url.host, url.port)
        if key not in self.obtained_answer:
            try:
                async with self._session(url, conn_timeout=1) as (session, url_to_use):
                    logger.debug(f"checking {url}...")
                    async with session.head(url_to_use) as resp:
                        if HEADER_NODE_ID not in resp.headers:
                            msg = f"no {HEADER_NODE_ID} header in {url}"
                            logger.error(msg)
                            self.obtained_answer[key] = None
                        else:
                            self.obtained_answer[key] = resp.headers[HEADER_NODE_ID]

            except:
                self.obtained_answer[key] = None

            res = self.obtained_answer[key]
            if res is None:
                logger.warning(f"no {HEADER_NODE_ID} header in {url}: not part of system?")

        res = self.obtained_answer[key]

        return res

    @asynccontextmanager
    async def _session(
        self, url: URL, conn_timeout: Optional[float] = None
    ) -> AsyncIterator[tuple[aiohttp.ClientSession, str]]:
        assert isinstance(url, URL)
        if url.scheme == "http+unix":
            connector = UnixConnector(path=url.host)
            use_url = str(url._replace(scheme="http", host="localhost"))
        elif url.scheme in ("http", "https"):
            connector = TCPConnector()
            use_url = str(url)
        else:
            raise ValueError(f"unknown scheme {url.scheme!r}")

        async with aiohttp.ClientSession(connector=connector, conn_timeout=conn_timeout) as session:
            yield session, use_url

    async def get_alternates(self, url: URLString | URL) -> list[URL]:
        url = self._look_cache(url)
        try:
            async with self._session(url, conn_timeout=2) as (session, use_url):
                async with session.head(use_url) as resp:
                    if resp.status == 404:
                        return []
                    resp.raise_for_status()
                    assert resp.status == 200, resp
                    # logger.info(f"headers : {resp.headers}")
                    alternatives0 = resp.headers.getall("Content-Location", [])
        except (TimeoutError, ClientConnectorError):
            return []
        return [parse_url_unescape(_) for _ in alternatives0]

    # async def listen_url(
    #     self, reachability: list[TopicReachability],
    #     cb: Callable[[RawData], Any]
    # ) -> "asyncio.Task[None]":
    #

    async def choose_best(self, reachability: list[TopicReachability]) -> URL:
        res = await self.find_best_alternative(
            [(parse_url_unescape(r.url), r.answering) for r in reachability]
        )
        if res is None:
            msg = f"no reachable url for {reachability}"
            logger.error(msg)
            raise ValueError(msg)
        return res

    async def listen_url(self, url: URL, cb: Callable[[RawData], Any]) -> "asyncio.Task[None]":
        url = self._look_cache(url)
        async with self._session(url) as (session, use_url):
            async with session.get(use_url) as resp:
                resp.raise_for_status()
                assert resp.status == 200, resp

                if (preferred := await self.prefer_alternative(url, resp)) is not None:
                    logger.info(f"Using preferred alternative to {url} -> {repr(preferred)}")
                    return await self.listen_url(preferred, cb)

                # res = await resp.text()
                # logger.info(f'got {res!r} {resp.headers}')

                if HEADER_SEE_EVENTS in resp.headers:
                    events_purl = resp.headers[HEADER_SEE_EVENTS]
                    url_events = join(url, events_purl)

                    event_ready = asyncio.Event()
                    t = asyncio.create_task(self._listen_events(url_events, cb, event_ready))
                    self.tasks.append(t)
                    return t
                else:
                    raise EventListeningNotAvailable(f"no {HEADER_SEE_EVENTS} in {resp.headers}")

    async def listen_url_events(self, url: URL) -> "AsyncIterator[DataReady]":
        async with self._session(url) as (session, use_url):
            async with session.ws_connect(use_url) as ws:
                logger.info(f"websocket to {url} ready")

                msg: WSMessage

                async for msg in ws:
                    dr = DataReady.from_json_string(msg.data)

                    yield dr

    async def _listen_events(
        self,
        # topic_name: TopicName,
        url: URL,
        cb: Callable[[RawData], Any],
        event_ready: asyncio.Event,
    ) -> None:
        # wait = 0.6
        # wait_exp = 1.5

        while True:
            try:
                logger.error(f"trying to reconnect to {url}")
                await self._listen_events_one(url, cb, event_ready)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"error in _listen_events_one: {e!r}")
                await asyncio.sleep(1)

    @async_error_catcher
    async def _listen_events_one(
        self,
        # topic_name: TopicName,
        url: URL,
        cb: Callable[[RawData], Any],
        event_ready: asyncio.Event,
    ) -> None:
        async with self._session(url) as (session, use_url):
            async with session.ws_connect(use_url) as ws:
                logger.info(f"websocket to {url} ready")

                msg: WSMessage

                counter = 0
                async for msg in ws:
                    event_ready.set()

                    dr = DataReady.from_json_string(msg.data)
                    url_datas = [join(url, _) for _ in dr.urls]
                    logger.info(f"got {url} \n -> {dr}")
                    try:
                        url_data = url_datas[0]  # XXX: try myltiple urls
                        async with self._session(url_data) as (session2, use_url2):
                            async with session.get(use_url2) as resp_data:
                                resp_data.raise_for_status()
                                # assert resp_data.status == 200
                                data = await resp_data.read()
                                content_type = resp_data.content_type
                                data = RawData(content_type=content_type, content=data)
                                cb(data)

                    except Exception as e:
                        logger.error(f"error in downloading {url_data!r}: {e!r}")

                    counter += 1
