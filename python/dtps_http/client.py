import asyncio
import os
import traceback
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, AsyncExitStack
from dataclasses import asdict, dataclass, replace
from typing import Any, AsyncContextManager, AsyncIterator, Callable, cast, Optional, TYPE_CHECKING, TypeVar

import aiohttp
import cbor2
from aiohttp import ClientWebSocketResponse, TCPConnector, UnixConnector, WSMessage
from tcp_latency import measure_latency

from . import logger
from .constants import (
    HEADER_CONTENT_LOCATION,
    HEADER_NODE_ID,
    REL_EVENTS_DATA,
    REL_EVENTS_NODATA,
    REL_HISTORY,
    REL_META,
)
from .exceptions import EventListeningNotAvailable
from .server import get_link_headers
from .structures import (
    channel_msgs_parse,
    ChannelInfo,
    Chunk,
    DataReady,
    ForwardingStep,
    LinkBenchmark,
    RawData,
    TopicReachability,
    TopicRef,
    TopicsIndex,
)
from .types import NodeID, TopicNameV, URLString
from .urls import (
    join,
    parse_url_unescape,
    URL,
    url_to_string,
    URLIndexer,
    URLTopic,
    URLWS,
    URLWSInline,
    URLWSOffline,
)
from .utils import async_error_catcher_iterator, method_lru_cache

__all__ = [
    "DTPSClient",
]

U = TypeVar("U", bound=URL)


@dataclass
class FoundMetadata:
    alternative_urls: list[URLTopic]
    answering: Optional[NodeID]
    events_url: Optional[URLWSOffline]
    events_data_inline_url: Optional[URLWSInline]
    meta_url: Optional[URLString]
    history_url: Optional[URLString]


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

    def __init__(self) -> None:
        self.S = AsyncExitStack()
        self.tasks = []
        self.sessions = {}
        self.preferred_cache = {}
        self.blacklist_protocol_host_port = set()
        self.obtained_answer = {}

    tasks: "list[asyncio.Task[Any]]"
    blacklist_protocol_host_port: set[tuple[str, str, int]]
    obtained_answer: dict[tuple[str, str, int], Optional[NodeID]]

    preferred_cache: dict[URL, URL]
    sessions: dict[str, aiohttp.ClientSession]

    async def init(self) -> None:
        pass

    async def aclose(self) -> None:
        for t in self.tasks:
            t.cancel()
        await self.S.aclose()

    async def ask_topics(self, url: URLIndexer) -> dict[TopicNameV, TopicRef]:
        url = self._look_cache(url)
        async with self.my_session(url) as (session, use_url):
            async with session.get(use_url) as resp:
                resp.raise_for_status()
                answering = resp.headers.get(HEADER_NODE_ID)

                #  logger.debug(f"ask topics {resp.headers}")
                if (preferred := await self.prefer_alternative(url, resp)) is not None:
                    logger.info(f"Using preferred alternative to {url} -> {repr(preferred)}")
                    return await self.ask_topics(preferred)
                assert resp.status == 200, resp.status
                res_bytes: bytes = await resp.read()
                res = cbor2.loads(res_bytes)  #  .decode("utf-8")

            alternatives0 = cast(list[URLString], resp.headers.getall(HEADER_CONTENT_LOCATION, []))
            where_this_available = [url] + [parse_url_unescape(_) for _ in alternatives0]

            s = TopicsIndex.from_json(res)
            available: dict[TopicNameV, TopicRef] = {}
            for k, tr0 in s.topics.items():
                reachability: list[TopicReachability] = []
                for r in tr0.reachability:
                    if "://" in r.url:
                        reachability.append(r)  #  r2 = replace(r, url=url2)
                    #  reachability.append(r2)
                    else:
                        for w in where_this_available:
                            url2 = url_to_string(join(w, r.url))

                            r2 = replace(r, url=url2)
                            reachability.append(r2)

                #  print(f"reachability {reachability}")

                #  urls: list[URLString] = [url_to_string(join(url, _)) for _ in tr0.urls]

                tr2 = replace(tr0, reachability=reachability)
                available[k] = tr2
            return available

    def _look_cache(self, url0: U) -> U:
        if not isinstance(url0, URL):
            url0 = parse_url_unescape(url0)
        return cast(U, self.preferred_cache.get(url0, url0))

    async def publish(self, url0: URL, rd: RawData) -> None:
        url = self._look_cache(url0)

        headers = {"content-type": rd.content_type}

        async with self.my_session(url) as (session, use_url):
            async with session.post(use_url, data=rd.content, headers=headers) as resp:
                resp.raise_for_status()
                assert resp.status == 200, resp
                await self.prefer_alternative(url, resp)  #  just memorize

    async def prefer_alternative(self, current: U, resp: aiohttp.ClientResponse) -> Optional[U]:
        assert isinstance(current, URL), current
        if current in self.preferred_cache:
            return cast(U, self.preferred_cache[current])

        nothing: list[URLString] = []
        alternatives0 = cast(list[URLString], resp.headers.getall(HEADER_CONTENT_LOCATION, nothing))

        if not alternatives0:
            return None
        alternatives = [current] + [parse_url_unescape(a) for a in alternatives0]
        answering = cast(NodeID, resp.headers.get(HEADER_NODE_ID))

        #  noinspection PyTypeChecker
        best = await self.find_best_alternative([(_, answering) for _ in alternatives])
        if best is None:
            best = current
        if best != current:
            self.preferred_cache[current] = best
            return cast(U, best)
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
            this_node_id,  #  complexity=benchmark.complexity,
            #  estimated_latency=benchmark.latency,
            #  estimated_bandwidth=benchmark.bandwidth,
            forwarding_node_connects_to=url_to_string(connects_to),
            performance=benchmark,
        )
        total = LinkBenchmark.identity()
        for f in forwarders:
            total |= f.performance
        total |= benchmark
        tr2 = TopicReachability(this_url, this_node_id, forwarders=forwarders + [me], benchmark=total)
        return tr2

    async def find_best_alternative(self, us: list[tuple[U, NodeID]]) -> Optional[U]:
        if not us:
            logger.warning("find_best_alternative: no alternatives")
            return None
        results: list[str] = []
        possible: list[tuple[float, float, float, U]] = []
        for a, expects_answer_from in us:
            if (score := await self.can_use_url(a, expects_answer_from)) is not None:
                possible.append((score.complexity, score.latency_ns, -score.bandwidth, a))
                results.append(f"✓ {str(a):<60} -> {score}")
            else:
                results.append(f"✗ {a} ")

        possible.sort(key=lambda x: (x[0], x[1]))
        if not possible:
            rs = "\n".join(results)
            logger.warning(
                f"find_best_alternative: no alternatives found:\n {rs}",
            )
            return None
        best = possible[0][-1]

        results.append(f"best: {best}")
        logger.debug("\n".join(results))

        return best

    @method_lru_cache()
    def measure_latency(self, host: str, port: int) -> Optional[float]:
        logger.debug(f"computing latency to {host}:{port}...")
        res = cast(list[float], measure_latency(host, port, runs=5, wait=0.01, timeout=0.5))

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
        do_measure_latency: bool = True,
        check_right_node: bool = True,
    ) -> Optional[LinkBenchmark]:
        """Returns None or a score for the url."""
        blacklist_key = (url.scheme, url.host, url.port or 0)
        if blacklist_key in self.blacklist_protocol_host_port:
            logger.debug(f"blacklisted {url}")
            return None

        if url.scheme in ("http", "https"):
            hops = 1
            complexity = 2
            bandwidth = 100_000_000
            reliability = 0.9
            if url.port is None:
                port = 80 if url.scheme == "http" else 443
            else:
                port = url.port

            if do_measure_latency:
                latency = self.measure_latency(url.host, port)
                if latency is None:
                    self.blacklist_protocol_host_port.add(blacklist_key)
                    return None
            else:
                latency = 0.1

            if check_right_node:
                who_answers = await self.get_who_answers(url)

                if who_answers != expects_answer_from:
                    msg = f"wrong {who_answers=} header in {url}, expected {expects_answer_from}"
                    logger.error(msg)

                    #

                    #  self.obtained_answer[blacklist_key] = resp.headers[HEADER_NODE_ID]

                    #

                    #  self.blacklist_protocol_host_port.add(blacklist_key)
                    return None

            latency_ns = int(latency * 1_000_000_000)
            reliability_percent = int(reliability * 100)
            return LinkBenchmark(complexity, bandwidth, latency_ns, reliability_percent, hops)
        if url.scheme == "http+unix":
            complexity = 1
            reliability_percent = 100
            hops = 1
            bandwidth = 100_000_000
            latency = 0.001
            path = url.host
            logger.info(f"checking {url}...")
            if not os.path.exists(path):
                logger.info(f" {url}: {path=!r} does not exist")
                return None
            who_answers = await self.get_who_answers(url)

            if who_answers != expects_answer_from:
                msg = f"wrong {who_answers=} header in {url}, expected {expects_answer_from}"
                logger.error(msg)

                #

                #  self.obtained_answer[blacklist_key] = resp.headers[HEADER_NODE_ID]

                #

                #  self.blacklist_protocol_host_port.add(blacklist_key)
                return None

            latency_ns = int(latency * 1_000_000_000)

            return LinkBenchmark(complexity, bandwidth, latency_ns, reliability_percent, hops)

            #
        #  if path is None:
        #      raise ValueError(f"no host in {repr(url)}")
        #  if os.path.exists(path):
        #
        #  else:
        #      logger.warning(f"unix socket {path!r} does not exist")
        #      self.blacklist_protocol_host_port.add(blacklist_key)
        #      return None
        if url.scheme == "http+ether":
            #  path = url.host

            #  if os.path.exists(path):

            #      return 2

            #  else:

            #      logger.warning(f"unix socket {path!r} does not exist")
            return None

        logger.warning(f"unknown scheme {url.scheme!r} for {url}")
        return None

    async def get_who_answers(self, url: URL) -> Optional[NodeID]:
        key = (url.scheme, url.host, url.port or 0)
        if key not in self.obtained_answer:
            try:
                md = await self.get_metadata(url)
                logger.warn(f"checking {url} -> {md}")
                return md.answering

                #   self.obtained_answer[
            #       key
            #   ] = (
            #       md.answering
            #   )
            #
            #   async with self.my_session(url, conn_timeout=1) as (session, url_to_use):
            #       logger.debug(f"checking {url}...")
            #       async with session.head(url_to_use) as resp:
            #           if HEADER_NODE_ID not in resp.headers:
            #               msg = f"no {HEADER_NODE_ID} header in {url}"
            #               logger.error(msg)
            #               self.obtained_answer[key] = None
            #           else:
            #               self.obtained_answer[key] = NodeID(resp.headers[HEADER_NODE_ID])

            except:
                logger.exception(f"error checking {url} {traceback.format_exc()}")
                return None
                self.obtained_answer[key] = None

            res = self.obtained_answer[key]
            if res is None:
                logger.warning(f"no {HEADER_NODE_ID} header in {url}: not part of system?")

        res = self.obtained_answer[key]

        return res

    if TYPE_CHECKING:

        def my_session(
            self, url: URL, /, *, conn_timeout: Optional[float] = None
        ) -> AsyncContextManager[tuple[aiohttp.ClientSession, URLString]]:
            ...

    else:

        @asynccontextmanager
        async def my_session(
            self, url: URL, /, *, conn_timeout: Optional[float] = None
        ) -> AsyncIterator[tuple[aiohttp.ClientSession, str]]:
            assert isinstance(url, URL)
            if url.scheme == "http+unix":
                connector = UnixConnector(path=url.host)

                #  noinspection PyProtectedMember
                use_url = str(url._replace(scheme="http", host="localhost"))
            elif url.scheme in ("http", "https"):
                connector = TCPConnector()
                use_url = str(url)
            else:
                raise ValueError(f"unknown scheme {url.scheme!r}")

            async with aiohttp.ClientSession(connector=connector, conn_timeout=conn_timeout) as session:
                yield session, use_url

    async def get_metadata(self, url0: URLTopic) -> FoundMetadata:
        url = self._look_cache(url0)
        use_url = None
        try:
            async with self.my_session(url, conn_timeout=2) as (session, use_url):
                async with session.head(use_url) as resp:
                    #  if resp.status == 404:

                    #      return FoundMetadata([], None, None, None)
                    logger.info(f"headers {url0}: {resp.headers}")
                    resp.raise_for_status()

                    #  assert resp.status == 200, resp

                    #  logger.info(f"headers : {resp.headers}")
                    if HEADER_CONTENT_LOCATION in resp.headers:
                        alternatives0 = cast(list[URLString], resp.headers.getall(HEADER_CONTENT_LOCATION))
                    else:
                        alternatives0 = []

                    links = get_link_headers(resp.headers)
                    if REL_EVENTS_DATA in links:
                        events_url_data = cast(URLWSInline, join(url, links[REL_EVENTS_DATA].url))
                    else:
                        events_url_data = None
                    if REL_EVENTS_NODATA in links:
                        events_url = cast(URLWSInline, join(url, links[REL_EVENTS_NODATA].url))
                    else:
                        events_url = None

                    if HEADER_NODE_ID not in resp.headers:
                        answering = None
                    else:
                        answering = NodeID(resp.headers[HEADER_NODE_ID])

                    if REL_HISTORY not in resp.headers:
                        history_url = None
                    else:
                        history_url = join(url, resp.headers[REL_HISTORY])
                    if REL_META not in resp.headers:
                        meta_url = None
                    else:
                        meta_url = join(url, resp.headers[REL_META])
        except:
            #  (TimeoutError, ClientConnectorError):
            logger.error(f"cannot connect to {url0=!r} {use_url=!r} \n{traceback.format_exc()}")

            #  return FoundMetadata([], None, None, None)
            raise
        urls = [cast(URLTopic, join(url, _)) for _ in alternatives0]
        return FoundMetadata(
            urls,
            answering=answering,
            events_url=events_url,
            events_data_inline_url=events_url_data,
            meta_url=meta_url,
            history_url=history_url,
        )

    async def choose_best(self, reachability: list[TopicReachability]) -> URL:
        res = await self.find_best_alternative(
            [(parse_url_unescape(r.url), r.answering) for r in reachability]
        )
        if res is None:
            msg = f"no reachable url for {reachability}"
            logger.error(msg)
            raise ValueError(msg)
        return res

    async def listen_topic(
        self, urlbase: URLIndexer, topic_name: TopicNameV, cb: Callable[[RawData], Any], inline_data: bool
    ) -> "asyncio.Task[None]":
        available = await self.ask_topics(urlbase)
        topic = available[topic_name]
        url = cast(URLTopic, await self.choose_best(topic.reachability))

        return await self.listen_url(url, cb, inline_data)

    async def listen_url(
        self, url_topic: URLTopic, cb: Callable[[RawData], Any], inline_data: bool
    ) -> "asyncio.Task[None]":
        url_topic = self._look_cache(url_topic)
        metadata = await self.get_metadata(url_topic)

        if inline_data:
            if metadata.events_data_inline_url is not None:
                url_events = metadata.events_data_inline_url
            else:
                raise EventListeningNotAvailable(f"cannot find metadata {url_topic}: {metadata}")

        else:
            if metadata.events_url is not None:
                url_events = metadata.events_url
            else:
                raise EventListeningNotAvailable(f"cannot find metadata {url_topic}: {metadata}")

        logger.info(f"listening to  {url_topic} -> {metadata} -> {url_events}")
        it = self.listen_url_events(url_events, inline_data)
        t = asyncio.create_task(self._listen_and_callback(it, cb))
        self.tasks.append(t)
        return t

    async def _listen_and_callback(
        self, it: AsyncIterator[tuple[DataReady, RawData]], cb: Callable[[RawData], Any]
    ) -> None:
        async for dr, rd in it:
            cb(rd)

    async def listen_url_events(
        self, url_events: URLWS, inline_data: bool
    ) -> "AsyncIterator[tuple[DataReady, RawData]]":
        if inline_data:
            if "?" not in str(url_events):
                raise ValueError(f"inline data requested but no ? in {url_events}")
            async for _ in self.listen_url_events_with_data_inline(url_events):
                yield _
        else:
            async for _ in self.listen_url_events_with_data_offline(url_events):
                yield _

    async def listen_url_events_with_data_offline(
        self,
        url_websockets: URLWS,
    ) -> AsyncIterator[tuple[DataReady, RawData]]:
        """Iterates using direct data using side loading"""
        use_url: URLString
        async with self.my_session(url_websockets) as (session, use_url):
            ws: ClientWebSocketResponse
            async with session.ws_connect(use_url) as ws:
                #  noinspection PyProtectedMember
                headers = "".join(f"{k}: {v}\n" for k, v in ws._response.headers.items())
                logger.info(f"websocket to {url_websockets} ready\n{headers}")

                while True:
                    if ws.closed:
                        break
                    msg: WSMessage = await ws.receive()
                    if msg.type == aiohttp.WSMsgType.BINARY:
                        try:
                            cm = channel_msgs_parse(msg.data)
                        except Exception as e:
                            logger.error(
                                f"error in parsing\n{msg.data!r}\nerror:\n{e.__class__.__name__} {e!r}"
                            )
                            continue

                        match cm:
                            case DataReady() as dr:
                                data = await self._download_from_urls(url_websockets, dr)
                                yield dr, data
                            case ChannelInfo() as ci:
                                logger.info(f"channel info {ci}")
                            case _:
                                logger.debug(f"cannot interpret {cm}")
                    else:
                        logger.error(f"unexpected message type {msg.type} {msg.data!r}")

    async def _download_from_urls(self, urlbase: URL, dr: DataReady) -> RawData:
        url_datas = [join(urlbase, _.url) for _ in dr.availability]

        #  logger.info(f"url_datas {url_datas}")
        if not url_datas:
            logger.error(f"no url_datas in {dr}")
            raise AssertionError(f"no url_datas in {dr}")

        url_data = url_datas[0]
        #  TODO: try multiple urls

        #  logger.info(f"downloading {url_data!r}")
        async with self.my_session(url_data) as (session2, use_url2):
            async with session2.get(use_url2) as resp_data:
                resp_data.raise_for_status()
                data = await resp_data.read()
                content_type = resp_data.content_type
                data = RawData(content_type=content_type, content=data)
                return data

    @async_error_catcher_iterator
    async def listen_url_events_with_data_inline(
        self, url_websockets: URLWS
    ) -> "AsyncIterator[tuple[DataReady, RawData]]":
        """Iterates using direct data in websocket."""
        logger.info(f"listen_url_events_with_data_inline {url_websockets}")
        async with self.my_session(url_websockets) as (session, use_url):
            ws: ClientWebSocketResponse
            async with session.ws_connect(use_url) as ws:
                #  noinspection PyProtectedMember
                headers = "".join(f"{k}: {v}\n" for k, v in ws._response.headers.items())
                logger.info(f"websocket to {url_websockets} ready\n{headers}")

                while True:
                    if ws.closed:
                        break
                    msg = await ws.receive()
                    if msg.type == aiohttp.WSMsgType.CLOSE:
                        break
                    if msg.type == aiohttp.WSMsgType.BINARY:
                        try:
                            cm = channel_msgs_parse(msg.data)

                        except Exception as e:
                            logger.error(f"error in parsing {msg.data!r}: {e.__class__.__name__} {e!r}")
                            continue
                    else:
                        logger.error(f"unexpected message type {msg.type} {msg.data!r}")
                        continue

                    match cm:
                        case DataReady() as dr:
                            if dr.chunks_arriving == 0:
                                logger.error(f"unexpected chunks_arriving {dr.chunks_arriving} in {dr}")
                                raise AssertionError(
                                    f"unexpected chunks_arriving {dr.chunks_arriving} in {dr}"
                                )

                            #  create a byte array initialized at

                            data = b""
                            for _ in range(dr.chunks_arriving):
                                msg = await ws.receive()

                                cm = channel_msgs_parse(msg.data)

                                match cm:
                                    case Chunk(index=index, data=this_data):
                                        data += this_data
                                    case _:
                                        continue
                                        logger.error(f"unexpected message {msg!r}")

                            if len(data) != dr.content_length:
                                logger.error(
                                    f"unexpected data length {len(data)} != {dr.content_length}\n{dr}"
                                )
                                raise AssertionError(
                                    f"unexpected data length {len(data)} != {dr.content_length}"
                                )

                            yield dr, RawData(content_type=dr.content_type, content=data)

                        case ChannelInfo() as ci:
                            logger.info(f"channel info {ci}")

                        case _:
                            logger.error(f"unexpected message {cm!r}")

    @asynccontextmanager
    async def push_through_websocket(
        self,
        url_websockets: URLWS,
    ) -> "AsyncIterator[PushInterface]":
        """Iterates using direct data using side loading"""
        use_url: URLString
        async with self.my_session(url_websockets) as (session, use_url):
            ws: ClientWebSocketResponse
            async with session.ws_connect(use_url) as ws:

                class PushInterfaceImpl(PushInterface):
                    async def push_through(self, data: bytes, content_type: str) -> None:
                        rd = RawData(content_type=content_type, content=data)
                        as_struct = {RawData.__name__: asdict(rd)}
                        cbor_data = cbor2.dumps(as_struct)

                        await ws.send_bytes(cbor_data)

                yield PushInterfaceImpl()


class PushInterface(ABC):
    @abstractmethod
    async def push_through(self, data: bytes, content_type: str) -> None:
        ...
