import asyncio
import json
import os
import traceback
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, AsyncExitStack
from dataclasses import asdict, dataclass, replace
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Callable,
    cast,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    TYPE_CHECKING,
    TypeVar,
)

import aiohttp
import cbor2
from aiohttp import (
    ClientResponseError,
    ClientWebSocketResponse,
    TCPConnector,
    UnixConnector,
    WSCloseCode,
    WSMessage,
)
from tcp_latency import measure_latency

from . import logger
from .constants import (
    CONTENT_TYPE_PATCH_JSON,
    HEADER_CONTENT_LOCATION,
    HEADER_NODE_ID,
    MIME_JSON,
    REL_EVENTS_DATA,
    REL_EVENTS_NODATA,
    REL_HISTORY,
    REL_META,
    TOPIC_PROXIED,
)
from .exceptions import EventListeningNotAvailable
from .link_headers import get_link_headers
from .structures import (
    channel_msgs_parse,
    ChannelInfo,
    Chunk,
    DataReady,
    ErrorMsg,
    FinishedMsg,
    ForwardingStep,
    InsertNotification,
    LinkBenchmark,
    ListenURLEvents,
    RawData,
    SilenceMsg,
    TopicReachability,
    TopicRef,
    TopicsIndex,
    WarningMsg,
)
from .types import ContentType, NodeID, TopicNameV, URLString
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
    "StopContinuousLoop",
    "my_raise_for_status",
]

U = TypeVar("U", bound=URL)


@dataclass
class ProxyJob:
    node_id: str
    urls: List[str]


@dataclass
class FoundMetadata:
    # url alternative
    alternative_urls: List[URLTopic]

    # se un nodo DTPS risponde
    answering: Optional[NodeID]

    # eventi websocket con dati offline
    events_url: Optional[URLWSOffline]
    # eventi websocket con dati inline
    events_data_inline_url: Optional[URLWSInline]

    # metadati della risorsa (il ContentInfo etc.)
    meta_url: Optional[URL]
    # servizio history
    history_url: Optional[URL]


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
    blacklist_protocol_host_port: Set[Tuple[str, str, int]]
    obtained_answer: Dict[Tuple[str, str, int], Optional[NodeID]]

    preferred_cache: Dict[URL, URL]
    sessions: Dict[str, aiohttp.ClientSession]

    async def init(self) -> None:
        pass

    async def aclose(self) -> None:
        for t in self.tasks:
            t.cancel()
        await self.S.aclose()

    async def ask_topics(self, url0: URLIndexer) -> Dict[TopicNameV, TopicRef]:
        url = self._look_cache(url0)
        async with self.my_session(url) as (session, use_url):
            async with session.get(use_url) as resp:
                await my_raise_for_status(resp, url0)
                # answering = resp.headers.get(HEADER_NODE_ID)

                #  logger.debug(f"ask topics {resp.headers}")
                if (preferred := await self.prefer_alternative(url, resp)) is not None:
                    logger.info(f"Using preferred alternative to {url} -> {repr(preferred)}")
                    return await self.ask_topics(preferred)
                assert resp.status == 200, resp.status
                res_bytes: bytes = await resp.read()
                res = cbor2.loads(res_bytes)

            alternatives0 = cast(List[URLString], resp.headers.getall(HEADER_CONTENT_LOCATION, []))
            where_this_available: List[URL] = [url]
            for a in alternatives0:
                try:
                    x = parse_url_unescape(a)
                except Exception:
                    logger.exception(f"cannot parse {a}")
                    continue
                else:
                    where_this_available.append(x)

            s = TopicsIndex.from_json(res)
            available: Dict[TopicNameV, TopicRef] = {}
            for k, tr0 in s.topics.items():
                reachability: List[TopicReachability] = []
                for r in tr0.reachability:
                    if "://" in r.url:
                        reachability.append(r)
                    else:
                        for w in where_this_available:
                            url2 = url_to_string(join(w, r.url))

                            r2 = replace(r, url=url2)
                            reachability.append(r2)

                tr2 = replace(tr0, reachability=reachability)
                available[k] = tr2
            return available

    def _look_cache(self, url0: U) -> U:
        return cast(U, self.preferred_cache.get(url0, url0))

    async def publish(self, url0: URL, rd: RawData) -> None:
        url = self._look_cache(url0)

        headers = {"content-type": rd.content_type}

        async with self.my_session(url) as (session, use_url):
            async with session.post(use_url, data=rd.content, headers=headers) as resp:
                await my_raise_for_status(resp, url0)
                assert resp.status == 200, resp
                await self.prefer_alternative(url, resp)  # just memorize

    async def prefer_alternative(self, current: U, resp: aiohttp.ClientResponse) -> Optional[U]:
        assert isinstance(current, URL), current
        if current in self.preferred_cache:
            return cast(U, self.preferred_cache[current])

        nothing: List[URLString] = []
        alternatives0 = cast(List[URLString], resp.headers.getall(HEADER_CONTENT_LOCATION, nothing))

        if not alternatives0:
            return None
        alternatives = [current]
        for a in alternatives0:
            try:
                x = parse_url_unescape(a)
            except Exception:
                logger.exception(f"cannot parse {a}")
                continue
            else:
                alternatives.append(x)
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
        connects_to: URLTopic,
        expects_answer_from: NodeID,
        forwarders: List[ForwardingStep],
    ) -> Optional[TopicReachability]:
        if (benchmark := await self.can_use_url(connects_to, expects_answer_from)) is None:
            return None

        me = ForwardingStep(
            forwarding_node=this_node_id,
            forwarding_node_connects_to=url_to_string(connects_to),
            performance=benchmark,
        )
        total = LinkBenchmark.identity()
        for f in forwarders:
            total |= f.performance
        total |= benchmark
        tr2 = TopicReachability(
            url=this_url, answering=this_node_id, forwarders=forwarders + [me], benchmark=total
        )
        return tr2

    async def find_best_alternative(self, us: List[Tuple[URLTopic, Optional[NodeID]]]) -> Optional[URLTopic]:
        if not us:
            logger.warning("find_best_alternative: no alternatives")
            return None
        results: List[str] = []
        possible: List[Tuple[float, float, float, URLTopic]] = []
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
        res = cast(List[float], measure_latency(host, port, runs=5, wait=0.01, timeout=0.5))

        if not res:
            logger.debug(f"latency to {host}:{port} -> unreachable")
            return None

        latency_seconds = (sum(res) / len(res)) / 1000.0

        logger.debug(f"latency to {host}:{port} is  {latency_seconds}s  [{res}]")
        return latency_seconds

    async def can_use_url(
        self,
        url: URLTopic,
        expects_answer_from: Optional[NodeID],
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

            if check_right_node and expects_answer_from is not None:
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
            return LinkBenchmark(
                complexity=complexity,
                bandwidth=bandwidth,
                latency_ns=latency_ns,
                reliability_percent=reliability_percent,
                hops=hops,
            )
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

            return LinkBenchmark(
                complexity=complexity,
                bandwidth=bandwidth,
                latency_ns=latency_ns,
                reliability_percent=reliability_percent,
                hops=hops,
            )

        if url.scheme == "http+ether":
            return None

        logger.warning(f"unknown scheme {url.scheme!r} for {url}")
        return None

    async def get_who_answers(self, url: URLTopic) -> Optional[NodeID]:
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

    # if TYPE_CHECKING:

    #     def my_session(
    #         self, url: URL, /, *, conn_timeout: Optional[float] = None
    #     ) -> AsyncContextManager[Tuple[aiohttp.ClientSession, URLString]]:
    #         ...

    # else:

    @asynccontextmanager
    async def my_session(
        self, url: URL, /, *, conn_timeout: Optional[float] = None
    ) -> AsyncIterator[Tuple[aiohttp.ClientSession, URLString]]:
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
            yield session, URLString(use_url)

    async def get_proxied(self, url0: URLIndexer) -> Dict[TopicNameV, ProxyJob]:
        url = join(url0, TOPIC_PROXIED.as_relative_url())
        rd = await self.get(url, accept=MIME_JSON)

        js = json.loads(rd.content)
        res: Dict[TopicNameV, ProxyJob] = {}
        for k, v in js.items():
            res[TopicNameV.from_dash_sep(k)] = ProxyJob(v["node_id"], v["urls"])
        return res

    async def add_proxy(
        self, url0: URLIndexer, topic_name: TopicNameV, node_id: str, urls: List[str]
    ) -> bool:
        """Returns true if there were changes to be made"""

        found = await self.get_proxied(url0)
        path = "/" + escape_json_pointer(topic_name.as_dash_sep())
        patch: List[Dict[str, Any]] = []
        if topic_name in found:
            if found[topic_name].node_id == node_id and found[topic_name].urls == urls:
                return False
            else:
                patch.append(
                    {
                        "op": "remove",
                        "path": path,
                    }
                )
        else:
            patch.append({"op": "add", "path": path, "value": {"node_id": node_id, "urls": urls}})
        as_json = json.dumps(patch).encode("utf-8")
        url = join(url0, TOPIC_PROXIED.as_relative_url())
        res = await self.patch(url, CONTENT_TYPE_PATCH_JSON, as_json)
        return True

    async def remove_proxy(self, url0: URLIndexer, topic_name: TopicNameV) -> None:
        patch = [{"op": "remove", "path": "/" + escape_json_pointer(topic_name.as_dash_sep())}]
        as_json = json.dumps(patch).encode("utf-8")
        url = join(url0, TOPIC_PROXIED.as_relative_url())
        res = await self.patch(url, CONTENT_TYPE_PATCH_JSON, as_json)

    async def patch(self, url0: URL, content_type: Optional[str], data: bytes) -> RawData:
        headers = {"content-type": content_type} if content_type is not None else {}

        url = self._look_cache(url0)
        use_url = None
        try:
            async with self.my_session(url, conn_timeout=2) as (session, use_url):
                async with session.patch(use_url, data=data, headers=headers) as resp:
                    res_bytes: bytes = await resp.read()
                    content_type = ContentType(resp.headers.get("content-type", "application/octet-stream"))
                    rd = RawData(content=res_bytes, content_type=content_type)

                    if not resp.ok:
                        try:
                            message = res_bytes.decode("utf-8")
                        except:
                            message = res_bytes
                        raise ValueError(f"cannot patch {url0=!r} {use_url=!r} {resp=!r}\n{message}")

                    return rd

        except:
            logger.error(f"cannot connect to {url=!r} {use_url=!r} \n{traceback.format_exc()}")
            raise

    async def get(self, url0: URL, accept: Optional[str]) -> RawData:
        headers = {}
        if accept is not None:
            headers["accept"] = accept

        url = self._look_cache(url0)
        use_url = None
        try:
            async with self.my_session(url, conn_timeout=2) as (session, use_url):
                async with session.get(use_url) as resp:
                    res_bytes: bytes = await resp.read()
                    content_type = ContentType(resp.headers.get("content-type", "application/octet-stream"))
                    rd = RawData(content=res_bytes, content_type=content_type)

                    if not resp.ok:
                        try:
                            message = res_bytes.decode("utf-8")
                        except:
                            message = res_bytes
                        raise ValueError(f"cannot GET {url0=!r}\n{use_url=!r}\n{resp=!r}\n{message}")

                    if accept is not None and content_type != accept:
                        raise ValueError(
                            f"GET gave a different content type ({accept=!r}, {content_type}\n{url0=}"
                        )
                    return rd

        except:
            logger.error(f"cannot connect to {url=!r} {use_url=!r} \n{traceback.format_exc()}")
            raise

    async def get_metadata(self, url0: URLTopic) -> FoundMetadata:
        url = self._look_cache(url0)
        use_url = None
        try:
            async with self.my_session(url, conn_timeout=2) as (session, use_url):
                # logger.info(f"get_metadata {url0=!r} {use_url=!r}")
                async with session.head(use_url) as resp:
                    #  if resp.status == 404:

                    #      return FoundMetadata([], None, None, None)
                    logger.info(f"headers {url0}: {resp.headers}")

                    await my_raise_for_status(resp, url0)

                    #  assert resp.status == 200, resp

                    #  logger.info(f"headers : {resp.headers}")
                    if HEADER_CONTENT_LOCATION in resp.headers:
                        alternatives0 = cast(List[URLString], resp.headers.getall(HEADER_CONTENT_LOCATION))
                    else:
                        alternatives0 = []

                    links = get_link_headers(resp.headers)
                    if REL_EVENTS_DATA in links:
                        events_url_data = cast(URLWSInline, join(url, links[REL_EVENTS_DATA].url))
                    else:
                        events_url_data = None
                    if REL_EVENTS_NODATA in links:
                        events_url = cast(URLWSOffline, join(url, links[REL_EVENTS_NODATA].url))
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

    async def choose_best(self, reachability: List[TopicReachability]) -> URL:
        use: List[Tuple[URL, Optional[NodeID]]] = []
        for r in reachability:
            try:
                x = parse_url_unescape(r.url)
            except Exception:
                logger.exception(f"cannot parse {r.url}")
                continue
            else:
                use.append((x, r.answering))
        res = await self.find_best_alternative(use)
        if res is None:
            msg = f"no reachable url for {reachability}"
            logger.error(msg)
            raise ValueError(msg)
        return res

    async def listen_topic(
        self,
        urlbase: URLIndexer,
        topic_name: TopicNameV,
        cb: Callable[[RawData], Any],
        *,
        inline_data: bool,
        raise_on_error: bool,
    ) -> "asyncio.Task[None]":
        available = await self.ask_topics(urlbase)
        topic = available[topic_name]
        url = cast(URLTopic, await self.choose_best(topic.reachability))

        return await self.listen_url(url, cb, inline_data=inline_data, raise_on_error=raise_on_error)

    async def listen_url(
        self, url_topic: URLTopic, cb: Callable[[RawData], Any], *, inline_data: bool, raise_on_error: bool
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
        it = self.listen_url_events(
            url_events, inline_data=inline_data, raise_on_error=raise_on_error, add_silence=None
        )
        t = asyncio.create_task(_listen_and_callback(it, cb))
        self.tasks.append(t)
        return t

    async def listen_url_events(
        self,
        url_events: URLWS,
        *,
        inline_data: bool,
        raise_on_error: bool,
        add_silence: Optional[float],
    ) -> "AsyncIterator[ListenURLEvents]":
        if inline_data:
            if "?" not in str(url_events):
                raise ValueError(f"inline data requested but no ? in {url_events}")
            async for _ in self.listen_url_events_with_data_inline(
                url_events, raise_on_error=raise_on_error, add_silence=add_silence
            ):
                yield _
        else:
            async for _ in self.listen_url_events_with_data_offline(
                url_events, raise_on_error=raise_on_error, add_silence=add_silence
            ):
                yield _

    async def listen_url_events_with_data_offline(
        self,
        url_websockets: URLWS,
        *,
        raise_on_error: bool,
        add_silence: Optional[float],
    ) -> AsyncIterator[ListenURLEvents]:
        """Iterates using direct data using side loading"""
        use_url: URLString
        nreceived = 0

        async with self.my_session(url_websockets) as (session, use_url):
            ws: ClientWebSocketResponse
            async with session.ws_connect(use_url) as ws:
                #  noinspection PyProtectedMember
                # headers = "".join(f"{k}: {v}\n" for k, v in ws._response.headers.items())
                # logger.info(f"websocket to {url_websockets} ready\n{headers}")

                while True:
                    if ws.closed:
                        if nreceived == 0:
                            s = "Closed, but not even one event received"
                            logger.error(s)
                            yield ErrorMsg(comment=s)
                            if raise_on_error:
                                raise Exception(s)

                        yield FinishedMsg(comment="closed")
                        break
                    wmsg: WSMessage
                    if add_silence is not None:
                        try:
                            wmsg = await asyncio.wait_for(ws.receive(), timeout=add_silence)
                        except asyncio.exceptions.TimeoutError:
                            # logger.debug(f"add_silence {add_silence} expired")
                            yield SilenceMsg(dt=add_silence)
                            continue
                    else:
                        wmsg = await ws.receive()
                    # logger.info(f'raw: {wmsg}')
                    if wmsg.type == aiohttp.WSMsgType.CLOSE:  # aiohttp-specific
                        if wmsg.data == WSCloseCode.OK:
                            if nreceived == 0:
                                s = "Closed, but not even one event received"
                                logger.error(s)
                                yield ErrorMsg(comment=s)
                                if raise_on_error:
                                    raise Exception(s)

                            yield FinishedMsg(comment="closed")
                        else:
                            s = f"Closing with error: {wmsg.data}"
                            logger.error(s)
                            yield ErrorMsg(comment=s)
                            yield FinishedMsg(comment="closed")
                            if raise_on_error:
                                raise Exception(s)
                        break
                    elif wmsg.type == aiohttp.WSMsgType.CLOSING:  # aiohttp-specific
                        if nreceived == 0:
                            s = "Closing, but not even one event received"
                            logger.error(s)
                            yield ErrorMsg(comment=s)
                            if raise_on_error:
                                raise Exception(s)
                        yield FinishedMsg(comment="closing")
                        break
                    elif wmsg.type == aiohttp.WSMsgType.ERROR:
                        s = str(wmsg.data)
                        logger.error(s)
                        yield ErrorMsg(comment=s)
                        if raise_on_error:
                            raise Exception(s)

                    elif wmsg.type == aiohttp.WSMsgType.BINARY:
                        try:
                            cm = channel_msgs_parse(wmsg.data)
                        except Exception as e:
                            msg = f"error in parsing\n{wmsg.data!r}\nerror:\n{e.__class__.__name__} {e!r}"
                            logger.error(msg)
                            yield ErrorMsg(comment=msg)
                            if raise_on_error:
                                raise Exception(msg) from e
                            continue

                        if isinstance(cm, DataReady):
                            try:
                                data = await self._download_from_urls(url_websockets, cm)
                            except Exception as e:
                                # todo: send message
                                msg = f"error in downloading {cm}: {e.__class__.__name__} {e!r}"
                                logger.error(msg)
                                yield ErrorMsg(comment=msg)
                                if raise_on_error:
                                    raise Exception(msg) from e
                                continue

                            yield InsertNotification(data_saved=cm.as_data_saved(), raw_data=data)
                        elif isinstance(cm, ChannelInfo):
                            nreceived += 1
                            # logger.info(f"channel info {cm}")
                            pass
                        elif isinstance(cm, (WarningMsg, ErrorMsg, FinishedMsg)):
                            yield cm
                        else:
                            s = f"cannot interpret"
                            logger.error(s)
                            yield ErrorMsg(comment=s)
                            if raise_on_error:
                                raise Exception(s)

                    else:
                        s = f"unexpected message type {wmsg.type} {wmsg.data!r}"
                        logger.debug(s)
                        yield ErrorMsg(comment=s)
                        if raise_on_error:
                            raise Exception(s)

    async def _download_from_urls(self, urlbase: URL, dr: DataReady) -> RawData:
        url_datas = [join(urlbase, _.url) for _ in dr.availability]

        #  logger.info(f"url_datas {url_datas}")
        if not url_datas:
            logger.error(f"no url_datas in {dr}")
            raise AssertionError(f"no url_datas in {dr}")

        url_data = url_datas[0]
        #  TODO: try multiple urls

        return await self.get(url_data, accept=dr.content_type)

    @async_error_catcher_iterator
    async def listen_url_events_with_data_inline(
        self,
        url_websockets: URLWS,
        raise_on_error: bool,
        add_silence: Optional[float],
    ) -> "AsyncIterator[ListenURLEvents]":
        """Iterates using direct data in websocket."""
        logger.info(f"listen_url_events_with_data_inline {url_websockets}")
        nreceived = 0
        async with self.my_session(url_websockets) as (session, use_url):
            ws: ClientWebSocketResponse
            async with session.ws_connect(use_url) as ws:
                #  noinspection PyProtectedMember
                # headers = "".join(f"{k}: {v}\n" for k, v in ws._response.headers.items())
                # logger.info(f"websocket to {url_websockets} ready\n{headers}")

                try:
                    while True:
                        if ws.closed:
                            if nreceived == 0:
                                yield ErrorMsg(comment="Closed, but not even one event received")

                            yield FinishedMsg(comment="closed")
                            break
                        if add_silence is not None:
                            try:
                                msg = await asyncio.wait_for(ws.receive(), timeout=add_silence)
                            except asyncio.exceptions.TimeoutError:
                                # logger.debug(f"add_silence {add_silence} expired")
                                yield SilenceMsg(dt=add_silence)
                                continue
                        else:
                            msg = await ws.receive()

                        if msg.type == aiohttp.WSMsgType.CLOSE:  # aiohttp-specific
                            if nreceived == 0:
                                yield ErrorMsg(comment="Closed, but not even one event received")

                            yield FinishedMsg(comment="closed")
                            break
                        elif msg.type == aiohttp.WSMsgType.CLOSING:  # aiohttp-specific
                            if nreceived == 0:
                                yield ErrorMsg(comment="Closing, but not even one event received")
                            yield FinishedMsg(comment="closing")
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            yield ErrorMsg(comment=str(msg.data))
                            if raise_on_error:
                                raise Exception(str(msg.data))

                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            try:
                                cm = channel_msgs_parse(msg.data)
                            except Exception as e:
                                s = f"error in parsing {msg.data!r}: {e.__class__.__name__} {e!r}"
                                logger.error(s)
                                yield ErrorMsg(comment=s)
                                if raise_on_error:
                                    raise Exception(s)
                                continue
                            else:
                                if isinstance(cm, DataReady):
                                    dr = cm
                                    if dr.chunks_arriving == 0:
                                        s = f"unexpected chunks_arriving {dr.chunks_arriving} in {dr}"
                                        logger.error(s)
                                        yield ErrorMsg(comment=s)
                                        if raise_on_error:
                                            raise Exception(s)

                                    #  create a byte array initialized at

                                    data = b""
                                    for _ in range(dr.chunks_arriving):
                                        msg = await ws.receive()

                                        cm = channel_msgs_parse(msg.data)

                                        if isinstance(cm, Chunk):
                                            data += cm.data
                                        else:
                                            s = f"unexpected message {msg!r}"
                                            logger.error(s)
                                            yield ErrorMsg(comment=s)
                                            if raise_on_error:
                                                raise Exception(s)
                                            continue

                                    if len(data) != dr.content_length:
                                        s = f"unexpected data length {len(data)} != {dr.content_length}\n{dr}"
                                        logger.error(s)
                                        yield ErrorMsg(comment=s)
                                        if raise_on_error:
                                            raise Exception(
                                                f"unexpected data length {len(data)} != {dr.content_length}"
                                            )

                                    raw_data = RawData(content_type=dr.content_type, content=data)
                                    x = InsertNotification(data_saved=dr.as_data_saved(), raw_data=raw_data)
                                    yield x

                                elif isinstance(cm, ChannelInfo):
                                    nreceived += 1
                                    # logger.info(f"channel info {cm}")
                                elif isinstance(cm, (WarningMsg, ErrorMsg, FinishedMsg)):
                                    yield cm
                                else:
                                    s = f"unexpected message {cm!r}"
                                    logger.error(s)
                                    yield ErrorMsg(comment=s)
                                    if raise_on_error:
                                        raise Exception(s)

                        else:
                            s = f"unexpected message type {msg.type} {msg.data!r}"
                            logger.error(s)
                            yield ErrorMsg(comment=s)
                            if raise_on_error:
                                raise Exception(s)
                            continue

                except Exception as e:
                    msg = str(e)[:100]
                    await ws.close(code=WSCloseCode.ABNORMAL_CLOSURE, message=msg.encode())
                    logger.error(f"error in websocket {traceback.format_exc()}")
                    raise
                else:
                    await ws.close(code=WSCloseCode.OK)

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
                    async def push_through(self, data: bytes, content_type: ContentType) -> None:
                        rd = RawData(content_type=content_type, content=data)
                        as_struct = {RawData.__name__: asdict(rd)}
                        cbor_data = cbor2.dumps(as_struct)

                        await ws.send_bytes(cbor_data)

                yield PushInterfaceImpl()

    @async_error_catcher_iterator
    async def listen_continuous(
        self,
        urlbase0: URL,
        expect_node: Optional[NodeID],
        *,
        switch_identity_ok: bool,
        raise_on_error: bool,
        add_silence: Optional[float],
        inline_data: bool,
    ) -> AsyncIterator[ListenURLEvents]:
        while True:
            try:
                md = await self.get_metadata(urlbase0)
            except Exception as e:
                msg = f"Error getting metadata for {urlbase0!r}: {e!r}"
                logger.error(msg)

                if raise_on_error:
                    raise Exception(msg) from e

                await asyncio.sleep(1.0)
                continue

            # logger.info(
            #     f"Metadata for {urlbase0!r}:\n" + json.dumps(asdict(md), indent=2)
            # )  # available = await dtpsclient.ask_topics(urlbase0)

            if md.answering is None:
                msg = f"This is not a DTPS node."
                logger.error(msg)
                if raise_on_error:
                    raise Exception(msg)
                await asyncio.sleep(2.0)
                continue

            if expect_node is not None and md.answering != expect_node:
                if switch_identity_ok:
                    msg = f"Switching identity to {md.answering!r}."
                    logger.info(msg)
                else:
                    msg = f"This is not the expected node {expect_node!r}."
                    logger.error(msg)
                    if raise_on_error:
                        raise Exception(msg)
                    await asyncio.sleep(2.0)
                    continue

            expect_node = md.answering

            if md.events_url is None and md.events_data_inline_url == "":
                msg = f"This resource does not support events."
                logger.error(msg)
                if raise_on_error:
                    raise Exception(msg)
                await asyncio.sleep(2.0)
                continue

            if not inline_data:
                if md.events_url is None:
                    msg = f"This resource does not support events."
                    logger.error(msg)
                    if raise_on_error:
                        raise Exception(msg)
                    await asyncio.sleep(2.0)
                    continue

                iterator = self.listen_url_events(
                    md.events_url, raise_on_error=raise_on_error, inline_data=False, add_silence=add_silence
                )
            else:
                if md.events_data_inline_url is None:
                    msg = f"This resource does not support inline data events."
                    logger.error(msg)
                    if raise_on_error:
                        raise Exception(msg)
                    await asyncio.sleep(2.0)
                    continue
                iterator = self.listen_url_events(
                    md.events_data_inline_url,
                    raise_on_error=raise_on_error,
                    inline_data=True,
                    add_silence=add_silence,
                )

            should_break_outer = False
            try:
                async for _ in iterator:
                    try:
                        yield _
                    except StopContinuousLoop as e:
                        logger.error(f"obtained {e}")
                        should_break_outer = True

                        break
            except Exception as e:
                msg = f"Error listening to {urlbase0!r}:\n{traceback.format_exc()}"
                logger.error(msg)
                if raise_on_error:
                    raise Exception(msg) from e
                await asyncio.sleep(1.0)
                continue

            if should_break_outer:
                break
            await asyncio.sleep(1.0)


class PushInterface(ABC):
    @abstractmethod
    async def push_through(self, data: bytes, content_type: ContentType) -> None:
        ...


def escape_json_pointer(s: str) -> str:
    return s.replace("~", "~0").replace("/", "~1")


async def _listen_and_callback(it: AsyncIterator[ListenURLEvents], cb: Callable[[RawData], Any]) -> None:
    async for lue in it:
        if isinstance(lue, InsertNotification):
            cb(lue.raw_data)


async def my_raise_for_status(resp, url0) -> None:
    if not resp.ok:
        # reason should always be not None for a started response
        assert resp.reason is not None
        msg = await resp.read()
        # msg = msg.decode("utf-8")
        message = f"url0: {url0}\n{resp.reason}\n---\n{msg}---"
        resp.release()
        raise ClientResponseError(
            resp.request_info,
            resp.history,
            status=resp.status,
            message=message,
            headers=resp.headers,
        )


class StopContinuousLoop(Exception):
    pass
