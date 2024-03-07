import asyncio
import json
import os
import traceback
from abc import ABC, abstractmethod
from asyncio import CancelledError, Event
from contextlib import asynccontextmanager, AsyncExitStack
from dataclasses import asdict, dataclass
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    cast,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TYPE_CHECKING,
    TypeVar,
)
from urllib.parse import unquote

import aiohttp
import cbor2
import jsonpatch
from aiohttp import (
    ClientResponse,
    ClientResponseError,
    ClientWebSocketResponse,
    TCPConnector,
    UnixConnector,
    WSCloseCode,
)
from multidict import CIMultiDictProxy
from tcp_latency import measure_latency

from . import logger, logger as logger0
from .constants import (
    CONTENT_TYPE_PATCH_CBOR,
    CONTENT_TYPE_PATCH_JSON,
    HEADER_CONTENT_LOCATION,
    HEADER_DATA_ORIGIN_NODE_ID,
    HEADER_NODE_ID,
    MIME_JSON,
    MIME_OCTET,
    REL_CONNECTIONS,
    REL_EVENTS_DATA,
    REL_EVENTS_NODATA,
    REL_HISTORY,
    REL_META,
    REL_PROXIED,
    REL_STREAM_PUSH,
    TOPIC_PROXIED,
    HTTP_TIMEOUT,
)
from .exceptions import EventListeningNotAvailable, NoSuchTopic
from .link_headers import get_link_headers
from .structures import (
    ChannelInfo,
    ChannelMsgs,
    Chunk,
    ConnectionEstablished,
    ConnectionJob,
    DataReady,
    ErrorMsg,
    FinishedMsg,
    ForwardingStep,
    InsertNotification,
    LinkBenchmark,
    ListenURLEvents,
    ProxyJob,
    PushResult,
    RawData,
    SilenceMsg,
    TopicReachability,
    TopicRefAdd,
    TopicsIndex,
    TopicsIndexWire,
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
from .utils import (
    async_error_catcher,
    check_is_unix_socket,
    method_lru_cache,
    parse_cbor_tagged,
    pretty,
)

__all__ = [
    "DTPSClient",
    "StopContinuousLoop",
    "escape_json_pointer",
    "my_raise_for_status",
    "unescape_json_pointer",
]

U = TypeVar("U", bound=URL)

X = TypeVar("X")


@dataclass
class FoundMetadata:
    # The url that was used to get the metadata
    origin: URLTopic

    # url alternatives (Location: headers)
    alternative_urls: List[URLTopic]

    # NodeID if answering is a DTPS node
    answering: Optional[NodeID]

    origin_node: Optional[NodeID]  # # HEADER_DATA_ORIGIN_NODE_ID

    # websocket with offline data
    events_url: Optional[URLWSOffline]

    # websocket with inline data
    events_data_inline_url: Optional[URLWSInline]

    # metadati della risorsa (il ContentInfo etc.)
    meta_url: Optional[URL]

    # history url
    history_url: Optional[URL]

    # url for stream push
    stream_push_url: Optional[URLWS]

    connections_url: Optional[URL]

    proxied_url: Optional[URL]

    raw_headers: CIMultiDictProxy[str]


class ShutdownAsked(Exception):
    pass


class ListenDataInterface(ABC):
    @abstractmethod
    async def stop(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def wait_for_done(self) -> None:
        raise NotImplementedError()

    async def wait_for_done_or_stop_on_event(self, shutdown_event: asyncio.Event) -> None:
        wait1 = asyncio.create_task(shutdown_event.wait())
        wait2 = asyncio.create_task(self.wait_for_done())
        done, pending = await asyncio.wait([wait1, wait2], return_when=asyncio.FIRST_COMPLETED)
        if shutdown_event.is_set():
            wait2.cancel()
            await self.stop()
        else:
            return


@dataclass
class ListenDataImpl(ListenDataInterface):
    stop_condition: asyncio.Event
    task: "asyncio.Task[None]"

    async def stop(self):
        self.stop_condition.set()
        # self.task.cancel()
        await self.wait_for_done()
        try:
            await asyncio.wait_for(self.task, 5)
        except asyncio.TimeoutError:
            msg = f"ListenDataImpl: stop: timeout waiting for {self.task}"
            logger0.error(msg)
            self.task.cancel()
            return

    async def wait_for_done(self):
        try:
            await self.task
        except CancelledError:
            if self.task.done():
                return
            else:
                raise
            # note: cancelling() only for >= 3.11
            # if asyncio.current_task().cancelling() > 0:
            #     # propagate the exception up normally
            #     raise


class ConditionSatistied(Exception):
    pass


class DTPSClient:
    if TYPE_CHECKING:

        @classmethod
        def create(
            cls, nickname: Optional[str] = None, shutdown_event: Optional[asyncio.Event] = None
        ) -> "AsyncContextManager[DTPSClient]":
            ...

    else:

        @classmethod
        @asynccontextmanager
        async def create(
            cls, nickname: Optional[str] = None, shutdown_event: Optional[asyncio.Event] = None
        ) -> "AsyncIterator[DTPSClient]":
            ob = cls(nickname=nickname, shutdown_event=shutdown_event)
            await ob.init()
            try:
                yield ob
            finally:
                await ob.aclose()

    def __init__(self, nickname: Optional[str], shutdown_event: Optional[asyncio.Event]) -> None:
        if shutdown_event is None:
            shutdown_event = asyncio.Event()

        self.shutdown_event = shutdown_event
        self.S = AsyncExitStack()
        self.tasks = []
        self.sessions = {}
        self.preferred_cache = {}
        self.blacklist_protocol_host_port = set()
        self.obtained_answer = {}
        if nickname is None:
            nickname = str(id(self))
        self.nickname = nickname
        self.logger = logger0.getChild(nickname)
        self.shutdown_event = asyncio.Event()

    def remember_task(self, task: asyncio.Task) -> None:
        self.tasks.append(task)

    tasks: "List[asyncio.Task[Any]]"
    blacklist_protocol_host_port: Set[Tuple[str, str, int]]
    obtained_answer: Dict[Tuple[str, str, int], Optional[NodeID]]

    preferred_cache: Dict[URL, URL]
    sessions: Dict[str, aiohttp.ClientSession]
    shutdown_event: asyncio.Event

    async def init(self) -> None:
        pass

    async def aclose(self) -> None:
        # self.logger.debug(f"DTPSClient: aclose: setting shutdown event")
        self.shutdown_event.set()
        for t in self.tasks:
            t.cancel()
        # self.logger.debug(f"DTPSClient: aclose: gathering")
        # await asyncio.gather(*self.tasks, return_exceptions=True)
        # self.logger.debug(f"DTPSClient: aclose: closing S")
        await self.S.aclose()
        # self.logger.debug(f"DTPSClient: aclose done")

    async def ask_index(self, url0: URLIndexer) -> TopicsIndex:
        url = self._look_cache(url0)
        async with self.my_session(url) as (session, use_url):
            async with session.get(use_url) as resp:
                await my_raise_for_status(resp, url0)
                # answering = resp.headers.get(HEADER_NODE_ID)

                #  logger.debug(f"ask topics {resp.headers}")
                if (preferred := await self.prefer_alternative(url, resp)) is not None:
                    self.logger.debug(f"Using preferred alternative to {url} -> {repr(preferred)}")
                    return await self.ask_index(preferred)
                assert resp.status == 200, resp.status
                res_bytes: bytes = await resp.read()
                res = cbor2.loads(res_bytes)

            alternatives0 = cast(List[URLString], resp.headers.getall(HEADER_CONTENT_LOCATION, []))
            where_this_available: List[URL] = [url]
            for a in alternatives0:
                try:
                    x = parse_url_unescape(a)
                except Exception:
                    self.logger.exception(f"cannot parse {a}")
                    continue
                else:
                    where_this_available.append(x)

            s = TopicsIndexWire.from_json(res)
            q = s.to_internal([url])
            return q

    def _look_cache(self, url0: U) -> U:
        return cast(U, self.preferred_cache.get(url0, url0))

    async def publish(self, url0: URL, rd: RawData) -> None:
        url = self._look_cache(url0)

        headers = {"content-type": rd.content_type}

        async with self.my_session(url) as (session, use_url):
            async with session.post(use_url, data=rd.content, headers=headers) as resp:
                await my_raise_for_status(resp, url0)
                assert resp.status in [200, 201], resp
                await self.prefer_alternative(url, resp)

    async def call(self, url0: URL, rd: RawData) -> RawData:
        url = self._look_cache(url0)

        headers = {"content-type": rd.content_type}

        async with self.my_session(url) as (session, use_url):
            async with session.post(use_url, data=rd.content, headers=headers) as resp:
                await my_raise_for_status(resp, url0)
                assert resp.status in [200, 201], resp
                await self.prefer_alternative(url, resp)
                location = resp.headers.get("Location")
                if not location:
                    raise ValueError(f"no location header in response to call for {url} {resp}")

                url_redirect = join(url, location)
            return await self.get(url_redirect, accept=None)

    async def prefer_alternative(self, current: U, resp: aiohttp.ClientResponse) -> Optional[U]:
        assert isinstance(current, URL), current
        if current in self.preferred_cache:
            return cast(U, self.preferred_cache[current])

        nothing: List[URLString] = []
        alternatives0 = cast(List[URLString], resp.headers.getall(HEADER_CONTENT_LOCATION, nothing))

        if not alternatives0:
            return None
        alternatives: list[URL] = [current]
        for a in alternatives0:
            try:
                x = parse_url_unescape(a)
            except Exception:
                self.logger.exception(f"cannot parse {a}")
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
        # this_partial_url: URLString,
        connects_to: URLTopic,
        expects_answer_from: NodeID,
        forwarders: List[ForwardingStep],
    ) -> Optional[TopicReachability]:
        assert isinstance(connects_to, URL), connects_to
        # assert isinstance(this_partial_url, str), this_partial_url
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
            url=url_to_string(connects_to),
            answering=this_node_id,
            forwarders=forwarders + [me],
            benchmark=total,
        )
        return tr2

    async def find_best_alternative(self, us: Sequence[Tuple[U, Optional[NodeID]]]) -> Optional[U]:
        if not us:
            self.logger.warning("find_best_alternative: no alternatives")
            return None
        results: List[str] = []
        possible: List[Tuple[float, float, float, U]] = []
        for a, expects_answer_from in us:
            assert isinstance(a, URL), a
            if (score := await self.can_use_url(a, expects_answer_from)) is not None:
                possible.append((score.complexity, score.latency_ns, -score.bandwidth, a))
                results.append(f"✓ {str(a):<60} -> {score}")
            else:
                results.append(f"✗ {a} ")

        possible.sort(key=lambda x: (x[0], x[1]))
        if not possible:
            rs = "\n".join(results)
            self.logger.warning(
                f"find_best_alternative: no alternatives found:\n {rs}",
            )
            return None
        best = possible[0][-1]

        results.append(f"best: {best}")
        self.logger.debug("\n".join(results))

        return best

    @method_lru_cache()
    def measure_latency(self, host: str, port: int) -> Optional[float]:
        self.logger.debug(f"computing latency to {host}:{port}...")
        res = cast(List[float], measure_latency(host, port, runs=5, wait=0.01, timeout=0.5))

        if not res:
            self.logger.debug(f"latency to {host}:{port} -> unreachable")
            return None

        latency_seconds = (sum(res) / len(res)) / 1000.0

        self.logger.debug(f"latency to {host}:{port} is  {latency_seconds}s  [{res}]")
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
            self.logger.debug(f"blacklisted {url}")
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

                if expects_answer_from is not None and who_answers != expects_answer_from:
                    msg = f"can_use_url: wrong {who_answers=} header in {url}, expected {expects_answer_from}"
                    self.logger.error(msg)

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
            host = url.host
            self.logger.debug(f"checking {url}...  path={repr(url)}")
            if not os.path.exists(host):
                self.logger.warning(f" {url}: {host=!r} does not exist")
                return None
            who_answers = await self.get_who_answers(url)

            if expects_answer_from is not None and who_answers != expects_answer_from:
                msg = f"wrong {who_answers=} header in {url}, expected {expects_answer_from}"
                self.logger.error(msg)

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

        self.logger.warning(f"unknown scheme {url.scheme!r} for {url}")
        return None

    async def get_who_answers(self, url: URLTopic) -> Optional[NodeID]:
        key = (url.scheme, url.host, url.port or 0)
        if key not in self.obtained_answer:
            try:
                md = await self.get_metadata(url)
                # logger.warning(f"checking {url} -> {md}")
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
            except CancelledError:
                raise
            except:
                self.logger.exception(f"error checking {url} {traceback.format_exc()}")
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
        ) -> AsyncContextManager[Tuple[aiohttp.ClientSession, URLString]]:
            ...

    else:

        @asynccontextmanager
        async def my_session(
            self, url: URL, /, *, conn_timeout: Optional[float] = None
        ) -> AsyncIterator[Tuple[aiohttp.ClientSession, URLString]]:
            assert isinstance(url, URL), url
            if url.scheme == "http+unix":
                if url.host is None:
                    raise AssertionError(f"no host in {url!r}")
                path = unquote(url.host)
                connector = UnixConnector(path=path)
                #  noinspection PyProtectedMember
                use_url = url_to_string(url._replace(scheme="http", host="localhost"))

                try:
                    check_is_unix_socket(path)
                except ValueError as e:
                    msg = f"Cannot connect to url because the path does not exist: {url!r}"
                    raise ValueError(msg) from e

            elif url.scheme in ("http", "https"):
                connector = TCPConnector()
                use_url = url_to_string(url)
            else:
                raise ValueError(f"unknown scheme {url.scheme!r} for {repr(url)}")

            timeout = aiohttp.ClientTimeout(total=conn_timeout)
            async with connector:
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                    # self.logger.debug(f"my_session: {url} -> {use_url}")
                    yield session, use_url

    async def get_proxied(self, url0: URLIndexer) -> Dict[TopicNameV, ProxyJob]:
        # FIXME: need to use REL_PROXIED
        url = join(url0, TOPIC_PROXIED.as_relative_url())
        rd = await self.get(url, accept=MIME_JSON)

        js = json.loads(rd.content)
        res: Dict[TopicNameV, ProxyJob] = {}
        for k, v in js.items():
            res[TopicNameV.from_dash_sep(k)] = ProxyJob.from_json(v)
        return res

    async def add_proxy(
        self,
        url0: URLIndexer,
        topic_name: TopicNameV,
        node_id: Optional[NodeID],
        urls: List[URLString],
        mask_origin: bool,
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
        # add
        proxy_job = ProxyJob(node_id, urls, mask_origin)
        patch.append({"op": "add", "path": path, "value": asdict(proxy_job)})
        # compile patch
        as_json = json.dumps(patch).encode("utf-8")
        # FIXME: need to use REL_PROXIED
        url = join(url0, TOPIC_PROXIED.as_relative_url())
        res = await self.patch(url, CONTENT_TYPE_PATCH_JSON, as_json)
        return True

    async def remove_proxy(self, url0: URLIndexer, topic_name: TopicNameV) -> None:
        patch = [{"op": "remove", "path": "/" + escape_json_pointer(topic_name.as_dash_sep())}]
        as_json = json.dumps(patch).encode("utf-8")
        # FIXME: need to use REL_PROXIED
        url = join(url0, TOPIC_PROXIED.as_relative_url())
        res = await self.patch(url, CONTENT_TYPE_PATCH_JSON, as_json)

    async def add_topic(self, url0: URLIndexer, topic_name: TopicNameV, tra: TopicRefAdd) -> None:
        path = "/" + escape_json_pointer(topic_name.as_dash_sep())
        patch = jsonpatch.JsonPatch(
            [
                {"op": "add", "path": path, "value": asdict(tra)},
            ]
        )
        patch_json = patch.to_string().encode()

        res = await self.patch(url0, CONTENT_TYPE_PATCH_JSON, patch_json)

    async def patch(self, url0: URL, content_type: Optional[str], data: bytes) -> RawData:
        headers = {"content-type": content_type} if content_type is not None else {}

        url = self._look_cache(url0)
        use_url = None
        try:
            async with self.my_session(url, conn_timeout=HTTP_TIMEOUT) as (session, use_url):
                async with session.patch(use_url, data=data, headers=headers) as resp:
                    res_bytes: bytes = await resp.read()
                    content_type = ContentType(resp.headers.get("content-type", MIME_OCTET))
                    rd = RawData(content=res_bytes, content_type=content_type)

                    if not resp.ok:
                        try:
                            message = res_bytes.decode("utf-8")
                        except:
                            message = res_bytes
                        raise ValueError(f"cannot patch {url0=!r} {use_url=!r} {resp=!r}\n{message}")

                    return rd

        except CancelledError:
            raise
        except:
            self.logger.error(f"cannot connect to {url=!r} {use_url=!r} \n{traceback.format_exc()}")
            raise

    async def get(self, url0: URL, accept: Optional[str]) -> RawData:
        headers = {}
        if accept is not None:
            headers["accept"] = accept

        url = self._look_cache(url0)
        use_url = None
        try:
            async with self.my_session(url, conn_timeout=HTTP_TIMEOUT) as (session, use_url):
                async with session.get(use_url) as resp:
                    res_bytes: bytes = await resp.read()
                    content_type = ContentType(resp.headers.get("content-type", "application/octet-stream"))
                    rd = RawData(content=res_bytes, content_type=content_type)

                    if not resp.ok:
                        try:
                            message = res_bytes.decode("utf-8")
                        except:
                            message = res_bytes
                        resp: ClientResponse = resp
                        # TODO: error 500 should raise a ValueError instead. This is needed until DTSW-5206 is fixed
                        if resp.status in [404, 500]:
                            raise NoSuchTopic(f"cannot GET {url0=!r}\n{use_url=!r}\n{resp=!r}\n{message}")
                        raise ValueError(f"cannot GET {url0=!r}\n{use_url=!r}\n{resp=!r}\n{message}")

                    if accept is not None and content_type != accept:
                        raise ValueError(
                            f"GET gave a different content type ({accept=!r}, {content_type}\n{url0=}"
                        )
                    return rd
        except CancelledError:
            raise
        except NoSuchTopic:
            raise
        except:
            self.logger.error(f"cannot connect to {url=!r} {use_url=!r} \n{traceback.format_exc()}")
            raise

    async def delete(self, url0: URL) -> None:
        headers = {}

        url = self._look_cache(url0)
        use_url = None
        async with self.my_session(url, conn_timeout=HTTP_TIMEOUT) as (session, use_url):
            async with session.delete(use_url) as resp:
                res_bytes: bytes = await resp.read()
                resp.raise_for_status()

    async def get_metadata(self, url0: URLTopic) -> FoundMetadata:
        url = self._look_cache(url0)
        use_url = None
        try:
            async with self.my_session(url, conn_timeout=HTTP_TIMEOUT) as (session, use_url):
                async with session.head(use_url) as resp:
                    await my_raise_for_status(resp, url0)

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

                    if REL_STREAM_PUSH in links:
                        stream_push_url = cast(URLWS, join(url, links[REL_STREAM_PUSH].url))
                    else:
                        stream_push_url = None

                    if REL_META in links:
                        meta_url = join(url, links[REL_META].url)
                    else:
                        meta_url = None

                    if REL_CONNECTIONS in links:
                        connections_url = join(url, links[REL_CONNECTIONS].url)
                    else:
                        connections_url = None

                    if REL_PROXIED in links:
                        proxied_url = join(url, links[REL_PROXIED].url)
                    else:
                        proxied_url = None

                    if REL_HISTORY in links:
                        history_url = join(url, links[REL_HISTORY].url)
                    else:
                        history_url = None

                    if HEADER_NODE_ID not in resp.headers:
                        answering = None
                    else:
                        answering = NodeID(resp.headers[HEADER_NODE_ID])

                    if HEADER_DATA_ORIGIN_NODE_ID not in resp.headers:
                        origin_node = None
                    else:
                        origin_node = NodeID(resp.headers[HEADER_DATA_ORIGIN_NODE_ID])

        except:
            #  (TimeoutError, ClientConnectorError):
            # logger.error(f"cannot connect to {url0=!r} {use_url=!r} \n{traceback.format_exc()}")

            #  return FoundMetadata([], None, None, None)
            raise
        urls = [cast(URLTopic, join(url, _)) for _ in alternatives0]
        return FoundMetadata(
            url,
            urls,
            answering=answering,
            events_url=events_url,
            origin_node=origin_node,
            events_data_inline_url=events_url_data,
            meta_url=meta_url,
            stream_push_url=stream_push_url,
            history_url=history_url,
            connections_url=connections_url,
            raw_headers=resp.headers,
            proxied_url=proxied_url,
        )

    async def choose_best(self, reachability: List[TopicReachability]) -> URL:
        use: List[Tuple[URL, Optional[NodeID]]] = []
        for r in reachability:
            try:
                x = parse_url_unescape(r.url)
            except Exception:
                self.logger.exception(f"cannot parse {r.url}")
                continue
            else:
                use.append((x, r.answering))
        res = await self.find_best_alternative(use)
        if res is None:
            msg = f"no reachable url for {reachability}"
            self.logger.error(msg)
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
    ) -> ListenDataInterface:
        available = await self.ask_index(urlbase)
        topic = available.topics[topic_name]
        url = cast(URLTopic, await self.choose_best(topic.reachability))

        return await self.listen_url(url, cb, inline_data=inline_data, raise_on_error=raise_on_error)

    async def connect(
        self,
        url_index: URLIndexer,
        connection_name: TopicNameV,
        connection_job: ConnectionJob,
    ) -> None:
        url_index = self._look_cache(url_index)
        metadata = await self.get_metadata(url_index)
        if metadata.connections_url is None:
            msg = f"Connection functionality not available: {pretty(metadata)}"
            raise ValueError(msg)

        path = "/" + escape_json_pointer(connection_name.as_dash_sep())
        wire = connection_job.to_wire()
        op = {"op": "add", "path": path, "value": asdict(wire)}
        ops = [op]
        data = cbor2.dumps(ops)
        await self.patch(
            metadata.connections_url,
            CONTENT_TYPE_PATCH_CBOR,
            data,
        )

    async def disconnect(
        self,
        url_index: URLIndexer,
        connection_name: TopicNameV,
    ) -> None:
        url_index = self._look_cache(url_index)
        metadata = await self.get_metadata(url_index)
        if metadata.connections_url is None:
            msg = f"Connection functionality not available: {pretty(metadata)}"
            raise ValueError(msg)

        path = "/" + escape_json_pointer(connection_name.as_dash_sep())
        op = {"op": "remove", "path": path}
        ops = [op]
        data = cbor2.dumps(ops)
        await self.patch(
            metadata.connections_url,
            CONTENT_TYPE_PATCH_CBOR,
            data,
        )

    async def listen_url(
        self,
        url_topic: URLTopic,
        cb: Callable[[RawData], Awaitable[None]],
        *,
        inline_data: bool,
        raise_on_error: bool,
        connection_timeout: float = 10,
        # stop_condition: Optional[asyncio.Event] = None,
    ) -> ListenDataInterface:
        url_topic = self._look_cache(url_topic)
        metadata = await self.get_metadata(url_topic)

        if inline_data:
            if metadata.events_data_inline_url is not None:
                url_events = metadata.events_data_inline_url
            else:
                msg = (
                    f"cannot find field events_data_inline_url for url\n  {url_to_string(url_topic)}\n  "
                    f"{metadata=}"
                )
                raise EventListeningNotAvailable(msg)

        else:
            if metadata.events_url is not None:
                url_events = metadata.events_url
            else:
                msg = f"cannot find events_url for\n  {url_to_string(url_topic)}\n  {metadata=}"
                raise EventListeningNotAvailable(msg)

        # logger.info(f"listening to  {url_topic} -> {metadata} -> {url_events}")
        # desc = f"{url_topic} inline={inline_data}"

        connection_event = asyncio.Event()

        async def filter_data(lue: ListenURLEvents) -> None:
            # logger.debug(f"filter_data: {lue}")
            if isinstance(lue, ErrorMsg):
                logger.error(f"error in {url_events}: {lue.comment}")
            elif isinstance(lue, WarningMsg):
                logger.warning(f"warning in {url_events}: {lue.comment}")
            elif isinstance(lue, SilenceMsg):
                logger.debug(f"silence in {url_events}: {lue.comment}")
            elif isinstance(lue, FinishedMsg):
                logger.debug(f"finished in {url_events}: {lue.comment}")
            elif isinstance(lue, ConnectionEstablished):
                logger.debug(f"connection established in {url_events}")

                connection_event.set()
            elif isinstance(lue, InsertNotification):
                try:
                    await cb(lue.raw_data)
                except CancelledError:
                    raise
                except:
                    logger.error(f"error in handler: {traceback.format_exc()}")
                    return
            else:
                logger.error(f"unknown {lue}")
                raise ValueError(f"unknown {lue}")

        li = await self.listen_url_events3(
            url_events,
            inline_data=inline_data,
            raise_on_error=raise_on_error,
            add_silence=None,
            callback=filter_data,  # stop_condition=stop_condition
        )

        await asyncio.wait_for(connection_event.wait(), timeout=connection_timeout)

        return li

    async def listen_url_events3(
        self,
        url_events: URLWS,
        *,
        inline_data: bool,
        raise_on_error: bool,
        add_silence: Optional[float],
        callback: Callable[[ListenURLEvents], Awaitable[None]],
        # stop_condition: "Optional[asyncio.Event]",
    ) -> ListenDataInterface:
        # if stop_condition is None:
        stop_condition = asyncio.Event()
        # if inline_data:
        # if "?" not in url_to_string(url_events):
        #     raise ValueError(f"inline data requested but no ? in {url_events}")
        task = asyncio.create_task(
            self.listen_url_events_(
                url_events,
                inline_data=inline_data,
                raise_on_error=raise_on_error,
                add_silence=add_silence,
                callback=callback,
                stop_condition=stop_condition,
            )
        )
        # else:
        #     task = asyncio.create_task(
        #         self.listen_url_events_with_data_offline(
        #             url_events,
        #             raise_on_error=raise_on_error,
        #             add_silence=add_silence,
        #             callback=callback,
        #             stop_condition=stop_condition,
        #         )
        #     )
        # self.remember_task(task)

        return ListenDataImpl(stop_condition, task)

    async def _wait_until_shutdown(self, a: "asyncio.Task[X]", condition: Event) -> X:
        """Waits for an event, or for the shutdown event. In that case we raise ShutdownAsked.
        if the condition is set, we raise ConditionSatistied.
        """
        t_wait = asyncio.create_task(self.shutdown_event.wait())
        condition_wait = asyncio.create_task(condition.wait())
        tasks = [t_wait, a, condition_wait]
        try:
            finished, unfinished = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        except CancelledError:
            a.cancel()
            t_wait.cancel()
            condition_wait.cancel()
            raise

        if self.shutdown_event.is_set():
            a.cancel()
            condition_wait.cancel()
            raise ShutdownAsked()
        elif condition.is_set():
            a.cancel()
            t_wait.cancel()
            raise ConditionSatistied()

        else:
            t_wait.cancel()
            assert len(finished) == 1
            return finished.pop().result()

    async def _download_from_urls(self, urlbase: URL, dr: DataReady) -> RawData:
        url_datas = [join(urlbase, _.url) for _ in dr.availability]

        #  logger.info(f"url_datas {url_datas}")
        if not url_datas:
            self.logger.error(f"no url_datas in {dr}")
            raise AssertionError(f"no url_datas in {dr}")

        #  TODO: DTSW-4781: try multiple urls
        url_data = url_datas[0]

        return await self.get(url_data, accept=dr.content_type)

    #
    # async def wait_for_next_message(
    #     self,
    #     ws: ClientWebSocketResponse,
    #     stop_condition: "asyncio.Event",
    #     on_silence=Optional[Tuple[float, Callable]],
    # ) -> WSMessage:
    #     while True:
    #         msg = await ws.receive()
    #         if msg.type == aiohttp.WSMsgType.CLOSE:
    #             pass

    @async_error_catcher
    async def listen_url_events_(
        self,
        url_websockets: URLWS,
        raise_on_error: bool,
        inline_data: bool,
        add_silence: Optional[float],
        callback: Callable[[ListenURLEvents], Awaitable[None]],
        stop_condition: "asyncio.Event",
    ) -> None:
        """Iterates using direct data in websocket."""
        # self.logger.info(f"listen_url_events_with_data_inline {url_websockets}")
        nreceived = 0
        received_first = False
        async with self.my_session(url_websockets) as (session, use_url):
            ws: ClientWebSocketResponse
            async with session.ws_connect(use_url) as ws:
                # await callback(ConnectionEstablished(comment=f"opened session to {url_websockets}"))
                #  noinspection PyProtectedMember
                # headers = "".join(f"{k}: {v}\n" for k, v in ws._response.headers.items())
                # logger.info(f"websocket to {url_websockets} ready\n{headers}")
                try:
                    while not stop_condition.is_set():
                        if ws.closed:
                            if nreceived == 0:
                                await callback(ErrorMsg(comment="Closed, but not even one event received"))

                            await callback(FinishedMsg(comment="closed"))
                            break

                        wmsg_task = asyncio.create_task(
                            self._wait_until_shutdown(asyncio.create_task(ws.receive()), stop_condition)
                        )

                        try:
                            if add_silence is not None:
                                try:
                                    msg = await asyncio.wait_for(wmsg_task, timeout=add_silence)
                                except asyncio.exceptions.TimeoutError:
                                    # logger.debug(f"add_silence {add_silence} expired")
                                    await callback(
                                        SilenceMsg(dt=add_silence, comment=f"nreceived={nreceived}")
                                    )
                                    continue
                            else:
                                msg = await wmsg_task
                        except ShutdownAsked:
                            msg = f"shutdown asked: ending listen_url"
                            await callback(FinishedMsg(comment=msg))
                            break
                        except ConditionSatistied:
                            msg = f"condition satisfied: ending listen_url"
                            await callback(FinishedMsg(comment=msg))
                            break

                        if not received_first:
                            await callback(ConnectionEstablished(comment=f"received {msg}"))
                            received_first = True

                        if msg.type == aiohttp.WSMsgType.CLOSE:  # aiohttp-specific
                            if nreceived == 0:
                                await callback(ErrorMsg(comment="Closed, but not even one event received"))

                            await callback(FinishedMsg(comment="closed"))
                            break
                        elif msg.type == aiohttp.WSMsgType.CLOSING:  # aiohttp-specific
                            if nreceived == 0:
                                await callback(ErrorMsg(comment="Closing, but not even one event received"))
                            await callback(FinishedMsg(comment="closing"))
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            await callback(ErrorMsg(comment=str(msg.data)))
                            if raise_on_error:
                                raise Exception(str(msg.data))
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            try:
                                cm: ChannelMsgs = channel_msgs_parse(msg.data)
                            except Exception as e:
                                s = f"error in parsing {msg.data!r}: {e.__class__.__name__}:\n{e}"
                                self.logger.error(s)
                                await callback(ErrorMsg(comment=s))
                                if raise_on_error:
                                    raise Exception(s)
                                continue
                            else:
                                if isinstance(cm, DataReady):
                                    dr = cm

                                    if inline_data:
                                        if dr.chunks_arriving == 0:
                                            s = (
                                                f"unexpected chunks_arriving {dr.chunks_arriving} in {dr}, "
                                                f"{inline_data=}"
                                            )
                                            self.logger.error(s)
                                            await callback(ErrorMsg(comment=s))
                                            if raise_on_error:
                                                raise Exception(s)

                                        #  create a byte array initialized at

                                        data = b""
                                        for _ in range(dr.chunks_arriving):
                                            msg = await ws.receive()
                                            cm = channel_msgs_parse(msg.data)  # FIXME: need to use primitives

                                            if isinstance(cm, Chunk):
                                                data += cm.data
                                            else:
                                                s = f"unexpected message while waiting for chunks {msg!r}"
                                                self.logger.error(s)
                                                await callback(ErrorMsg(comment=s))
                                                if raise_on_error:
                                                    raise Exception(s)
                                                continue

                                        if len(data) != dr.content_length:
                                            s = (
                                                f"unexpected data length {len(data)} != "
                                                f"{dr.content_length}\n{dr}"
                                            )
                                            self.logger.error(s)
                                            await callback(ErrorMsg(comment=s))
                                            if raise_on_error:
                                                raise Exception(
                                                    f"unexpected data length {len(data)} != "
                                                    f"{dr.content_length}"
                                                )

                                        raw_data = RawData(content_type=dr.content_type, content=data)
                                        x = InsertNotification(
                                            data_saved=dr.as_data_saved(), raw_data=raw_data
                                        )
                                        await callback(x)
                                    else:
                                        if dr.chunks_arriving > 0:
                                            s = (
                                                f"unexpected chunks_arriving {dr.chunks_arriving} in {dr}, "
                                                f"{inline_data=}"
                                            )
                                            self.logger.error(s)
                                            await callback(ErrorMsg(comment=s))
                                            if raise_on_error:
                                                raise Exception(s)

                                        try:
                                            # TODO: re-use the same session for gets
                                            # logger.debug(f"downloading {url_websockets} from {cm}")
                                            data = await self._download_from_urls(url_websockets, cm)
                                        except Exception as e:
                                            msg = f"error in downloading {cm}: {e.__class__.__name__} {e!r}"
                                            self.logger.error(msg)
                                            await callback(ErrorMsg(comment=msg))
                                            if raise_on_error:
                                                await ws.close(message=msg.encode())
                                                raise Exception(msg) from e
                                            continue

                                        await callback(
                                            InsertNotification(data_saved=cm.as_data_saved(), raw_data=data)
                                        )

                                elif isinstance(cm, ChannelInfo):
                                    nreceived += 1
                                    m = ConnectionEstablished(comment=f"received {nreceived}")
                                    await callback(m)
                                    # logger.info(f"channel info {cm}")
                                elif isinstance(cm, (WarningMsg, ErrorMsg, FinishedMsg)):
                                    await callback(cm)
                                elif isinstance(cm, SilenceMsg):
                                    await callback(cm)
                                else:
                                    s = f"listen_url_events_: unexpected message {cm!r}"
                                    self.logger.error(s)
                                    await callback(ErrorMsg(comment=s))
                                    if raise_on_error:
                                        raise Exception(s)

                        else:
                            s = f"listen_url_events_: unexpected message type {msg.type} with {msg.data!r}"
                            self.logger.error(s)
                            await callback(ErrorMsg(comment=s))
                            if raise_on_error:
                                raise Exception(s)
                            continue
                except CancelledError:
                    raise
                except Exception as e:
                    msg = str(e)[:100]
                    await ws.close(code=WSCloseCode.ABNORMAL_CLOSURE, message=msg.encode())
                    self.logger.error(f"error in websocket {traceback.format_exc()}")
                    raise
                else:
                    await ws.close(code=WSCloseCode.OK)
        return None

    @asynccontextmanager
    async def push_through_websocket(
        self,
        url_websockets: URLWS,
    ) -> "AsyncIterator[PushInterface]":
        """Iterates using direct data using side loading"""
        from .server import get_tagged_cbor

        use_url: URLString
        async with self.my_session(url_websockets) as (session, use_url):
            ws: ClientWebSocketResponse
            async with session.ws_connect(use_url) as ws:

                class PushInterfaceImpl(PushInterface):
                    async def push_through(self, data: bytes, content_type: ContentType) -> bool:
                        rd = RawData(content_type=content_type, content=data)

                        await ws.send_bytes(get_tagged_cbor(rd))
                        while True:
                            response = await ws.receive()
                            if response.type == aiohttp.WSMsgType.CLOSE:
                                return False

                            elif response.type == aiohttp.WSMsgType.BINARY:
                                pr = parse_cbor_tagged(response.data, PushResult)
                                return pr.result
                            else:
                                logger.error(f"unexpected {response}")
                                continue

                yield PushInterfaceImpl()

    @async_error_catcher
    async def listen_continuous(
        self,
        urlbase0: URL,
        expect_node: Optional[NodeID],
        *,
        switch_identity_ok: bool,
        raise_on_error: bool,
        add_silence: Optional[float],
        inline_data: bool,
        callback: Callable[[ListenURLEvents], Awaitable[None]],
    ) -> ListenDataInterface:
        i = ListenDataContinuousImp(None)
        t = asyncio.create_task(
            self._listen_continuous_(
                urlbase0,
                expect_node,
                switch_identity_ok=switch_identity_ok,
                raise_on_error=raise_on_error,
                add_silence=add_silence,
                inline_data=inline_data,
                callback=callback,
                ldi=i,
            )
        )
        i.task = t
        self.remember_task(t)

        return i

    @async_error_catcher
    async def _listen_continuous_(
        self,
        urlbase0: URL,
        expect_node: Optional[NodeID],
        *,
        ldi: "ListenDataContinuousImp",
        switch_identity_ok: bool,
        raise_on_error: bool,
        add_silence: Optional[float],
        inline_data: bool,
        callback: Callable[[ListenURLEvents], Awaitable[None]],
    ):
        while not ldi.stop_condition.is_set():
            try:
                md = await self.get_metadata(urlbase0)
            except Exception as e:
                msg = f"Error getting metadata for {urlbase0!r}: {e!r}"
                self.logger.error(msg)

                if raise_on_error:
                    raise Exception(msg) from e

                await asyncio.sleep(1.0)
                continue

            # logger.info(
            #     f"Metadata for {urlbase0!r}:\n" + json.dumps(asdict(md), indent=2)
            # )  # available = await dtpsclient.ask_topics(urlbase0)

            if md.answering is None:
                msg = f"This is not a DTPS node."
                self.logger.error(msg)
                if raise_on_error:
                    raise Exception(msg)
                await asyncio.sleep(2.0)
                continue

            if expect_node is not None and md.answering != expect_node:
                if switch_identity_ok:
                    msg = f"Switching identity to {md.answering!r}."
                    self.logger.info(msg)
                else:
                    msg = f"This is not the expected node {expect_node!r}."
                    self.logger.error(msg)
                    if raise_on_error:
                        raise Exception(msg)
                    await asyncio.sleep(2.0)
                    continue

            expect_node = md.answering

            if md.events_url is None and md.events_data_inline_url == "":
                msg = f"This resource does not support events."
                self.logger.error(msg)
                if raise_on_error:
                    raise Exception(msg)
                await asyncio.sleep(2.0)
                continue

            if not inline_data:
                if md.events_url is None:
                    msg = f"This resource does not support events."
                    self.logger.error(msg)
                    if raise_on_error:
                        raise Exception(msg)
                    await asyncio.sleep(2.0)
                    continue

                listen_data = await self.listen_url_events3(
                    md.events_url,
                    raise_on_error=raise_on_error,
                    inline_data=False,
                    add_silence=add_silence,
                    callback=callback,  # stop_condition=stop_condition
                )

            else:
                if md.events_data_inline_url is None:
                    msg = f"This resource does not support inline data events."
                    self.logger.error(msg)
                    if raise_on_error:
                        raise Exception(msg)
                    await asyncio.sleep(2.0)
                    continue
                listen_data = await self.listen_url_events3(
                    md.events_data_inline_url,
                    raise_on_error=raise_on_error,
                    inline_data=True,
                    add_silence=add_silence,
                    callback=callback,
                )

            should_break_outer = False
            try:
                try:
                    finish = asyncio.create_task(listen_data.wait_for_done())
                    try:
                        await self._wait_until_shutdown(finish, ldi.stop_condition)
                    except ShutdownAsked:
                        return
                    except ConditionSatistied:
                        return

                except StopContinuousLoop as e:
                    self.logger.error(f"obtained {e}")
                    should_break_outer = True
                    break
            except CancelledError:
                raise
            except Exception as e:
                msg = f"Error listening to {urlbase0!r}:\n{traceback.format_exc()}"
                self.logger.error(msg)
                if raise_on_error:
                    raise Exception(msg) from e
                await asyncio.sleep(1.0)
                continue

            if should_break_outer:
                break
            await asyncio.sleep(1.0)

    @async_error_catcher
    async def push_continuous(
        self,
        urlbase0: URL,
        *,
        queue_in: "asyncio.Queue[RawData]",
        queue_out: "asyncio.Queue[bool]",
    ) -> "asyncio.Task[None]":
        try:
            md = await self.get_metadata(urlbase0)
        except Exception as e:
            msg = f"Error getting metadata for {urlbase0!r}: {e!r}"
            self.logger.error(msg)
            raise ValueError(msg) from e

        if md.stream_push_url is None:
            raise ValueError(f"no stream push url in {md}")

        task = asyncio.create_task(pusher(self, md.stream_push_url, queue_in, queue_out))
        self.remember_task(task)

        return task


@async_error_catcher
async def pusher(
    client: DTPSClient, to_url: URLWS, queue_in: "asyncio.Queue[RawData]", queue_out: "asyncio.Queue[bool]"
):
    async with client.push_through_websocket(to_url) as p:
        while True:
            rd = await queue_in.get()
            success = await p.push_through(rd.content, rd.content_type)
            queue_in.task_done()
            queue_out.put_nowait(success)


class PushInterface(ABC):
    @abstractmethod
    async def push_through(self, data: bytes, content_type: ContentType) -> bool:
        ...


def escape_json_pointer(s: str) -> str:
    return s.replace("~", "~0").replace("/", "~1")


def unescape_json_pointer(s: str) -> str:
    return s.replace("~1", "/").replace("~0", "~")


@async_error_catcher
async def _listen_and_callback(
    desc: str, it: AsyncIterator[ListenURLEvents], cb: Callable[[RawData], Awaitable[None]]
) -> None:
    try:
        # logger.debug(f"_listen_and_callback ({desc}): starting")
        i = 0
        async for lue in it:
            i += 1
            # logger.debug(f"_listen_and_callback ({desc}): {i} {lue}")
            if isinstance(lue, InsertNotification):
                await cb(lue.raw_data)

    except CancelledError:
        # logger.debug(f"_listen_and_callback ({desc}): cancelled")
        raise
    # logger.debug(f"_listen_and_callback ({desc}): finished")


async def my_raise_for_status(resp: ClientResponse, url0: URL) -> None:
    if not resp.ok:
        # reason should always be not None for a started response
        assert resp.reason is not None
        msg = await resp.read()
        try:
            msg = msg.decode("utf-8")
        except:
            pass

        message = ""
        message += f"method: {resp.method}\n"
        message += f"url0: {url_to_string(url0)}\n"
        message += f"reason: {resp.reason}\n"
        message += f"msg:\n{msg}\n"

        raise ClientResponseError(
            resp.request_info,
            resp.history,
            status=resp.status,
            message=message,
            headers=resp.headers,
        )


class StopContinuousLoop(Exception):
    pass


class ListenDataContinuousImp(ListenDataInterface):
    ldi: Optional[ListenDataInterface] = None
    stop_condition: Event
    task: "Optional[asyncio.Task[None]]"

    def __init__(self, ldi: Optional[ListenDataInterface]):
        self.ldi = ldi
        self.stop_condition = asyncio.Event()
        self.task = None

    async def stop(self) -> None:
        self.stop_condition.set()
        assert self.task is not None
        await self.task

    async def wait_for_done(self) -> None:
        assert self.task is not None
        await self.task


def channel_msgs_parse(d: bytes) -> "ChannelMsgs":
    Ts = (
        ChannelInfo,
        DataReady,
        Chunk,
        FinishedMsg,
        ErrorMsg,
        WarningMsg,
        SilenceMsg,
    )

    return parse_cbor_tagged(d, *Ts)


#
# pub async fn add_tpt_connection(
#     conbase: &TypeOfConnection,
#     connection_name: &CompositeName,
#     connection_job: &ConnectionJob,
# ) -> DTPSR<()> {
#     let md = crate::get_metadata(conbase).await?;
#     let url = match md.connections_url {
#         None => {
#             return not_available!(
#                 "cannot remove connection: no connections_url in metadata for {}",
#                 conbase.to_string()
#             );
#         }
#         Some(url) => url,
#     };
#
#     let patch = create_add_tpt_connection_patch(connection_name, connection_job)?;
#
#     client_verbs::patch_data(&url, &patch).await
# }
#
# fn create_add_tpt_connection_patch(
#     connection_name: &CompositeName,
#     connection_job: &ConnectionJob,
# ) -> Result<Patch, DTPSError> {
#     let mut path: String = String::new();
#     path.push('/');
#     path.push_str(utils_patch::escape_json_patch(connection_name.as_dash_sep()).as_str());
#
#     let wire = connection_job.to_wire();
#     let value = serde_json::to_value(wire)?;
#
#     let add_operation = AddOperation { path, value };
#     let operation1 = PatchOperation::Add(add_operation);
#     let patch = json_patch::Patch(vec![operation1]);
#     Ok(patch)
# }
#
# pub async fn remove_tpt_connection(conbase: &TypeOfConnection, connection_name: &CompositeName) ->
# DTPSR<()> {
#     let md = crate::get_metadata(conbase).await?;
#     let url = match md.connections_url {
#         None => {
#             return not_available!(
#                 "cannot remove connection: no connections_url in metadata for {}",
#                 conbase.to_string()
#             );
#         }
#         Some(url) => url,
#     };
#
#     let patch = create_remove_tpt_connection_patch(connection_name);
#
#     client_verbs::patch_data(&url, &patch).await
# }
#
# fn create_remove_tpt_connection_patch(connection_name: &CompositeName) -> Patch {
#     let mut path: String = String::new();
#     path.push('/');
#     path.push_str(utils_patch::escape_json_patch(connection_name.as_dash_sep()).as_str());
#
#     let remove_operation = RemoveOperation { path };
#     let operation1 = PatchOperation::Remove(remove_operation);
#     json_patch::Patch(vec![operation1])
# }
