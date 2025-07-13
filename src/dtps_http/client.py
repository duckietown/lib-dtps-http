"""Client."""

__all__ = [
    "AbstractListenDataInterface",
    "DTPSClient",
    "FoundMetadata",
    "escape_json_pointer",
    "my_raise_for_status",
    "unescape_json_pointer",
]

import asyncio
import traceback
from abc import ABC, abstractmethod
from asyncio import FIRST_COMPLETED, CancelledError, Event, Queue, Task
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import (
    AbstractAsyncContextManager,
    AsyncExitStack,
    asynccontextmanager,
    suppress,
)
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar, cast
from urllib import parse

import aiohttp
import cbor2
import tcp_latency
from aiohttp import (
    ClientResponse,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
    ClientWebSocketResponse,
    TCPConnector,
    UnixConnector,
    WSCloseCode,
    WSMessage,
    WSMsgType,
)
from multidict import CIMultiDictProxy

from dtps_http import logger
from dtps_http import logger as logger0
from dtps_http.constants import (
    CONTENT_TYPE_PATCH_CBOR,
    HEADER_CONTENT_LOCATION,
    HEADER_DATA_ORIGIN_NODE_ID,
    HEADER_MAX_FREQUENCY,
    HEADER_NODE_ID,
    HTTP_TIMEOUT,
    MIME_CBOR,
    MIME_OCTET,
    REL_CONNECTIONS,
    REL_EVENTS_DATA,
    REL_EVENTS_NODATA,
    REL_HISTORY,
    REL_META,
    REL_PROXIED,
    REL_STREAM_PUSH,
    STATUS_ERROR,
    STATUS_SUCCESS,
    STATUS_UNAVAILABLE,
    TOPIC_PROXIED,
)
from dtps_http.exceptions import (
    ConditionSatistiedError,
    EventListeningNotAvailableError,
    NoSuchTopicError,
    ShutdownAskedError,
    StopContinuousLoopError,
    TopicOriginUnavailableError,
)
from dtps_http.link_headers import get_link_headers
from dtps_http.server import get_tagged_cbor
from dtps_http.structures import (
    CHANNEL_MESSAGE_TYPES,
    ChannelInfo,
    Chunk,
    ConnectionEstablishedMessage,
    ConnectionJob,
    DataReady,
    ErrorMessage,
    FinishedMessage,
    ForwardingStep,
    InsertNotification,
    LinkBenchmark,
    ListenURLEvents,
    ProxyJob,
    PushResult,
    RawData,
    SilenceMessage,
    TopicReachability,
    TopicRefAdd,
    TopicsIndex,
    TopicsIndexWire,
    WarningMessage,
)
from dtps_http.types_ import ContentType, NodeID, TopicNameV, URLString
from dtps_http.urls import (
    URL,
    URLWS,
    URLIndexer,
    URLTopic,
    URLWSInline,
    URLWSOffline,
    join,
    parse_url_unescape,
    url_to_string,
)
from dtps_http.utils import (
    async_error_catcher,
    check_is_unix_socket,
    method_lru_cache,
    parse_cbor_tagged,
    pretty,
)

U = TypeVar("U", bound=URL)
X = TypeVar("X")


class AbstractListenDataInterface(ABC):
    """Abstract listen data interface."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop."""
        raise NotImplementedError

    @abstractmethod
    async def wait_for_done(self) -> None:
        """Wait for `done`."""
        raise NotImplementedError

    @async_error_catcher
    async def wait_for_done_or_stop_on_event(
        self,
        shutdown_event: Event,
    ) -> None:
        """Wait for `done` or stop on event."""
        shutdown_coroutine = shutdown_event.wait()
        wait_for_shutdown_task = asyncio.create_task(shutdown_coroutine)
        wait_for_done_coroutine = self.wait_for_done()
        wait_for_done_task = asyncio.create_task(wait_for_done_coroutine)
        _, pending_tasks = await asyncio.wait(
            [wait_for_shutdown_task, wait_for_done_task],
            return_when=FIRST_COMPLETED,
        )
        for pending_task in pending_tasks:
            pending_task.cancel()
        if shutdown_event.is_set():
            wait_for_done_task.cancel()
            await self.stop()


class DTPSClient:
    """DTPS client."""

    tasks: list[Task[Any]]
    blacklist_protocol_host_port: set[tuple[str, str, int]]
    obtained_answer: dict[tuple[str, str, int], NodeID | None]
    preferred_cache: dict[URL, URL]
    sessions: dict[str, ClientSession]
    shutdown_event: Event

    if TYPE_CHECKING:

        @classmethod
        def create(
            cls,
            nickname: str | None = None,
            shutdown_event: Event | None = None,
        ) -> "AbstractAsyncContextManager[DTPSClient]":
            """Create."""

    else:

        @classmethod
        @asynccontextmanager
        async def create(
            cls,
            nickname: str | None = None,
            shutdown_event: Event | None = None,
        ) -> "AsyncIterator[DTPSClient]":
            """Create."""
            object_ = cls(nickname=nickname, shutdown_event=shutdown_event)
            await object_.initialize()
            try:
                yield object_
            finally:
                await object_.aclose()

    def __init__(
        self,
        nickname: str | None,
        shutdown_event: Event | None,
    ) -> None:
        """Initialize DTPS client."""
        if shutdown_event is None:
            shutdown_event = Event()
        self.shutdown_event = shutdown_event
        self.async_exit_stack = AsyncExitStack()
        self.tasks = []
        self.sessions = {}
        self.preferred_cache = {}
        self.blacklist_protocol_host_port = set()
        self.obtained_answer = {}
        if nickname is None:
            id_ = id(self)
            nickname = str(id_)
        self.nickname = nickname
        self.logger = logger0.getChild(nickname)
        self.shutdown_event = Event()

    def remember_task(self, task: Task[Any]) -> None:
        """Remember task."""
        self.tasks.append(task)

    @async_error_catcher
    async def aclose(self) -> None:
        """Close asynchronously."""
        self.shutdown_event.set()
        for task in self.tasks:
            task.cancel()
        await self.async_exit_stack.aclose()

    @async_error_catcher
    async def ask_index(self, url0: URLIndexer) -> TopicsIndex:
        """Return topic index."""
        url = self._look_cache(url0)
        async with self.my_session(url) as (session, use_url):
            async with session.get(use_url) as response:
                await my_raise_for_status(response, url0)
                preferred = await self.prefer_alternative(url, response)
                if preferred is not None:
                    self.logger.debug(
                        "Using preferred alternative to %s -> %r",
                        url,
                        preferred,
                    )
                    return await self.ask_index(preferred)
                if response.status != STATUS_SUCCESS:
                    raise ValueError(response.status)
                response_bytes: bytes = await response.read()
                respnse_object = cbor2.loads(response_bytes)
            alternatives0 = response.headers.getall(
                HEADER_CONTENT_LOCATION,
                [],
            )
            where_this_available: list[URL] = [url]
            for alternative in cast(list[URLString], alternatives0):
                try:
                    parsed_alternative = parse_url_unescape(alternative)
                except ValueError:
                    self.logger.exception("Cannot parse %s.", alternative)
                    continue
                else:
                    where_this_available.append(parsed_alternative)
            topics_index_wire = TopicsIndexWire.from_json(respnse_object)
            return topics_index_wire.to_internal([url])

    def _look_cache(self, url0: U) -> U:
        url = self.preferred_cache.get(url0, url0)
        return cast(U, url)

    @async_error_catcher
    async def publish(self, url0: URL, raw_data: RawData) -> None:
        """Publish."""
        url = self._look_cache(url0)
        headers = {
            "content-type": raw_data.content_type,
        }
        async with (
            self.my_session(url) as (session, use_url),
            session.post(
                use_url,
                data=raw_data.content,
                headers=headers,
            ) as response,
        ):
            await my_raise_for_status(response, url0)
            if response.status not in (200, 201):
                raise ValueError(response)
            await self.prefer_alternative(url, response)

    @async_error_catcher
    async def call(self, url0: URL, raw_data: RawData) -> RawData:
        """Call."""
        url = self._look_cache(url0)
        headers = {
            "content-type": raw_data.content_type,
        }
        async with self.my_session(url) as (session, use_url):
            async with session.post(
                use_url,
                data=raw_data.content,
                headers=headers,
            ) as response:
                await my_raise_for_status(response, url0)
                if response.status not in (200, 201):
                    raise ValueError(response)
                await self.prefer_alternative(url, response)
                location = response.headers.get("Location")
                if not location:
                    message = (
                        f"No location header in response to call for {url} "
                        f"{response}."
                    )
                    raise ValueError(message)
                url_redirect = join(url, location)
            return await self.get(url_redirect, accept=None)

    @async_error_catcher
    async def prefer_alternative(
        self,
        current: U,
        response: aiohttp.ClientResponse,
    ) -> U | None:
        """Return alternative URL."""
        if not isinstance(current, URL):
            raise TypeError(current)
        if current in self.preferred_cache:
            return cast(U, self.preferred_cache[current])
        nothing: list[URLString] = []
        urls = response.headers.getall(HEADER_CONTENT_LOCATION, nothing)
        alternatives0 = cast(list[URLString], urls)
        if not alternatives0:
            return None
        alternatives: list[URL] = [current]
        for alternative in alternatives0:
            try:
                parsed_alternative = parse_url_unescape(alternative)
            except ValueError:
                self.logger.exception("Cannot parse %s.", alternative)
                continue
            else:
                alternatives.append(parsed_alternative)
        header = response.headers.get(HEADER_NODE_ID)
        answering = cast(NodeID, header)
        best = await self.find_best_alternative(
            [(_, answering) for _ in alternatives],
        )
        if best is None:
            best = current
        if best != current:
            self.preferred_cache[current] = best
            return cast(U, best)
        return None

    @async_error_catcher
    async def compute_with_hop(
        self,
        this_node_id: NodeID,
        connects_to: URLTopic,
        expects_answer_from: NodeID,
        forwarders: list[ForwardingStep],
    ) -> TopicReachability | None:
        """Compute with hop."""
        if not isinstance(connects_to, URL):
            raise TypeError(connects_to)
        benchmark = await self.can_use_url(connects_to, expects_answer_from)
        if benchmark is None:
            return None
        forwarding_node_connects_to = url_to_string(connects_to)
        forwarding_step = ForwardingStep(
            forwarding_node=this_node_id,
            forwarding_node_connects_to=forwarding_node_connects_to,
            performance=benchmark,
        )
        total = LinkBenchmark.identity()
        for forwarder in forwarders:
            total |= forwarder.performance
        total |= benchmark
        url_string = url_to_string(connects_to)
        return TopicReachability(
            url=url_string,
            answering=this_node_id,
            forwarders=[*forwarders, forwarding_step],
            benchmark=total,
        )

    @async_error_catcher
    async def find_best_alternative(
        self,
        us: Sequence[tuple[U, NodeID | None]],
    ) -> U | None:
        """Return best alternative."""
        if not us:
            self.logger.warning("find_best_alternative: no alternatives")
            return None
        results: list[str] = []
        possible: list[tuple[float, float, float, U]] = []
        for url, expects_answer_from in us:
            if not isinstance(url, URL):
                raise TypeError(url)
            score = await self.can_use_url(url, expects_answer_from)
            if score is not None:
                possible.append(
                    (
                        score.complexity,
                        score.latency_ns,
                        -score.bandwidth,
                        url,
                    ),
                )
                # TODO: 60 is a magic number?
                results.append(f"✓ {url!s:<60} -> {score}")
            else:
                results.append(f"✗ {url} ")
        possible.sort(key=lambda x: (x[0], x[1]))
        if not possible:
            results_ = "\n".join(results)
            self.logger.warning(
                "find_best_alternative: no alternatives found:\n %s",
                results_,
            )
            return None
        best = possible[0][-1]
        results.append(f"best: {best}")
        results_ = "\n".join(results)
        self.logger.debug(results_)
        return best

    @method_lru_cache()
    def measure_latency(self, host: str, port: int) -> float | None:
        """Measure latency."""
        self.logger.debug("computing latency to%s:%s...", host, port)
        latency_points = tcp_latency.measure_latency(
            host,
            port,
            runs=5,
            wait=0.01,
            timeout=0.5,
        )
        latency_points = cast(list[float], latency_points)
        if not latency_points:
            self.logger.debug("latency to %s:%s -> unreachable", host, port)
            return None
        res_sum = sum(latency_points)
        res_length = len(latency_points)
        latency_seconds = res_sum / res_length / 1000
        self.logger.debug(
            "latency to %s:%s is  %ss  [%s]",
            host,
            port,
            latency_seconds,
            latency_points,
        )
        return latency_seconds

    @async_error_catcher
    async def _process_http_or_https_scheme(
        self,
        url: URLTopic,
        expects_answer_from: NodeID | None,
        blacklist_key: tuple[str, str, int],
        *,
        do_measure_latency: bool = True,
        check_right_node: bool = True,
    ) -> LinkBenchmark | None:
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
            if (
                expects_answer_from is not None
                and who_answers != expects_answer_from
            ):
                self.logger.exception(
                    "can_use_url: wrong %s header in %s, expected %s",
                    who_answers,
                    url,
                    expects_answer_from,
                )
                return None
        latency_ns = int(latency * 1_000_000_000)
        reliability_percent = int(reliability * 100)
        return LinkBenchmark(
            complexity,
            bandwidth,
            latency_ns,
            reliability_percent,
            hops,
        )

    @async_error_catcher
    async def _process_http_plus_unix_scheme(
        self,
        url: URLTopic,
        expects_answer_from: NodeID | None,
    ) -> LinkBenchmark | None:
        complexity = 1
        reliability_percent = 100
        hops = 1
        bandwidth = 100_000_000
        latency = 0.001
        host = url.host
        self.logger.debug("checking %s...  path=%r", url, url)
        path = Path(host)
        if not path.exists():
            self.logger.warning("%s: %r does not exist", url, host)
            return None
        who_answers = await self.get_who_answers(url)
        if (
            expects_answer_from is not None
            and who_answers != expects_answer_from
        ):
            self.logger.exception(
                "wrong %s header in %s, expected %s",
                who_answers,
                url,
                expects_answer_from,
            )
            return None
        latency_ns = int(latency * 1_000_000_000)
        return LinkBenchmark(
            complexity,
            bandwidth,
            latency_ns,
            reliability_percent,
            hops,
        )

    @async_error_catcher
    async def can_use_url(
        self,
        url: URLTopic,
        expects_answer_from: NodeID | None,
        *,
        do_measure_latency: bool = True,
        check_right_node: bool = True,
    ) -> LinkBenchmark | None:
        """Return `None` or a score for the URL."""
        blacklist_key = (url.scheme, url.host, url.port or 0)
        if blacklist_key in self.blacklist_protocol_host_port:
            self.logger.debug("blacklisted %s", url)
            return None
        if url.scheme in ("http", "https"):
            return await self._process_http_or_https_scheme(
                url,
                expects_answer_from,
                blacklist_key,
                do_measure_latency=do_measure_latency,
                check_right_node=check_right_node,
            )
        if url.scheme == "http+unix":
            return await self._process_http_plus_unix_scheme(
                url,
                expects_answer_from,
            )
        if url.scheme == "http+ether":
            return None
        self.logger.warning("Unknown scheme %r for %s", url.scheme, url)
        return None

    @async_error_catcher
    async def get_who_answers(self, url: URLTopic) -> NodeID | None:
        """Return who answers."""
        key = (url.scheme, url.host, url.port or 0)
        if key not in self.obtained_answer:
            try:
                metadata = await self.get_metadata(url)
            except CancelledError:
                raise
            except Exception:
                exception = traceback.format_exc()
                self.logger.exception("Error checking %s\n%s", url, exception)
                return None
            else:
                return metadata.answering
        return self.obtained_answer[key]

    if TYPE_CHECKING:

        def my_session(
            self,
            url: URL,
            /,
            *,
            conn_timeout: float | None = None,
        ) -> AbstractAsyncContextManager[tuple[ClientSession, URLString]]:
            """Return session."""

    else:

        @asynccontextmanager
        async def my_session(
            self,
            url: URL,
            /,
            *,
            conn_timeout: float | None = None,
        ) -> AsyncIterator[tuple[ClientSession, URLString]]:
            """Return session."""
            if not isinstance(url, URL):
                raise TypeError(url)
            connector: TCPConnector | UnixConnector
            if url.scheme == "http+unix":
                if url.host is None:
                    message = f"No host in {url!r}."
                    raise AssertionError(message)
                path = parse.unquote(url.host)
                connector = UnixConnector(path)
                use_url = url_to_string(
                    url._replace(scheme="http", host="localhost"),
                )
                try:
                    check_is_unix_socket(path)
                except ValueError as error:
                    message = (
                        "Cannot connect to url because the path does not exist"
                        f" : {url!r}"
                    )
                    raise ValueError(message) from error
            elif url.scheme in ("http", "https"):
                connector = TCPConnector()
                use_url = url_to_string(url)
            else:
                message = f"unknown scheme {url.scheme!r} for {url!r}"
                raise ValueError(message)
            timeout = ClientTimeout(conn_timeout)
            async with (
                connector,
                ClientSession(connector=connector, timeout=timeout) as session,
            ):
                yield session, use_url

    @async_error_catcher
    async def get_proxied(
        self,
        url0: URLIndexer,
    ) -> dict[TopicNameV, ProxyJob]:
        """Return proxied."""
        # FIXME: Need to use REL_PROXIED
        relative_url = TOPIC_PROXIED.as_relative_url()
        url = join(url0, relative_url)
        raw_data = await self.get(url, MIME_CBOR)
        data_object = cbor2.loads(raw_data.content)
        data_dictionary = cast(dict, data_object)
        proxy_jobs: dict[TopicNameV, ProxyJob] = {}
        for key, value in data_dictionary.items():
            topic_name = TopicNameV.from_dash_sep(key)
            proxy_jobs[topic_name] = ProxyJob.from_json(value)
        return proxy_jobs

    @async_error_catcher
    async def add_proxy(
        self,
        url0: URLIndexer,
        topic_name: TopicNameV,
        node_id: NodeID | None,
        urls: list[URLString],
        *,
        mask_origin: bool,
    ) -> bool:
        """Return `True` if changes required, `False` otherwise."""
        proxied = await self.get_proxied(url0)
        dash_separated_topic_name = topic_name.as_dash_sep()
        path = "/" + escape_json_pointer(dash_separated_topic_name)
        patch: list[dict[str, Any]] = []
        if topic_name in proxied:
            proxy_job = proxied[topic_name]
            if proxy_job.node_id == node_id and proxy_job.urls == urls:
                return False
            patch.append(
                {
                    "op": "remove",
                    "path": path,
                },
            )
        proxy_job = ProxyJob(node_id, urls, mask_origin)
        value = asdict(proxy_job)
        patch.append(
            {
                "op": "add",
                "path": path,
                "value": value,
            },
        )
        data = cbor2.dumps(patch)
        # FIXME: DTSW-5454: Need to use REL_PROXIED
        relative_url = TOPIC_PROXIED.as_relative_url()
        url = join(url0, relative_url)
        await self.patch(url, CONTENT_TYPE_PATCH_CBOR, data)
        return True

    @async_error_catcher
    async def remove_proxy(
        self,
        url0: URLIndexer,
        topic_name: TopicNameV,
    ) -> None:
        """Remove proxy."""
        dash_separated_topic_name = topic_name.as_dash_sep()
        path = "/" + escape_json_pointer(dash_separated_topic_name)
        patch = [
            {
                "op": "remove",
                "path": path,
            },
        ]
        data = cbor2.dumps(patch)
        # FIXME: DTSW-5454: Need to use REL_PROXIED
        relative_url = TOPIC_PROXIED.as_relative_url()
        url = join(url0, relative_url)
        await self.patch(url, CONTENT_TYPE_PATCH_CBOR, data)

    @async_error_catcher
    async def add_topic(
        self,
        url0: URLIndexer,
        topic_name: TopicNameV,
        tra: TopicRefAdd,
    ) -> None:
        """Add topic."""
        dash_separated_topic_name = topic_name.as_dash_sep()
        path = "/" + escape_json_pointer(dash_separated_topic_name)
        value = asdict(tra)
        patch = [
            {
                "op": "add",
                "path": path,
                "value": value,
            },
        ]
        data = cbor2.dumps(patch)
        await self.patch(url0, CONTENT_TYPE_PATCH_CBOR, data)

    @async_error_catcher
    async def _process_patch_response(
        self,
        response: ClientResponse,
        url0: URL,
        use_url: URLString,
    ) -> RawData:
        data = await response.read()
        if not response.ok:
            message = f"Cannot patch {url0=!r} {use_url=!r} {response=!r}.\n"
            try:
                message += data.decode("utf-8")
            except UnicodeDecodeError:
                message += str(data)
            raise ValueError(message)
        headers = response.headers.get("content-type", MIME_OCTET)
        content_type = ContentType(headers)
        return RawData(data, content_type)

    @async_error_catcher
    async def patch(
        self,
        url0: URL,
        content_type: str | None,
        data: bytes,
    ) -> RawData:
        """Patch."""
        headers = {}
        if content_type is not None:
            headers["content-type"] = content_type
        url = self._look_cache(url0)
        use_url = None
        try:
            async with (
                self.my_session(url, conn_timeout=HTTP_TIMEOUT) as (
                    session,
                    use_url,
                ),
                session.patch(use_url, data=data, headers=headers) as response,
            ):
                return await self._process_patch_response(
                    response,
                    url0,
                    use_url,
                )
        except CancelledError:
            raise
        except:
            exception = traceback.format_exc()
            self.logger.exception(
                "Cannot connect to %r %r\n%s",
                url,
                use_url,
                exception,
            )
            raise

    @async_error_catcher
    async def _process_get_response(
        self,
        response: ClientResponse,
        url0: URL,
        accept: str | None,
        use_url: URLString,
    ) -> RawData:
        data = await response.read()
        headers = response.headers.get(
            "content-type",
            "application/octet-stream",
        )
        content_type = ContentType(headers)
        raw_data = RawData(data, content_type)
        if not response.ok:
            message = f"Cannot GET {url0=!r}\n{use_url=!r}\n{response=!r}\n"
            try:
                message += data.decode("utf-8")
            except UnicodeDecodeError:
                message += str(data)
            if response.status == STATUS_ERROR:
                raise NoSuchTopicError(message)
            if response.status == STATUS_UNAVAILABLE:
                raise TopicOriginUnavailableError(message)
            raise ValueError(message)
        if accept is not None and content_type != accept:
            response_headers_dictionary = dict(response.headers)
            pretty_response_headers_dictionary = pretty(
                response_headers_dictionary,
            )
            message = (
                f"GET gave a different content type {accept=!r}, "
                f"{content_type}\n{url0=}\n"
                f"{pretty_response_headers_dictionary}"
            )
            raise ValueError(message)
        return raw_data

    @async_error_catcher
    async def get(self, url0: URL, accept: str | None) -> RawData:
        """Return raw data."""
        headers: dict[str, str] = {}
        if accept is not None:
            headers["accept"] = accept
        url = self._look_cache(url0)
        use_url = None
        try:
            async with (
                self.my_session(url, conn_timeout=HTTP_TIMEOUT) as (
                    session,
                    use_url,
                ),
                session.get(use_url) as response,
            ):
                return await self._process_get_response(
                    response,
                    url0,
                    accept,
                    use_url,
                )
        except CancelledError:
            raise
        except NoSuchTopicError:
            raise
        except TopicOriginUnavailableError:
            raise
        except:
            exception = traceback.format_exc()
            self.logger.exception(
                "Cannot connect to %r %r \n%s",
                url,
                use_url,
                exception,
            )
            raise

    @async_error_catcher
    async def delete(self, url0: URL) -> None:
        """Delete."""
        url = self._look_cache(url0)
        async with (
            self.my_session(url, conn_timeout=HTTP_TIMEOUT) as (
                session,
                use_url,
            ),
            session.delete(use_url) as response,
        ):
            response.raise_for_status()

    @async_error_catcher
    async def _process_head_response(
        self,
        response: ClientResponse,
        url: URLTopic,
        url0: URL,
    ) -> "FoundMetadata":
        await my_raise_for_status(response, url0)
        if HEADER_CONTENT_LOCATION in response.headers:
            headers = response.headers.getall(
                HEADER_CONTENT_LOCATION,
            )
            alternatives0 = [URLString(header) for header in headers]
        else:
            alternatives0 = []
        links = get_link_headers(response.headers)
        if REL_EVENTS_DATA in links:
            url = join(url, links[REL_EVENTS_DATA].url)
            events_url_data = cast(URLWSInline, url)
        else:
            events_url_data = None
        if REL_EVENTS_NODATA in links:
            url = join(url, links[REL_EVENTS_NODATA].url)
            events_url = cast(URLWSOffline, url)
        else:
            events_url = None
        if REL_STREAM_PUSH in links:
            url = join(url, links[REL_STREAM_PUSH].url)
            stream_push_url = cast(URLWS, url)
        else:
            stream_push_url = None
        meta_url = (
            join(url, links[REL_META].url) if REL_META in links else None
        )
        connections_url = (
            join(url, links[REL_CONNECTIONS].url)
            if REL_CONNECTIONS in links
            else None
        )
        proxied_url = (
            join(url, links[REL_PROXIED].url) if REL_PROXIED in links else None
        )
        history_url = (
            join(url, links[REL_HISTORY].url) if REL_HISTORY in links else None
        )
        answering = (
            NodeID(response.headers[HEADER_NODE_ID])
            if HEADER_NODE_ID in response.headers
            else None
        )
        origin_node = (
            NodeID(response.headers[HEADER_DATA_ORIGIN_NODE_ID])
            if HEADER_DATA_ORIGIN_NODE_ID in response.headers
            else None
        )
        urls = []
        for alternative in alternatives0:
            url = join(url, alternative)
            url = cast(URLTopic, url)
            urls.append(url)
        return FoundMetadata(
            url,
            urls,
            answering=answering,
            origin_node=origin_node,
            events_url=events_url,
            events_data_inline_url=events_url_data,
            meta_url=meta_url,
            history_url=history_url,
            stream_push_url=stream_push_url,
            connections_url=connections_url,
            proxied_url=proxied_url,
            raw_headers=response.headers,
        )

    @async_error_catcher
    async def get_metadata(self, url0: URLTopic) -> "FoundMetadata":
        """Return metadata."""
        url = self._look_cache(url0)
        async with (
            self.my_session(url, conn_timeout=HTTP_TIMEOUT) as (
                session,
                use_url,
            ),
            session.head(use_url) as response,
        ):
            return await self._process_head_response(response, url, url0)

    @async_error_catcher
    async def choose_best_alternative(
        self,
        reachability: list[TopicReachability],
    ) -> URL:
        """Return best alternative."""
        use: list[tuple[URL, NodeID | None]] = []
        for r in reachability:
            try:
                parsed_url = parse_url_unescape(r.url)
            except ValueError:
                self.logger.exception("Cannot parse %s", r.url)
                continue
            else:
                use.append((parsed_url, r.answering))
        best_alternative = await self.find_best_alternative(use)
        if best_alternative is None:
            message = f"No reachable url for {reachability}."
            self.logger.exception(message)
            raise ValueError(message)
        return best_alternative

    @async_error_catcher
    async def connect(
        self,
        url_index: URLIndexer,
        connection_name: TopicNameV,
        connection_job: ConnectionJob,
    ) -> None:
        """Connect."""
        url_index = self._look_cache(url_index)
        metadata = await self.get_metadata(url_index)
        if metadata.connections_url is None:
            pretty_metadata = pretty(metadata)
            message = (
                f"Connection functionality not available: {pretty_metadata}"
            )
            raise ValueError(message)
        dash_separated_connection_name = connection_name.as_dash_sep()
        path = "/" + escape_json_pointer(dash_separated_connection_name)
        wire = connection_job.to_wire()
        value = asdict(wire)
        op = {
            "op": "add",
            "path": path,
            "value": value,
        }
        ops = [op]
        data = cbor2.dumps(ops)
        await self.patch(
            metadata.connections_url,
            CONTENT_TYPE_PATCH_CBOR,
            data,
        )

    @async_error_catcher
    async def disconnect(
        self,
        url_index: URLIndexer,
        connection_name: TopicNameV,
    ) -> None:
        """Disconnect."""
        url_index = self._look_cache(url_index)
        metadata = await self.get_metadata(url_index)
        if metadata.connections_url is None:
            pretty_metadata = pretty(metadata)
            message = (
                f"Connection functionality not available: {pretty_metadata}"
            )
            raise ValueError(message)
        dash_separated_connection_name = connection_name.as_dash_sep()
        path = "/" + escape_json_pointer(dash_separated_connection_name)
        op = {
            "op": "remove",
            "path": path,
        }
        ops = [op]
        data = cbor2.dumps(ops)
        await self.patch(
            metadata.connections_url,
            CONTENT_TYPE_PATCH_CBOR,
            data,
        )

    @async_error_catcher
    async def listen_url(
        self,
        url_topic: URLTopic,
        callback: Callable[[RawData], Awaitable[None]],
        *,
        inline_data: bool,
        raise_on_error: bool,
        connection_timeout: float = 10,
        max_frequency: float | None,
        on_finished: Callable[[FinishedMessage], Awaitable[None]]
        | None = None,
    ) -> AbstractListenDataInterface:
        """Listen to URL."""
        url_topic = self._look_cache(url_topic)
        metadata = await self.get_metadata(url_topic)
        logger.debug(
            "listen_url: listening to %s for %s -",
            metadata.origin_node,
            url_topic,
        )
        url_events: URLWSInline | URLWSOffline
        if inline_data:
            if metadata.events_data_inline_url is not None:
                url_events = metadata.events_data_inline_url
            else:
                url = url_to_string(url_topic)
                message = (
                    f"Cannot find field `events_data_inline_url` for\n{url}\n"
                    f"{metadata=}"
                )
                raise EventListeningNotAvailableError(message)
        elif metadata.events_url is not None:
            url_events = metadata.events_url
        else:
            url = url_to_string(url_topic)
            message = f"Cannot find `events_url` for\n{url}\n{metadata=}"
            raise EventListeningNotAvailableError(message)
        connection_event = Event()
        filter_data = self._get_filter_data(
            url_events,
            connection_event,
            callback,
            on_finished,
        )
        li = await self.listen_url_events3(
            url_websockets=url_events,
            inline_data=inline_data,
            raise_on_error=raise_on_error,
            add_silence=None,
            max_frequency=max_frequency,
            callback=filter_data,
        )
        future = connection_event.wait()
        await asyncio.wait_for(future, connection_timeout)
        return li

    @staticmethod
    def _get_filter_data(
        url_events: URLWSInline | URLWSOffline,
        connection_event: Event,
        callback: Callable[[RawData], Awaitable[None]],
        on_finished: Callable[[FinishedMessage], Awaitable[None]] | None,
    ) -> Any:
        @async_error_catcher
        async def filter_data(listen_url_events: ListenURLEvents) -> None:
            if isinstance(listen_url_events, ErrorMessage):
                logger.exception(
                    f"filter_data: error in {url_events}: "
                    f"{listen_url_events.comment}",
                )
            elif isinstance(listen_url_events, WarningMessage):
                logger.warning(
                    f"filter_data: warning in {url_events}: "
                    f"{listen_url_events.comment}",
                )
            elif isinstance(listen_url_events, SilenceMessage):
                logger.debug(
                    f"filter_data: silence in {url_events}: "
                    f"{listen_url_events.comment}",
                )
            elif isinstance(listen_url_events, FinishedMessage):
                logger.debug(
                    f"filter_data: finished in {url_events}: "
                    f"{listen_url_events.comment}",
                )
                if on_finished is not None:
                    await on_finished(listen_url_events)
            elif isinstance(listen_url_events, ConnectionEstablishedMessage):
                logger.debug(
                    "filter_data: connection established in %s",
                    url_events,
                )
                connection_event.set()
            elif isinstance(listen_url_events, InsertNotification):
                try:
                    await callback(listen_url_events.raw_data)
                except CancelledError:
                    raise
                except Exception:
                    exception = traceback.format_exc()
                    logger.exception(
                        "filter_data: error in handler: %s",
                        exception,
                    )
                    return
            else:
                logger.exception("filter_data: unknown %s", listen_url_events)
                message = f"Unknown {listen_url_events}"
                raise TypeError(message)

        return filter_data

    @async_error_catcher
    async def listen_url_events3(
        self,
        *,
        url_websockets: URLWS,
        inline_data: bool,
        raise_on_error: bool,
        add_silence: float | None,
        max_frequency: float | None,
        callback: Callable[[ListenURLEvents], Awaitable[None]],
    ) -> AbstractListenDataInterface:
        """Listen for URL events."""
        stop_condition = Event()
        coroutine = self.listen_url_events_(
            url_websockets=url_websockets,
            inline_data=inline_data,
            raise_on_error=raise_on_error,
            add_silence=add_silence,
            max_frequency=max_frequency,
            callback=callback,
            stop_condition=stop_condition,
        )
        task = asyncio.create_task(coroutine)
        return ListenData(stop_condition, task)

    @async_error_catcher
    async def _wait_until_shutdown(
        self,
        task: Task[X],
        condition: Event,
    ) -> X:
        """Wait until shutdown.

        Waits for an event or for the shutdown event, in which case we
        raise `ShutdownAskedError`. If the condition is set, we raise
        `ConditionSatistiedError`.
        """
        coroutine_1 = self.shutdown_event.wait()
        task_wait = asyncio.create_task(coroutine_1)
        coroutine_2 = condition.wait()
        task_condition_wait = asyncio.create_task(coroutine_2)
        _, incomplete_tasks = await asyncio.wait(
            [task_wait, task, task_condition_wait],
            return_when=FIRST_COMPLETED,
        )
        for incomplete_task in incomplete_tasks:
            incomplete_task.cancel()
        if self.shutdown_event.is_set():
            raise ShutdownAskedError
        if condition.is_set():
            raise ConditionSatistiedError
        return await task

    @async_error_catcher
    async def _download_from_urls(
        self,
        url_base: URL,
        data_ready: DataReady,
    ) -> RawData:
        url_data_list = []
        for _ in data_ready.availability:
            url_base = join(url_base, _.url)
            url_data_list.append(url_base)
        if not url_data_list:
            self.logger.exception("No `url_data_list` in %s", data_ready)
            message = f"no url_datas in {data_ready}"
            raise AssertionError(message)
        #  TODO: DTSW-4781: Try multiple urls
        url_data = url_data_list[0]
        return await self.get(url_data, accept=data_ready.content_type)

    @async_error_catcher
    async def listen_url_events_(
        self,
        *,
        url_websockets: URLWS,
        inline_data: bool,
        raise_on_error: bool,
        add_silence: float | None,
        max_frequency: float | None,
        callback: Callable[[ListenURLEvents], Awaitable[None]],
        stop_condition: Event,
    ) -> None:
        """Iterate using direct data in websocket."""
        try:
            async with self.my_session(url_websockets) as (session, use_url):
                headers: dict[str, str] = {}
                if max_frequency is not None:
                    headers[HEADER_MAX_FREQUENCY] = str(max_frequency)
                async with session.ws_connect(
                    use_url,
                    headers=headers,
                ) as websocket:
                    try:
                        await self._process_url_events(
                            websocket,
                            url_websockets=url_websockets,
                            inline_data=inline_data,
                            raise_on_error=raise_on_error,
                            add_silence=add_silence,
                            callback=callback,
                            stop_condition=stop_condition,
                        )
                    except CancelledError:
                        self.logger.debug("listen_url_events_: canceled")
                        raise
                    except Exception as exception:
                        self.logger.exception(
                            "listen_url_events_: error in websocket %s",
                        )
                        message = str(exception)[:100]
                        encoded_message = message.encode()
                        await websocket.close(
                            code=WSCloseCode.ABNORMAL_CLOSURE,
                            message=encoded_message,
                        )
                        raise
                    else:
                        self.logger.debug(
                            "listen_url_events_: closed normally",
                        )
                        await websocket.close(code=WSCloseCode.OK)
        finally:
            self.logger.debug("listen_url_events_: finally")

    @staticmethod
    @async_error_catcher
    async def _callback_wrap(
        callback: Callable[[ListenURLEvents], Awaitable[None]],
        listen_url_events: ListenURLEvents,
    ) -> None:
        logger.debug(f"callback_wrap {listen_url_events}")
        try:
            await callback(listen_url_events)
        except CancelledError:
            raise
        except Exception:
            exception = traceback.format_exc()
            logger.exception("Error in callback %s", exception)

    @async_error_catcher
    async def _get_websocket_message(
        self,
        websocket: ClientWebSocketResponse,
        *,
        add_silence: float | None,
        callback: Callable[[ListenURLEvents], Awaitable[None]],
        stop_condition: Event,
    ) -> WSMessage | None:
        coroutine = websocket.receive()
        task = asyncio.create_task(coroutine)
        websocket_message_task = self._wait_until_shutdown(
            task,
            stop_condition,
        )
        websocket_message = None
        if add_silence is not None:
            try:
                websocket_message = await asyncio.wait_for(
                    websocket_message_task,
                    timeout=add_silence,
                )
            except asyncio.exceptions.TimeoutError:
                if add_silence is not None:
                    silenced_message = SilenceMessage(
                        dt=add_silence,
                        comment=f"{self.number_received=}",
                    )
                    await self._callback_wrap(callback, silenced_message)
        else:
            with suppress(asyncio.exceptions.TimeoutError):
                websocket_message = await websocket_message_task
        return websocket_message

    @async_error_catcher
    async def _process_url_events(
        self,
        websocket: ClientWebSocketResponse,
        *,
        url_websockets: URLWS,
        inline_data: bool,
        raise_on_error: bool,
        add_silence: float | None,
        callback: Callable[[ListenURLEvents], Awaitable[None]],
        stop_condition: Event,
    ) -> None:
        self.number_received = 0
        received_first = False
        while not stop_condition.is_set():
            if websocket.closed:
                if self.number_received == 0:
                    error_message = ErrorMessage(
                        "Closed, but not even one event received.",
                    )
                    await self._callback_wrap(callback, error_message)
                finished_message = FinishedMessage("Closed")
                await self._callback_wrap(callback, finished_message)
                break
            try:
                websocket_message = await self._get_websocket_message(
                    websocket,
                    add_silence=add_silence,
                    callback=callback,
                    stop_condition=stop_condition,
                )
                if websocket_message is None:
                    continue
            except ShutdownAskedError:
                finished_message = FinishedMessage(
                    "Shutdown asked: ending `listen_url`...",
                )
                await self._callback_wrap(callback, finished_message)
                break
            except ConditionSatistiedError:
                finished_message = FinishedMessage(
                    "Condition satisfied: ending `listen_url`...",
                )
                await self._callback_wrap(callback, finished_message)
                break
            if not received_first:
                connection_established_message = ConnectionEstablishedMessage(
                    f"Received {websocket_message}",
                )
                await self._callback_wrap(
                    callback,
                    connection_established_message,
                )
                received_first = True
            exit_loop = await self._process_websocket_message(
                websocket_message,
                websocket,
                url_websockets=url_websockets,
                inline_data=inline_data,
                raise_on_error=raise_on_error,
                callback=callback,
            )
            if exit_loop:
                break

    @async_error_catcher
    async def _process_websocket_message(
        self,
        websocket_message: WSMessage,
        websocket: ClientWebSocketResponse,
        *,
        url_websockets: URLWS,
        inline_data: bool,
        raise_on_error: bool,
        callback: Callable[[ListenURLEvents], Awaitable[None]],
    ) -> bool:
        if websocket_message.type in (WSMsgType.CLOSE, WSMsgType.CLOSING):
            if self.number_received == 0:
                message = (
                    "Closed but not even one event received."
                    if websocket_message.type == WSMsgType.CLOSE
                    else "Closing but not even one event received"
                )
                error_message = ErrorMessage(message)
                await self._callback_wrap(callback, error_message)
            message = (
                "Closed."
                if websocket_message.type == WSMsgType.CLOSE
                else "Closing..."
            )
            finished_message = FinishedMessage(message)
            await self._callback_wrap(callback, finished_message)
            return True
        if websocket_message.type == WSMsgType.CLOSED:
            finished_message = FinishedMessage("Closed.")
            await self._callback_wrap(callback, finished_message)
            return True
        if websocket_message.type == WSMsgType.ERROR:
            message = str(websocket_message.data)
            error_message = ErrorMessage(message)
            await self._callback_wrap(callback, error_message)
            if raise_on_error:
                raise Exception(message)
        elif websocket_message.type == WSMsgType.BINARY:
            await self._process_binary_websocket_message(
                websocket_message,
                websocket,
                url_websockets,
                inline_data=inline_data,
                raise_on_error=raise_on_error,
                callback=callback,
            )
        else:
            message = (
                f"listen_url_events_: unexpected message type "
                f"{websocket_message.type} with {websocket_message.data!r}"
            )
            self.logger.exception(message)
            error_message = ErrorMessage(message)
            await self._callback_wrap(callback, error_message)
            if raise_on_error:
                raise Exception(message)
        return False

    @async_error_catcher
    async def _process_binary_websocket_message(
        self,
        websocket_message: WSMessage,
        websocket: ClientWebSocketResponse,
        url_websockets: URLWS,
        *,
        inline_data: bool,
        raise_on_error: bool,
        callback: Callable[[ListenURLEvents], Awaitable[None]],
    ) -> None:
        try:
            channel_messages = parse_cbor_tagged(
                websocket_message.data,
                *CHANNEL_MESSAGE_TYPES,
            )
        except Exception as exception:
            message = (
                f"error in parsing {websocket_message.data!r}: "
                f"{exception.__class__.__name__}:\n{exception}"
            )
            self.logger.exception(message)
            error_message = ErrorMessage(message)
            await self._callback_wrap(callback, error_message)
            if raise_on_error:
                raise Exception(message) from exception
            return
        else:
            if isinstance(channel_messages, DataReady):
                data_ready = channel_messages
                if inline_data:
                    if data_ready.chunks_arriving == 0:
                        message = (
                            f"Unexpected `chunks_arriving` "
                            f"{data_ready.chunks_arriving} in "
                            f"{data_ready}, {inline_data=}."
                        )
                        self.logger.exception(message)
                        error_message = ErrorMessage(message)
                        await self._callback_wrap(callback, error_message)
                        if raise_on_error:
                            raise Exception(message)
                    data = b""
                    for _ in range(data_ready.chunks_arriving):
                        websocket_message = await websocket.receive()
                        # FIXME: Need to use primitives
                        inner_channel_messages = parse_cbor_tagged(
                            websocket_message.data,
                            *CHANNEL_MESSAGE_TYPES,
                        )
                        if isinstance(inner_channel_messages, Chunk):
                            data += inner_channel_messages.data
                        else:
                            message = (
                                "unexpected message while waiting for chunks "
                                f"{websocket_message!r}"
                            )
                            self.logger.exception(message)
                            error_message = ErrorMessage(message)
                            await self._callback_wrap(callback, error_message)
                            if raise_on_error:
                                raise Exception(message)
                            continue
                    data_length = len(data)
                    if data_length != data_ready.content_length:
                        message = (
                            f"unexpected data length {data_length} != "
                            f"{data_ready.content_length}\n"
                            f"{data_ready}"
                        )
                        self.logger.exception(message)
                        error_message = ErrorMessage(message)
                        await self._callback_wrap(callback, error_message)
                        if raise_on_error:
                            message = (
                                f"Unexpected data length {data_length} != "
                                f"{data_ready.content_length}"
                            )
                            raise Exception(message)
                    raw_data = RawData(data, data_ready.content_type)
                    data_saved = data_ready.as_data_saved()
                    insert_notification = InsertNotification(
                        data_saved,
                        raw_data,
                    )
                    await self._callback_wrap(callback, insert_notification)
                else:
                    if data_ready.chunks_arriving > 0:
                        message = (
                            f"unexpected chunks_arriving "
                            f"{data_ready.chunks_arriving} in "
                            f"{data_ready}, {inline_data=}"
                        )
                        self.logger.exception(message)
                        error_message = ErrorMessage(message)
                        await self._callback_wrap(callback, error_message)
                        if raise_on_error:
                            raise Exception(message)
                    try:
                        # TODO: Re-use the same session for gets
                        raw_data = await self._download_from_urls(
                            url_websockets,
                            data_ready,
                        )
                    except Exception as exception:
                        message = (
                            f"error in downloading {data_ready}: "
                            f"{exception.__class__.__name__}\n{exception}"
                        )
                        self.logger.exception(message)
                        error_message = ErrorMessage(message)
                        await self._callback_wrap(callback, error_message)
                        if raise_on_error:
                            encoded_message = message.encode()
                            await websocket.close(message=encoded_message)
                            raise Exception(message) from exception
                        return
                    data_saved = data_ready.as_data_saved()
                    insert_notification = InsertNotification(
                        data_saved,
                        raw_data,
                    )
                    await self._callback_wrap(callback, insert_notification)
            elif isinstance(data_ready, ChannelInfo):
                self.number_received += 1
                connection_established_message = ConnectionEstablishedMessage(
                    f"Received {self.number_received}.",
                )
                await self._callback_wrap(
                    callback,
                    connection_established_message,
                )
            elif isinstance(
                data_ready,
                ErrorMessage
                | FinishedMessage
                | SilenceMessage
                | WarningMessage,
            ):
                await self._callback_wrap(callback, data_ready)
            else:
                message = (
                    f"listen_url_events_: unexpected message {data_ready!r}"
                )
                self.logger.exception(message)
                error_message = ErrorMessage(message)
                await self._callback_wrap(callback, error_message)
                if raise_on_error:
                    raise Exception(message)

    @asynccontextmanager
    async def push_through_websocket(
        self,
        url_websockets: URLWS,
    ) -> AsyncIterator["PushInterface"]:
        """Iterate using direct data using side loading."""
        use_url: URLString
        websocket: ClientWebSocketResponse
        async with (
            self.my_session(url_websockets) as (session, use_url),
            session.ws_connect(use_url) as websocket,
        ):
            yield PushInterface(websocket)

    @async_error_catcher
    async def listen_continuous(
        self,
        urlbase0: URL,
        expect_node: NodeID | None,
        *,
        switch_identity_ok: bool,
        raise_on_error: bool,
        add_silence: float | None,
        inline_data: bool,
        callback: Callable[[ListenURLEvents], Awaitable[None]],
        max_frequency: float | None,
    ) -> AbstractListenDataInterface:
        """Listen continuously."""
        listen_data_interface = ListenDataContinuous(None)
        coroutine = self._listen_continuous_(
            urlbase0,
            expect_node,
            switch_identity_ok=switch_identity_ok,
            raise_on_error=raise_on_error,
            add_silence=add_silence,
            inline_data=inline_data,
            callback=callback,
            listen_data_interface=listen_data_interface,
            max_frequency=max_frequency,
        )
        task = asyncio.create_task(coroutine)
        listen_data_interface.task = task
        self.remember_task(task)
        return listen_data_interface

    @async_error_catcher
    async def _listen_continuous_(
        self,
        urlbase0: URL,
        expect_node: NodeID | None,
        *,
        listen_data_interface: "ListenDataContinuous",
        switch_identity_ok: bool,
        raise_on_error: bool,
        add_silence: float | None,
        inline_data: bool,
        callback: Callable[[ListenURLEvents], Awaitable[None]],
        max_frequency: float | None,
    ) -> None:
        while not listen_data_interface.stop_condition.is_set():
            try:
                metadata = await self.get_metadata(urlbase0)
            except Exception as exception:
                message = (
                    f"Error getting metadata for {urlbase0!r}: {exception!r}"
                )
                self.logger.exception(message)
                if raise_on_error:
                    raise Exception(message) from exception
                await asyncio.sleep(1)
                continue
            if metadata.answering is None:
                message = "This is not a DTPS node."
                self.logger.exception(message)
                if raise_on_error:
                    raise Exception(message)
                await asyncio.sleep(2)
                continue
            if expect_node is not None and metadata.answering != expect_node:
                if switch_identity_ok:
                    self.logger.debug(
                        "Switching identity to %r.",
                        metadata.answering,
                    )
                else:
                    message = f"This is not the expected node {expect_node!r}."
                    self.logger.exception(message)
                    if raise_on_error:
                        raise Exception(message)
                    await asyncio.sleep(2)
                    continue
            expect_node = metadata.answering
            if (
                metadata.events_url is None
                and metadata.events_data_inline_url == ""
            ):
                message = "This resource does not support events."
                self.logger.exception(message)
                if raise_on_error:
                    raise Exception(message)
                await asyncio.sleep(2)
                continue
            if not inline_data:
                if metadata.events_url is None:
                    message = "This resource does not support events."
                    self.logger.exception(message)
                    if raise_on_error:
                        raise Exception(message)
                    await asyncio.sleep(2)
                    continue
                listen_data = await self.listen_url_events3(
                    url_websockets=metadata.events_url,
                    inline_data=False,
                    raise_on_error=raise_on_error,
                    add_silence=add_silence,
                    max_frequency=max_frequency,
                    callback=callback,
                )
            else:
                if metadata.events_data_inline_url is None:
                    message = (
                        "This resource does not support inline data events."
                    )
                    self.logger.exception(message)
                    if raise_on_error:
                        raise Exception(message)
                    await asyncio.sleep(2)
                    continue
                listen_data = await self.listen_url_events3(
                    url_websockets=metadata.events_data_inline_url,
                    inline_data=True,
                    raise_on_error=raise_on_error,
                    add_silence=add_silence,
                    max_frequency=max_frequency,
                    callback=callback,
                )
            try:
                try:
                    coroutine = listen_data.wait_for_done()
                    finish = asyncio.create_task(coroutine)
                    try:
                        await self._wait_until_shutdown(
                            finish,
                            listen_data_interface.stop_condition,
                        )
                    except ShutdownAskedError:
                        return
                    except ConditionSatistiedError:
                        return
                except StopContinuousLoopError:
                    break
            except Exception as exception:
                formatted_traceback = traceback.format_exc()
                message = (
                    f"Error listening to {urlbase0!r}:\n{formatted_traceback}"
                )
                self.logger.exception(message)
                if raise_on_error:
                    raise Exception(message) from exception
                await asyncio.sleep(1)
                continue
            await asyncio.sleep(1)

    @async_error_catcher
    async def push_continuous(
        self,
        urlbase0: URL,
        *,
        queue_in: Queue[RawData],
        queue_out: Queue[bool],
    ) -> Task[None]:
        """Push continuously."""
        try:
            metadata = await self.get_metadata(urlbase0)
        except Exception as error:
            message = f"Error getting metadata for {urlbase0!r}: {error!r}"
            self.logger.exception(message)
            raise ValueError(message) from error
        if metadata.stream_push_url is None:
            message = f"No stream push url in {metadata}."
            raise ValueError(message)
        coroutine = pusher(self, metadata.stream_push_url, queue_in, queue_out)
        task = asyncio.create_task(coroutine)
        self.remember_task(task)
        return task


@dataclass
class FoundMetadata:
    """Found metadata."""

    # The URL that was used to get the metadata
    origin: URLTopic
    # URL alternatives (Location: headers)
    alternative_urls: list[URLTopic]
    # NodeID if answering is a DTPS node
    answering: NodeID | None
    # HEADER_DATA_ORIGIN_NODE_ID
    origin_node: NodeID | None
    # Websocket with offline data
    events_url: URLWSOffline | None
    # Websocket with inline data
    events_data_inline_url: URLWSInline | None
    # Meta URL
    meta_url: URL | None
    # History URL
    history_url: URL | None
    # URL for stream push
    stream_push_url: URLWS | None
    connections_url: URL | None
    proxied_url: URL | None
    raw_headers: CIMultiDictProxy[str]


@dataclass
class ListenData(AbstractListenDataInterface):
    stop_condition: Event
    task: Task[None]

    @async_error_catcher
    async def stop(self) -> None:
        self.stop_condition.set()
        self.task.cancel()

    @async_error_catcher
    async def wait_for_done(self) -> None:
        try:
            await self.task
        except CancelledError:
            if self.task.done():
                return


class ListenDataContinuous(AbstractListenDataInterface):
    listen_data_interface: AbstractListenDataInterface | None = None
    stop_condition: Event
    task: Task[None] | None

    def __init__(
        self,
        listen_data_interface: AbstractListenDataInterface | None,
    ) -> None:
        self.listen_data_interface = listen_data_interface
        self.stop_condition = Event()
        self.task = None

    @async_error_catcher
    async def stop(self) -> None:
        self.stop_condition.set()
        if self.task is None:
            raise ValueError
        await self.task

    @async_error_catcher
    async def wait_for_done(self) -> None:
        if self.task is None:
            raise ValueError
        await self.task


class PushInterface:
    """Push interface."""

    websocket: ClientWebSocketResponse

    def __init__(self, websocket: ClientWebSocketResponse) -> None:
        self.websocket = websocket

    @async_error_catcher
    async def push_through(
        self,
        data: bytes,
        content_type: ContentType,
    ) -> bool:
        """Push through."""
        raw_data = RawData(data, content_type)
        tagged_cbor = get_tagged_cbor(raw_data)
        await self.websocket.send_bytes(tagged_cbor)
        while True:
            response = await self.websocket.receive()
            if response.type in (
                WSMsgType.CLOSE,
                WSMsgType.CLOSED,
                WSMsgType.CLOSING,
            ):
                return False
            if response.type == WSMsgType.BINARY:
                push_result = parse_cbor_tagged(response.data, PushResult)
                return push_result.result
            logger.exception("Unexpected %s.", response)


def escape_json_pointer(string: str) -> str:
    """Return escaped JSON pointer."""
    string = string.replace("~", "~0")
    return string.replace("/", "~1")


@async_error_catcher
async def my_raise_for_status(response: ClientResponse, url0: URL) -> None:
    """My raise for status."""
    if not response.ok:
        # Reason should always be `not None` for a started response
        if response.reason is None:
            raise ValueError
        url_string = url_to_string(url0)
        message = (
            f"method: {response.method}\nurl0: {url_string}\nreason: "
            f"{response.reason}\nmessage:\n"
        )
        response_payload = await response.read()
        try:
            decoded_response_payload = response_payload.decode("utf-8")
            message += f"{decoded_response_payload}\n"
        except UnicodeDecodeError:
            message += f"{response_payload!s}\n"
        raise ClientResponseError(
            response.request_info,
            response.history,
            status=response.status,
            message=message,
            headers=response.headers,
        )


@async_error_catcher
async def pusher(
    client: DTPSClient,
    to_url: URLWS,
    queue_in: Queue[RawData],
    queue_out: Queue[bool],
) -> None:
    """Pusher."""
    async with client.push_through_websocket(to_url) as push_interface:
        while True:
            raw_data = await queue_in.get()
            success = await push_interface.push_through(
                raw_data.content,
                raw_data.content_type,
            )
            queue_in.task_done()
            queue_out.put_nowait(success)


def unescape_json_pointer(string: str) -> str:
    """Return unescaped JSON pointer."""
    string = string.replace("~1", "/")
    return string.replace("~0", "~")
