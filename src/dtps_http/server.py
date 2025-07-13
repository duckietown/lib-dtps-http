"""Server."""

__all__ = ["DTPSServer", "ForwardedTopic", "get_tagged_cbor"]

import asyncio
import base64
import time
import traceback
import uuid
from asyncio import FIRST_COMPLETED, CancelledError, Event, Task
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Iterator,
    Sequence,
)
from contextlib import (
    AbstractAsyncContextManager,
    asynccontextmanager,
    contextmanager,
)
from dataclasses import asdict, replace
from dataclasses import dataclass as original_dataclass
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

import cbor2
import yaml
from aiohttp import WSMsgType
from aiohttp.web import (
    Application,
    HTTPNotFound,
    Request,
    Response,
    RouteTableDef,
    StreamResponse,
    WebSocketResponse,
)
from aiohttp.web_exceptions import HTTPBadRequest
from aiopubsub import Hub
from cbor2 import CBORDecodeError
from jsonpatch import (
    AddOperation,
    CopyOperation,
    JsonPatch,
    MoveOperation,
    RemoveOperation,
    ReplaceOperation,
    TestOperation,
    static,
)
from multidict import CIMultiDict
from pydantic.dataclasses import dataclass

from dtps_http import __version__
from dtps_http import logger as logger0
from dtps_http.blob_manager import BlobManager
from dtps_http.client import DTPSClient, FoundMetadata, unescape_json_pointer
from dtps_http.constants import (
    CONTENT_TYPE_DTPS_DATAREADY_CBOR,
    CONTENT_TYPE_DTPS_INDEX_CBOR,
    CONTENT_TYPE_PATCH_CBOR,
    CONTENT_TYPE_PATCH_JSON,
    CONTENT_TYPE_PATCH_YAML,
    CONTENT_TYPE_TOPIC_HISTORY_CBOR,
    EVENTS_SUFFIX,
    HEADER_CONTENT_LOCATION,
    HEADER_DATA_ORIGIN_NODE_ID,
    HEADER_DATA_UNIQUE_ID,
    HEADER_MAX_FREQUENCY,
    HEADER_NO_AVAIL,
    HEADER_NO_CACHE,
    HEADER_NODE_ID,
    HEADER_NODE_PASSED_THROUGH,
    MIME_CBOR,
    MIME_JSON,
    REL_EVENTS_DATA,
    REL_EVENTS_NODATA,
    REL_HISTORY,
    REL_META,
    REL_PROXIED,
    REL_STREAM_PUSH,
    REL_STREAM_PUSH_SUFFIX,
    REL_URL_HISTORY,
    REL_URL_META,
    TOPIC_AVAILABILITY,
    TOPIC_CLOCK,
    TOPIC_LIST,
    TOPIC_LOGS,
    TOPIC_PROXIED,
    TOPIC_STATE_NOTIFICATION,
    TOPIC_STATE_SUMMARY,
)
from dtps_http.link_headers import put_link_header
from dtps_http.object_queue import (
    ObjectQueue,
    ObjectServeFunction,
    ObjectTransformFunction,
    TransformError,
    transform_identity,
)
from dtps_http.structures import (
    Bounds,
    ChannelMessages,
    Chunk,
    ConnectionEstablishedMessage,
    ContentInfo,
    DataReady,
    Digest,
    ErrorMessage,
    FinishedMessage,
    InsertNotification,
    LinkBenchmark,
    ListenURLEvents,
    ProxyJob,
    PushResult,
    RawData,
    Registration,
    ResourceAvailability,
    SilenceMessage,
    TopicProperties,
    TopicReachability,
    TopicRef,
    TopicRefAdd,
    TopicsIndex,
    TopicsIndexWire,
    WarningMessage,
    is_image,
    is_structure,
)
from dtps_http.types_ import (
    ContentType,
    HTTPResponse,
    NodeID,
    SourceID,
    TopicNameV,
    URLString,
)
from dtps_http.types_of_source import (
    AbstractSource,
    ForwardedQueue,
    Native,
    NotAvailableYet,
    NotFound,
    OurQueue,
    ResolvedData,
    SourceComposition,
)
from dtps_http.urls import URL, URLWS, URLIndexer, parse_url_unescape
from dtps_http.utils import async_error_catcher, multidict_update
from dtps_http.utils_every_once_in_a_while import EveryOnceInAWhile

MAX_AVAILABILITY = 10
ROOT = TopicNameV.root()
SEND_DATA_ARGNAME = "send_data"


@dataclass
class ForwardedTopic:
    """Forwarded topic."""

    # Unique ID for the stream
    unique_id: SourceID
    # Unique ID of the node that created the stream
    origin_node: NodeID
    app_data: dict[str, Any]
    forward_url_data: URL
    forward_url_events: URLWS | None
    forward_url_events_inline_data: URLWS | None
    reachability: list[TopicReachability]
    properties: TopicProperties
    content_info: ContentInfo
    bounds: Bounds


@original_dataclass
class ForwardInfoEstablished:
    best_url: URL
    found_metadata: FoundMetadata
    index_internal: TopicsIndex


@original_dataclass
class ForwardInfo:
    urls: list[URLString]
    expect_node_id: NodeID | None
    established: ForwardInfoEstablished | None
    mask_origin: bool
    task: Task[Any] | None

    def __post_init__(self) -> None:
        for url in self.urls:
            parse_url_unescape(url)


def get_static_dir() -> str:
    path = Path(__file__)
    parent = None
    options: list[Path] = []
    for _ in range(3):
        parent = path.parent if parent is None else parent.parent
        options.append(parent / "static")
    for option in options:
        if option.exists():
            return str(option)
    message = f"Static directory not found: {options}."
    raise FileNotFoundError(message)


class DTPSServer:
    """DTPS server."""

    _mount_points: dict[TopicNameV, ForwardInfo]
    available_urls: list[URLString]
    blob_manager: BlobManager
    forwarded: dict[TopicNameV, ForwardedTopic]
    nickname: str
    node_app_data: dict[str, Any]
    node_id: NodeID
    object_queues: dict[TopicNameV, ObjectQueue]
    registrations: list[Registration]
    # Set when we have been going through the startup process
    started: Event
    tasks: list[Task[Any]]

    @classmethod
    def create(
        cls,
        on_startup: "Sequence[Callable[[DTPSServer], Awaitable[None]]]" = (),
        nickname: str | None = None,
        *,
        enable_clock: bool = True,
    ) -> "DTPSServer":
        """Create."""
        return cls(
            on_startup=on_startup,
            nickname=nickname,
            enable_clock=enable_clock,
        )

    def __init__(
        self,
        *,
        on_startup: "Sequence[Callable[[DTPSServer], Awaitable[None]]]",
        nickname: str | None,
        enable_clock: bool,
    ) -> None:
        """Initialize DTPS server."""
        if nickname is None:
            self_id = id(self)
            nickname = str(self_id)
        self.nickname = nickname
        self.logger = logger0.getChild(nickname)
        self.app = Application()
        self.node_app_data = {}
        self.node_started = time.time_ns()
        routes = RouteTableDef()
        self._more_on_startup = on_startup
        self.app.on_startup.append(self.on_startup)
        route = routes.get(
            "/{ignore:.*}/:blobs/{digest}/{content_type_base64:.*}",
        )
        route(self.serve_blob)
        route = routes.get("/{topic:.*EVENTS_SUFFIX}/")
        route(self.serve_events)
        route = routes.get("/{topic:.*REL_URL_META}/")
        route(self.serve_meta)
        route = routes.get("/{topic:.*REL_URL_HISTORY}/")
        route(self.serve_history)
        route = routes.get("/{topic:.*REL_STREAM_PUSH_SUFFIX}/")
        route(self.serve_push_stream)
        route = routes.post("/{topic:.*}")
        route(self.serve_post)
        route = routes.patch("/{topic:.*}")
        route(self.serve_patch)
        route = routes.get("/{topic:.*}")
        route(self.serve_get)
        route = routes.delete("/{topic:.*}")
        route(self.serve_delete)
        # TODO: Make smaller than 5
        self.blob_manager = BlobManager(
            cleanup_interval=5,
            forget_forgetting_interval=5,
        )
        # Mount a static directory for the web interface
        static_dir = get_static_dir()
        self.logger.debug("Using static dir: %s", static_dir)
        static_route = static("/static", static_dir)
        self.app.add_routes([static_route])
        self.app.add_routes(routes)
        self.hub = Hub()
        self.object_queues = {}
        self._mount_points = {}
        self.forwarded = {}
        self.tasks = []
        self.available_urls = []
        uuid4 = uuid.uuid4()
        uuid4_string = str(uuid4)
        self.node_id = NodeID(f"{self.nickname}-{uuid4_string[:8]}")
        self.registrations = []
        self.started = Event()
        self.shutdown_event = Event()
        self.enable_clock = enable_clock

    @staticmethod
    @async_error_catcher
    async def _send(
        channel_messages: ChannelMessages,
        websocket: WebSocketResponse,
    ) -> None:
        data = get_tagged_cbor(channel_messages)
        await websocket.send_bytes(data)

    def _get_callback(
        self,
        websocket: WebSocketResponse,
        url_websockets: URLWS,
        *,
        inline_data_send: bool,
    ) -> Any:
        @async_error_catcher
        async def callback(listen_url_events: ListenURLEvents) -> None:
            if isinstance(listen_url_events, InsertNotification):
                data_saved = listen_url_events.data_saved
                if inline_data_send:
                    availability = []
                    chunks_arriving = 1
                else:
                    available_until = time.time() + MAX_AVAILABILITY
                    digest = data_saved.digest
                    the_url = self.blob_manager.get_use_once_link_store(
                        digest,
                        listen_url_events.raw_data.content,
                        listen_url_events.raw_data.content_type,
                        MAX_AVAILABILITY,
                    )
                    self.logger.debug(
                        "serve_events_forwarder_one: sending ref %s, %s",
                        the_url,
                        available_until,
                    )
                    availability = [
                        ResourceAvailability(the_url, available_until),
                    ]
                    chunks_arriving = 0
                data_ready = DataReady(
                    index=data_saved.index,
                    time_inserted=data_saved.time_inserted,
                    digest=data_saved.digest,
                    content_type=data_saved.content_type,
                    content_length=data_saved.content_length,
                    availability=availability,
                    chunks_arriving=chunks_arriving,
                    clocks=data_saved.clocks,
                    origin_node=data_saved.origin_node,
                    unique_id=data_saved.unique_id,
                )
                await self._send(data_ready, websocket)
                if inline_data_send:
                    # TODO: Divide chunks
                    chunk = Chunk(
                        data_ready.digest,
                        0,
                        1,
                        0,
                        listen_url_events.raw_data.content,
                    )
                    await self._send(chunk, websocket)
                else:
                    pass
            elif isinstance(listen_url_events, ConnectionEstablishedMessage):
                silence_message = SilenceMessage(
                    0,
                    f"Connection established to {url_websockets}.",
                )
                await self._send(silence_message, websocket)
            elif isinstance(
                listen_url_events,
                WarningMessage
                | ErrorMessage
                | FinishedMessage
                | SilenceMessage,
            ):
                await self._send(listen_url_events, websocket)
            else:
                self.logger.warning(
                    "Unknown message type %s.",
                    listen_url_events,
                )
                message = f"Cannot handle {listen_url_events!r}."
                raise NotImplementedError(message)

        return callback

    def add_registrations(self, registrations: Sequence[Registration]) -> None:
        """Add registrations."""
        self.registrations.extend(registrations)

    def has_forwarded(self, topic_name: TopicNameV) -> bool:
        """Return `True` if forwarded, `False` otherwise."""
        return topic_name in self.forwarded

    def get_header_alternatives(
        self,
        request: Request,
    ) -> CIMultiDict[str]:
        """Return header alternatives."""
        original_url = str(request.url)
        sock = request.transport._sock
        sockname = sock.getsockname()
        if isinstance(sockname, str):
            path = sockname.replace("/", "%2F")
            use_url = original_url.replace("http://", "http+unix://")
            use_url = use_url.replace("localhost", path)
        else:
            use_url = original_url
        res: CIMultiDict[str] = CIMultiDict()
        if not self.available_urls:
            res[HEADER_NO_AVAIL] = "No alternative URLs available"
            return res
        alternatives = []
        for a in (
            *self.available_urls,
            f"http://127.0.0.1:{request.url.port}/",
            f"http://localhost:{request.url.port}/",
        ):
            if use_url.startswith(a):
                for b in self.available_urls:
                    if a == b:
                        continue
                    alternative = b + removeprefix(use_url, a)
                    alternatives.append(alternative)
        use_url_string = URLString(use_url)
        url = parse_url_unescape(use_url_string)
        if url.path == "/":
            alternatives.extend(self.available_urls)
        alternatives_set = set(alternatives)
        for a in sorted(alternatives_set):
            res.add(HEADER_CONTENT_LOCATION, a)
        if not alternatives:
            res[HEADER_NO_AVAIL] = (
                f"Nothing matched {use_url} of {self.available_urls}"
            )
        else:
            res.popall(HEADER_NO_AVAIL, None)
        return res

    @async_error_catcher
    async def add_available_url(self, url: URLString) -> None:
        """Add available URL."""
        if url in self.available_urls:
            return
        parse_url_unescape(url)
        self.available_urls.append(url)
        available_urls_set = set(self.available_urls)
        available_urls_list = list(available_urls_set)
        self.available_urls = sorted(available_urls_list)
        object_queue = self.get_object_queue(TOPIC_AVAILABILITY)
        await object_queue.publish_json(self.available_urls)

    def remember_task(self, task: Task[Any]) -> None:
        """Remember task.

        Adds a task to the list of tasks to be cancelled on shutdown
        """
        self.tasks.append(task)

    @async_error_catcher
    async def _update_lists(self) -> None:
        if TOPIC_LIST not in self.object_queues:
            relative_url = TOPIC_LIST.as_relative_url()
            message = f"Topic {relative_url} not found."
            raise AssertionError(message)
        if ROOT not in self.object_queues:
            relative_url = ROOT.as_relative_url()
            message = f"Topic {relative_url} not found."
            raise AssertionError(message)
        topics: list[TopicNameV] = []
        object_queue_keys = self.object_queues.keys()
        topics.extend(object_queue_keys)
        forwarded_keys = self.forwarded.keys()
        topics.extend(forwarded_keys)
        relative_urls = []
        for topic in topics:
            relative_url = topic.as_relative_url()
            relative_urls.append(relative_url)
        urls = sorted(relative_urls)
        await self.object_queues[TOPIC_LIST].publish_json(urls)
        topics_index = self.create_root_index()
        topics_index_wire = topics_index.to_wire()
        topics_index_wire_dictionary = asdict(topics_index_wire)
        await self.object_queues[ROOT].publish_cbor(
            topics_index_wire_dictionary,
            CONTENT_TYPE_DTPS_INDEX_CBOR,
        )

    @async_error_catcher
    async def remove_object_queue(self, name: TopicNameV) -> None:
        """Remove object queue."""
        if name in self.object_queues:
            self.object_queues.pop(name)
            await self._update_lists()

    @async_error_catcher
    async def remove_forward(self, name: TopicNameV) -> None:
        """Remove forward."""
        if name in self.forwarded:
            self.forwarded.pop(name)
            await self._update_lists()

    @async_error_catcher
    async def _add_proxied_mountpoint(
        self,
        name: TopicNameV,
        node_id: NodeID | None,
        urls: list[URLString],
        *,
        mask_origin: bool,
    ) -> None:
        if name in self._mount_points or name in self.object_queues:
            message = f"Topic {name} already exists."
            raise ValueError(message)
        forward_info = ForwardInfo(
            urls=urls,
            expect_node_id=node_id,
            established=None,
            mask_origin=mask_origin,
            task=None,
        )
        self._mount_points[name] = forward_info
        coroutine = self._ask_for_topics_continuous(name, forward_info)
        forward_info.task = asyncio.create_task(coroutine)
        self.remember_task(forward_info.task)

    def _get_on_data(
        self,
        dtps_client: DTPSClient,
        topic_name: TopicNameV,
        best_url: URLIndexer,
        forward_info: ForwardInfo,
    ) -> Any:
        @async_error_catcher
        async def on_data(raw_data: RawData) -> None:
            od = raw_data.get_as_native_object()
            ti2_ = TopicsIndexWire.from_json(od)
            ti2 = ti2_.to_internal([best_url])
            await self._process_change_topics(
                dtps_client,
                topic_name,
                ti2,
                mask_origin=forward_info.mask_origin,
            )

        return on_data

    @async_error_catcher
    async def _ask_for_topics_continuous(
        self,
        topic_name: TopicNameV,
        forward_info: ForwardInfo,
    ) -> None:
        dash_separated_topic_name = topic_name.as_dash_sep()
        nickname = f"{self.nickname}:proxyreader({dash_separated_topic_name})"
        self.logger.debug(
            "Starting %s forward_info=%s",
            nickname,
            forward_info,
        )
        async with self.client(nickname) as dtps_client:
            url = parse_url_unescape(forward_info.urls[0])
            url_indexer = URLIndexer(url)
            metadata = await dtps_client.get_metadata(url_indexer)
            if (
                metadata.answering is not None
                and forward_info.expect_node_id is not None
                and metadata.answering != forward_info.expect_node_id
            ):
                self.logger.exception(
                    "Node %s expected but s% found",
                    forward_info.expect_node_id,
                    metadata.answering,
                )
                await asyncio.sleep(1)
            # TODO: Check node ID
            best_url = url_indexer
            topics_index = await dtps_client.ask_index(url_indexer)
            index_internal = TopicsIndex({})
            forward_info.established = ForwardInfoEstablished(
                best_url,
                metadata,
                index_internal,
            )
            await self._process_change_topics(
                dtps_client,
                topic_name,
                topics_index,
                mask_origin=forward_info.mask_origin,
            )
            on_data = self._get_on_data(
                dtps_client,
                topic_name,
                best_url,
                forward_info,
            )
            listen_data_interface = await dtps_client.listen_url(
                url_indexer,
                on_data,
                inline_data=True,
                raise_on_error=False,
                max_frequency=None,
            )
            try:
                wait_coroutine = self.shutdown_event.wait()
                condition = asyncio.create_task(wait_coroutine)
                wait_for_done_coroutine = listen_data_interface.wait_for_done()
                waiting = asyncio.create_task(wait_for_done_coroutine)
                await asyncio.wait(
                    [condition, waiting],
                    return_when=FIRST_COMPLETED,
                )
            except Exception:
                await listen_data_interface.stop()

    @staticmethod
    def _process_change_topics_key(
        topic_reachability: TopicReachability,
    ) -> tuple[int, float, float]:
        return (
            topic_reachability.benchmark.complexity,
            topic_reachability.benchmark.latency_ns,
            -topic_reachability.benchmark.bandwidth,
        )

    @async_error_catcher
    async def _process_change_topics(
        self,
        dtps_client: DTPSClient,
        prefix: TopicNameV,
        topic_index: TopicsIndex,
        *,
        mask_origin: bool,
    ) -> None:
        info = self._mount_points[prefix]
        if info.established is None:
            message = f"Established is None for {prefix}."
            raise AssertionError(message)
        previous = list(info.established.index_internal.topics)
        current = list(topic_index.topics)
        previous_set = set(previous)
        current_set = set(current)
        removed = previous_set - current_set
        added = current_set - previous_set
        self.logger.debug("added=%r removed=%r", added, removed)
        for topic_name in removed:
            new_topic = prefix + topic_name
            if self.has_forwarded(new_topic):
                self.logger.debug("removing topic %s", new_topic)
                await self.remove_forward(new_topic)
        # TODO: Note that this remains the choice for ever
        for topic_name in added:
            topic_reference = topic_index.topics[topic_name]
            new_topic = prefix + topic_name
            if self.has_forwarded(new_topic):
                self.logger.debug("already have topic %s", new_topic)
                continue
            possible: list[TopicReachability] = []
            for reachability in topic_reference.reachability:
                url = parse_url_unescape(reachability.url)
                metadata0 = await dtps_client.get_metadata(url)
                for url_topic in metadata0.alternative_urls:  # + [rurl]:
                    reach_with_me = await dtps_client.compute_with_hop(
                        self.node_id,
                        connects_to=url_topic,
                        expects_answer_from=reachability.answering,
                        forwarders=reachability.forwarders,
                    )
                    if reach_with_me is not None:
                        possible.append(reach_with_me)
            if not possible:
                self.logger.exception(
                    "Topic %s cannot be reached,",
                    topic_name,
                )
                continue
            possible.sort(key=self._process_change_topics_key)
            topic_reachability = possible[0]
            url_to_use = parse_url_unescape(topic_reachability.url)
            if not isinstance(url_to_use, URL):
                raise TypeError
            self.logger.debug(
                "Proxying %s through %s with benchmark info %s",
                new_topic,
                url_to_use,
                topic_reachability.benchmark,
            )
            metadata = await dtps_client.get_metadata(url_to_use)
            if mask_origin:
                topic_reference_2 = replace(
                    topic_reference,
                    reachability=[topic_reachability],
                )
            else:
                topic_reference_2 = replace(
                    topic_reference,
                    reachability=[
                        *topic_reference.reachability,
                        topic_reachability,
                    ],
                )
            forwarded_topic = ForwardedTopic(
                unique_id=topic_reference_2.unique_id,
                origin_node=topic_reference_2.origin_node,
                app_data=topic_reference_2.app_data,
                reachability=topic_reference_2.reachability,
                forward_url_data=metadata.origin,
                forward_url_events=metadata.events_url,
                forward_url_events_inline_data=metadata.events_data_inline_url,
                content_info=topic_reference_2.content_info,  # FIXME: Content info
                properties=topic_reference_2.properties,
                bounds=topic_reference_2.bounds,
            )
            await self._add_forwarded(new_topic, forwarded_topic)

    @async_error_catcher
    async def expose(
        self,
        topic_name: TopicNameV,
        expect_node_id: NodeID | None,
        urls: Sequence[URLString],
        *,
        mask_origin: bool,
    ) -> None:
        """Expose."""
        queue = self.get_object_queue(TOPIC_PROXIED)
        raw_data = queue.last_data()
        native_object = raw_data.get_as_native_object()
        dictionary = cast(dict[str, Any], native_object)
        urls = list(urls)
        urls = sorted(urls)
        proxy_job = ProxyJob(expect_node_id, urls, mask_origin)
        topic_name_separated_name = topic_name.as_dash_sep()
        dictionary[topic_name_separated_name] = asdict(proxy_job)
        await queue.publish_cbor(dictionary)
        while True:
            if topic_name in self._mount_points:
                self.logger.debug("Found %s in mountpoints.", topic_name)
                if self._mount_points[topic_name].established is not None:
                    self.logger.debug(
                        "Found %s in mountpoints and established is not "
                        "`None`.",
                        topic_name,
                    )
                    break
            await asyncio.sleep(0.1)

    @async_error_catcher
    async def _add_forwarded(
        self,
        name: TopicNameV,
        forwarded: ForwardedTopic,
    ) -> None:
        if name in self.forwarded or name in self.object_queues:
            message = f"Topic {name} already exists."
            raise ValueError(message)
        self.forwarded[name] = forwarded
        await self._update_lists()

    def get_object_queue(self, topic_name: TopicNameV) -> ObjectQueue:
        """Return object queue."""
        if topic_name in self.forwarded:
            dash_separated_topic_name = topic_name.as_dash_sep()
            message = f"Topic {dash_separated_topic_name} is a forwarded one."
            raise ValueError(message)
        return self.object_queues[topic_name]

    @async_error_catcher
    async def create_object_queue(
        self,
        topic_name: TopicNameV,
        content_info: ContentInfo,
        *,
        topic_properties: TopicProperties | None,
        bounds: Bounds | None,
        transform: ObjectTransformFunction = transform_identity,
        serve: ObjectServeFunction | None = None,
        app_data: dict[str, bytes] | None = None,
    ) -> ObjectQueue:
        """Create object queue."""
        if app_data is None:
            app_data = {}
        if bounds is None:
            bounds = Bounds.default()
        if topic_name in self.forwarded:
            dash_separated_topic_name = topic_name.as_dash_sep()
            message = (
                f"Topic '{dash_separated_topic_name}' is a forwarded one."
            )
            raise ValueError(message)
        if topic_name in self.object_queues:
            return self.object_queues[topic_name]
        unique_id = get_unique_id(self.node_id, topic_name)
        relative_url = topic_name.as_relative_url()
        benchmark = LinkBenchmark.identity()
        treach = TopicReachability(
            url=relative_url,
            answering=self.node_id,
            forwarders=[],
            benchmark=benchmark,
        )
        reachability: list[TopicReachability] = [treach]
        if topic_properties is None:
            topic_properties = TopicProperties.default()
        current_time = time.time_ns()
        topic_reference = TopicRef(
            unique_id=unique_id,
            origin_node=self.node_id,
            app_data=app_data,
            reachability=reachability,
            created=current_time,
            properties=topic_properties,
            content_info=content_info,
            bounds=bounds,
        )
        self.object_queues[topic_name] = ObjectQueue(
            self.hub,
            topic_name,
            topic_reference,
            bounds=bounds,
            blob_manager=self.blob_manager,
            transform=transform,
            serve=serve,
        )
        await self._update_lists()
        return self.object_queues[topic_name]

    @async_error_catcher
    async def on_startup(self, _: Application) -> None:
        """Run on startup."""
        content_info = ContentInfo.simple(CONTENT_TYPE_DTPS_INDEX_CBOR)
        unique_id = get_unique_id(self.node_id, ROOT)
        properties = TopicProperties.streamable_readonly()
        current_time = time.time_ns()
        bounds = Bounds.max_length(1)
        topic_reference = TopicRef(
            unique_id=unique_id,
            origin_node=self.node_id,
            app_data={},
            reachability=[],
            content_info=content_info,
            properties=properties,
            created=current_time,
            bounds=bounds,
        )
        bounds = Bounds.max_length(1)
        self.object_queues[ROOT] = ObjectQueue(
            self.hub,
            ROOT,
            topic_reference,
            blob_manager=self.blob_manager,
            bounds=bounds,
        )
        index = self.create_root_index()
        wire = index.to_wire()
        wire_dictionary = asdict(wire)
        content = cbor2.dumps(wire_dictionary)
        await self.object_queues[ROOT].publish(
            RawData(
                content=content,
                content_type=CONTENT_TYPE_DTPS_INDEX_CBOR,
            ),
        )
        content_info = ContentInfo.simple(MIME_JSON)
        unique_id = get_unique_id(self.node_id, TOPIC_LIST)
        properties = TopicProperties.streamable_readonly()
        current_time = time.time_ns()
        bounds = Bounds.max_length(1)
        topic_reference = TopicRef(
            unique_id=unique_id,
            origin_node=self.node_id,
            app_data={},
            reachability=[],
            content_info=content_info,
            properties=properties,
            created=current_time,
            bounds=bounds,
        )
        bounds = Bounds.max_length(1)
        self.object_queues[TOPIC_LIST] = ObjectQueue(
            self.hub,
            TOPIC_LIST,
            topic_reference,
            blob_manager=self.blob_manager,
            bounds=bounds,
        )
        content_info = ContentInfo.simple(MIME_JSON)
        bounds = Bounds.max_length(100)
        await self.create_object_queue(
            TOPIC_LOGS,
            content_info=content_info,
            topic_properties=None,
            bounds=bounds,
        )
        if self.enable_clock:
            content_info = ContentInfo.simple(MIME_JSON)
            bounds = Bounds.max_length(1)
            await self.create_object_queue(
                TOPIC_CLOCK,
                content_info=content_info,
                topic_properties=None,
                bounds=bounds,
            )
        content_info = ContentInfo.simple(MIME_JSON)
        bounds = Bounds.max_length(1)
        await self.create_object_queue(
            TOPIC_AVAILABILITY,
            content_info=content_info,
            topic_properties=None,
            bounds=bounds,
        )
        content_info = ContentInfo.simple(MIME_JSON)
        bounds = Bounds.max_length(1)
        await self.create_object_queue(
            TOPIC_STATE_SUMMARY,
            content_info=content_info,
            topic_properties=None,
            bounds=bounds,
        )
        content_info = ContentInfo.simple(MIME_CBOR)
        topic_properties = TopicProperties.patchable_only()
        bounds = Bounds.max_length(1)
        object_queue = await self.create_object_queue(
            TOPIC_PROXIED,
            content_info=content_info,
            topic_properties=topic_properties,
            bounds=bounds,
        )
        await object_queue.publish_cbor({})
        object_queue.subscribe(self.on_proxied_changed)
        content_info = ContentInfo.simple(MIME_CBOR)
        bounds = Bounds.max_length(1)
        await self.create_object_queue(
            TOPIC_STATE_NOTIFICATION,
            content_info=content_info,
            topic_properties=None,
            bounds=bounds,
        )
        if self.enable_clock:
            coroutine = update_clock(self, TOPIC_CLOCK, 1, 0)
            task = asyncio.create_task(coroutine)
            self.remember_task(task)
        for function in self._more_on_startup:
            await function(self)
        for registration in self.registrations:
            coroutine = self._register(registration)
            task = asyncio.create_task(coroutine)
            self.remember_task(task)
        self.started.set()

    @async_error_catcher
    async def on_proxied_changed(
        self,
        _: ObjectQueue,
        insert_notification: InsertNotification,
    ) -> None:
        """Run on proxied changed."""
        current = list(self.forwarded)
        insert_notification_object = (
            insert_notification.raw_data.get_as_native_object()
        )
        insert_notification_dictionary = cast(
            dict[str, Any],
            insert_notification_object,
        )
        topic_names = []
        for dash_separated_topic_name in insert_notification_dictionary:
            topic_name = TopicNameV.from_dash_sep(dash_separated_topic_name)
            topic_names.append(topic_name)
        topic_names_set = set(topic_names)
        current_set = set(current)
        added = topic_names_set - current_set
        removed = current_set - topic_names_set
        for topic_name in removed:
            await self.remove_forward(topic_name)
        for topic_name in added:
            dash_separated_topic_name = topic_name.as_dash_sep()
            proxy_job = ProxyJob.from_json(
                insert_notification_dictionary[dash_separated_topic_name],
            )
            await self._add_proxied_mountpoint(
                topic_name,
                proxy_job.node_id,
                proxy_job.urls,
                mask_origin=proxy_job.mask_origin,
            )

    @async_error_catcher
    async def aclose(self) -> None:
        """Close asynchronously."""
        self.shutdown_event.set()
        for task in self.tasks:
            task.cancel()
        for queue in self.object_queues.values():
            await queue.aclose()

    @async_error_catcher
    async def _register(self, registration: Registration) -> None:
        count = 0
        while True:
            try:
                changes = await self._try_register(registration)
            except Exception:
                self.logger.exception(
                    "Error while registering %s",
                    registration,
                )
                await asyncio.sleep(1)
            else:
                dash_separated_topic_name = registration.topic.as_dash_sep()
                if count == 0:
                    self.logger.debug(
                        "Registered as %s on %s",
                        dash_separated_topic_name,
                        registration.switchboard_url,
                    )
                elif changes:
                    self.logger.debug(
                        "Re-registered as %s on %s",
                        dash_separated_topic_name,
                        registration.switchboard_url,
                    )
                count += 1
                # TODO: DTSW-4782: Just open a websocket connection and
                # see when it closes
                await asyncio.sleep(10)

    @async_error_catcher
    async def _try_register(self, registration: Registration) -> bool:
        async with self.client() as client:
            if not self.available_urls:
                message = (
                    f"Cannot register {registration} because of no available "
                    "URLs."
                )
                self.logger.exception(message)
                raise ValueError(message)
            postfix = registration.namespace.as_relative_url()
            urls = []
            for available_url in self.available_urls:
                url = cast(URLString, available_url + postfix)
                urls.append(url)
            return await client.add_proxy(
                registration.switchboard_url,
                registration.topic,
                self.node_id,
                urls,
                mask_origin=False,
            )

    def create_root_index(self) -> TopicsIndex:
        """Create root index."""
        topics: dict[TopicNameV, TopicRef] = {}
        for topic_name, object_queue in self.object_queues.items():
            qual_topic_name = topic_name
            url = qual_topic_name.as_relative_url()
            benchmark = LinkBenchmark.identity()
            reach = TopicReachability(url, self.node_id, [], benchmark)
            topic_ref = replace(
                object_queue.topic_reference,
                reachability=[reach],
            )
            topics[qual_topic_name] = topic_ref
        for topic_name, forwarded_topic in self.forwarded.items():
            qual_topic_name = topic_name
            current_time = time.time_ns()
            topic_reference = TopicRef(
                forwarded_topic.unique_id,
                forwarded_topic.origin_node,
                {},
                forwarded_topic.reachability,
                current_time,
                forwarded_topic.properties,
                forwarded_topic.content_info,
                forwarded_topic.bounds,
            )
            topics[qual_topic_name] = topic_reference
        for topic_name in list(topics):
            for prefix in topic_name.nontrivial_prefixes():
                if prefix not in topics:
                    url = prefix.as_relative_url()
                    benchmark = LinkBenchmark.identity()
                    reachability = [
                        TopicReachability(url, self.node_id, [], benchmark),
                    ]
                    unique_id = get_unique_id(self.node_id, prefix)
                    properties = TopicProperties.streamable_readonly()
                    current_time = time.time_ns()
                    content_info = ContentInfo.simple(
                        CONTENT_TYPE_DTPS_INDEX_CBOR,
                    )
                    bounds = Bounds.unbounded()
                    topics[prefix] = TopicRef(
                        unique_id,
                        self.node_id,
                        {},
                        reachability,
                        current_time,
                        properties,
                        content_info,
                        bounds,
                    )
        return TopicsIndex(topics)

    @async_error_catcher
    async def serve_index(self, request: Request) -> Response:
        """Serve index."""
        headers_ = request.headers.items()
        headers_string = "".join(
            f"{key}: {value}\n" for key, value in headers_
        )
        index_internal = self.create_root_index()
        index_wire = index_internal.to_wire()
        headers: CIMultiDict[str] = CIMultiDict()
        add_nocache_headers(headers)
        header_alternatives = self.get_header_alternatives(request)
        multidict_update(headers, header_alternatives)
        self._add_own_headers(headers)
        properties = self.object_queues[ROOT].topic_reference.properties
        put_meta_headers(headers, properties)
        json_data = asdict(index_wire)
        relative_url = TOPIC_PROXIED.as_relative_url()
        put_link_header(
            headers,
            relative_url,
            REL_PROXIED,
            CONTENT_TYPE_DTPS_INDEX_CBOR,
        )
        headers.add(HEADER_DATA_ORIGIN_NODE_ID, self.node_id)
        # Get all the accept headers
        accept: list[str] = []
        default_empty: list[str] = []
        for accept_header in request.headers.getall("accept", default_empty):
            split_accept_header = accept_header.split(",")
            accept.extend(split_accept_header)
        if (
            "application/cbor" not in accept
            and CONTENT_TYPE_DTPS_INDEX_CBOR not in accept
            and "text/html" in accept
        ):
            topics_html = "<ul>"
            for topic_name in sorted(index_internal.topics):
                if topic_name.is_root():
                    continue
                relative_url = topic_name.as_relative_url()
                topics_html += (
                    f"<li><a href='{relative_url}'><code>{relative_url}</code>"
                    "</a></li>\n"
                )
            topics_html += "</ul>"
            node_app_data_yaml = yaml.dump(self.node_app_data, indent=3)
            json_data_yaml = yaml.dump(json_data, indent=3)
            cbor_source = (
                "https://cdn.jsdelivr.net/npm/cbor-js@0.1.0/cbor.min.js"
            )
            js_yaml_source = (
                "https://cdnjs.cloudflare.com/ajax/libs/js-yaml/4.1.0/"
                "js-yaml.min.js"
            )
            html_index = f"""
            <html lang="en">
            <head>
            <style>
            </style>
            <link rel="stylesheet" href="/static/style.css">
            <script src="{cbor_source}"></script>
            <script src="{js_yaml_source}"></script>
            <script src="/static/send.js"></script>
            <title>DTPS server</title>
            </head>
            <body>
            <h1>DTPS server</h1>
            <p> This response coming to you in HTML format because you
            requested it in HTML format.</p>
            <p>Node ID: <code>{self.node_id}</code></p>
            <p>Node App Data:</p>
            <pre><code>{node_app_data_yaml}</code></pre>
            <h2>Topics</h2>
            {topics_html}
            <h2>Index answer presented in YAML</h2>
            <pre><code>{json_data_yaml}</code></pre>
            <h2>Your request headers</h2>
            <pre><code>{headers_string}</code></pre>
            </body>
            </html>
            """
            return Response(
                body=html_index,
                content_type="text/html",
                headers=headers,
            )
        as_cbor = cbor2.dumps(json_data)
        return Response(
            body=as_cbor,
            content_type=CONTENT_TYPE_DTPS_INDEX_CBOR,
            headers=headers,
        )

    @async_error_catcher
    async def serve_history(self, request: Request) -> StreamResponse:
        """Serve history."""
        headers: CIMultiDict[str] = CIMultiDict()
        add_nocache_headers(headers)
        alternatives = self.get_header_alternatives(request)
        multidict_update(headers, alternatives)
        self._add_own_headers(headers)
        dash_separated_topic_name = request.match_info["topic"]
        try:
            source = self.resolve(dash_separated_topic_name)
        except KeyError as error:
            raise HTTPNotFound(text=f"{error}", headers=headers) from error
        if not isinstance(source, OurQueue):
            raise HTTPNotFound(
                text=f"Topic {dash_separated_topic_name} is not a queue.",
                headers=headers,
            )
        queue = self.get_object_queue(source.topic_name)
        history = {}
        for item in queue.stored:
            data_saved = queue.saved[item]
            content = self.blob_manager.get_blob(data_saved.digest)
            data_ready = queue.get_data_ready(
                data_saved,
                content,
                inline_data=False,
            )
            history[data_ready.index] = asdict(data_ready)
        data = cbor2.dumps(history)
        raw_data = RawData(data, CONTENT_TYPE_TOPIC_HISTORY_CBOR)
        return self.visualize_data(
            request,
            f"History for {dash_separated_topic_name}.",
            raw_data,
            headers,
            is_streamable=False,
            is_pushable=False,
        )

    @async_error_catcher
    async def serve_meta(self, request: Request) -> StreamResponse:
        """Serve meta."""
        headers: CIMultiDict[str] = CIMultiDict()
        add_nocache_headers(headers)
        header_alternatives = self.get_header_alternatives(request)
        multidict_update(headers, header_alternatives)
        self._add_own_headers(headers)
        dash_separated_topic_name = request.match_info["topic"]
        try:
            source = self.resolve(dash_separated_topic_name)
        except KeyError as error:
            raise HTTPNotFound(text=f"{error}", headers=headers) from error
        index_internal = await source.get_meta_info(request.url.path, self)
        index_wire = index_internal.to_wire()
        index_wire_dictionary = asdict(index_wire)
        data = cbor2.dumps(index_wire_dictionary)
        raw_data = RawData(data, CONTENT_TYPE_DTPS_INDEX_CBOR)
        return self.visualize_data(
            request,
            f"Meta for {dash_separated_topic_name}",
            raw_data,
            headers,
            is_streamable=False,
            is_pushable=False,
        )

    def resolve(self, url0: str) -> AbstractSource:
        """Resolve."""
        after: str | None
        url = url0
        if url and not url.endswith("/"):
            url, _, after = url.rpartition("/")
            url += "/"
        else:
            after = None
        topic_name = TopicNameV.from_relative_url(url)
        return self.resolve_topic_name(topic_name, url0, after)

    def _get_no_subtopics_message(
        self,
        topic_name: TopicNameV,
        url0: str,
        sources: dict[TopicNameV, AbstractSource],
    ) -> str:
        for (
            forward_info_topic_name,
            forward_info,
        ) in self._mount_points.items():
            if (
                forward_info.established is None
                and forward_info_topic_name.is_prefix_of(topic_name)
                is not None
            ):
                return (
                    f"This topic is on the mount point {topic_name} but the "
                    "connection is not established yet.\n"
                )
            message = f"Cannot find a matching topic for {url0!r}.\n"
            dash_separated_topic_name = topic_name.as_dash_sep()
            message += f"| topic name: {dash_separated_topic_name}\n"
            message += "| sources: \n"
            for source_topic_name, source in sources.items():
                dash_separated_topic_name = source_topic_name.as_dash_sep()
                source_class = type(source)
                message += (
                    f"| {dash_separated_topic_name!r}: {source_class.__name__}"
                    "\n"
                )
            if self._mount_points:
                message += "| mount points: \n"
                for (
                    inner_forward_info_topic_name,
                    inner_forward_info,
                ) in self._mount_points.items():
                    dash_separated_topic_name = (
                        inner_forward_info_topic_name.as_dash_sep()
                    )
                    forward_info_class = type(inner_forward_info)
                    message += (
                        f"| {dash_separated_topic_name!r}: "
                        f"{forward_info_class.__name__} established="
                        f"{inner_forward_info.established is not None}\n"
                    )
            else:
                message += "| no mount points\n"
        return message

    def resolve_topic_name(
        self,
        topic_name: TopicNameV,
        url0: str,
        after: str | None = None,
    ) -> AbstractSource | SourceComposition:
        """Resolve topic name."""
        sources = self.iterate_sources()
        subtopics = []
        for source_topic_name, source in sources.items():
            if source_topic_name.is_root() and not topic_name.is_root():
                continue
            potential_source = self._get_source(
                source,
                topic_name,
                after,
                source_topic_name,
            )
            if potential_source is not None:
                return potential_source
            is_pref_2 = topic_name.is_prefix_of(source_topic_name)
            if is_pref_2 is not None:
                subtopics.append((source_topic_name, *is_pref_2, source))
        if not subtopics:
            message = self._get_no_subtopics_message(topic_name, url0, sources)
            raise KeyError(message)
        origin_node = self.node_id
        established = self._mount_points[topic_name].established
        if (
            topic_name in self._mount_points
            and established is not None
            and established.found_metadata.answering is not None
        ):
            origin_node = established.found_metadata.answering
        unique_id = get_unique_id(origin_node, topic_name)
        subsources = {}
        for _, _, components, source in subtopics:
            source_topic_name = TopicNameV.from_components(components)
            subsources[source_topic_name] = source
        source_composition = SourceComposition(
            topic_name,
            subsources,
            unique_id,
            origin_node,
        )
        if after is not None:
            return source_composition.get_inside_after(after)
        return source_composition

    @staticmethod
    def _get_source(
        source: AbstractSource,
        topic_name: TopicNameV,
        after: str | None,
        source_topic_name: TopicNameV,
    ) -> AbstractSource | None:
        if source_topic_name == topic_name:
            if after is not None:
                return source.get_inside_after(after)
            return source
        is_pref = source_topic_name.is_prefix_of(topic_name)
        if is_pref is not None:
            _, rest = is_pref
            return source.resolve_extra(rest, after)
        return None

    @staticmethod
    def _iterate_sources_key(
        sources_item: tuple[TopicNameV, AbstractSource],
    ) -> int:
        return len(sources_item[0].components)

    def iterate_sources(self) -> dict[TopicNameV, AbstractSource]:
        """Iterate sources."""
        sources: dict[TopicNameV, AbstractSource] = {}
        queue: ForwardedQueue | OurQueue
        for topic_name in self.forwarded:
            queue = ForwardedQueue(topic_name)
            sources[topic_name] = queue
        for topic_name in self.object_queues:
            queue = OurQueue(topic_name)
            sources[topic_name] = queue
        sources_items = sources.items()
        sorted_sources = sorted(
            sources_items,
            key=self._iterate_sources_key,
            reverse=True,
        )
        return dict(sorted_sources)

    @async_error_catcher
    async def serve_delete(self, request: Request) -> StreamResponse:
        """Serve delete."""
        headers: CIMultiDict[str] = CIMultiDict()
        self._add_own_headers(headers)
        add_nocache_headers(headers)
        dash_separated_topic_name = request.match_info["topic"]
        try:
            source = self.resolve(dash_separated_topic_name)
        except KeyError as error:
            message = (
                f"404: {request.url!r}\nCannot find topic "
                f"'{dash_separated_topic_name}':\n{error.args[0]}"
            )
            return HTTPNotFound(text=message, headers=headers)
        result = await source.delete(dash_separated_topic_name, self)
        if isinstance(result, TransformError):
            return Response(
                status=result.http_code,
                text=result.message,
                headers=headers,
            )
        return Response(body="", headers=headers)

    @async_error_catcher
    async def serve_get(self, request: Request) -> StreamResponse:
        """Serve `GET` request."""
        with self._log_request(request):
            headers: CIMultiDict[str] = CIMultiDict()
            self._add_own_headers(headers)
            add_nocache_headers(headers)
            dash_separated_topic_name = request.match_info["topic"]
            if dash_separated_topic_name == "":
                return await self.serve_index(request)
            try:
                source = self.resolve(dash_separated_topic_name)
            except KeyError as error:
                message = (
                    f"404: {request.url!r}\nCannot find topic "
                    f"'{dash_separated_topic_name}':\n{error.args[0]}"
                )
                return HTTPNotFound(text=message, headers=headers)
            header_alternatives = self.get_header_alternatives(request)
            multidict_update(headers, header_alternatives)
            properties = source.get_properties(self)
            put_meta_headers(headers, properties)
            origin_node = await source.get_source_node_id(self)
            if origin_node is not None:
                headers.add(HEADER_DATA_ORIGIN_NODE_ID, origin_node)
            if isinstance(source, ForwardedQueue):
                # Optimization for streaming
                return await self.serve_get_proxied(
                    request,
                    self.forwarded[source.topic_name],
                )
            try:
                resolved_data = await source.get_resolved_data(
                    dash_separated_topic_name,
                    self,
                    request,
                )
            except KeyError as error:
                self.logger.exception(
                    "serve_get: %r -> %r",
                    request.url,
                    dash_separated_topic_name,
                )
                raise HTTPNotFound(
                    text=f"404\n{error}",
                    headers=headers,
                ) from error
            if isinstance(resolved_data, HTTPResponse):
                return resolved_data
            raw_data = self._get_raw_data(resolved_data)
            accept_headers = request.headers.get("accept", "")
            if (
                accept_headers
                and isinstance(raw_data, RawData)
                and "html" not in accept_headers
            ):
                raw_data = raw_data.get_as(accept_headers)
            return self.visualize_data(
                request,
                dash_separated_topic_name,
                raw_data,
                headers,
                is_streamable=properties.streamable,
                is_pushable=properties.pushable,
            )

    @staticmethod
    def _get_raw_data(
        resolved_data: ResolvedData,
    ) -> RawData | NotAvailableYet:
        raw_data: RawData | NotAvailableYet
        if isinstance(resolved_data, RawData):
            raw_data = resolved_data
        elif isinstance(resolved_data, Native):
            raw_data = RawData.cbor_from_native_object(resolved_data.object_)
            # TODO: Implement
        elif isinstance(resolved_data, NotAvailableYet):
            raw_data = resolved_data
        elif isinstance(resolved_data, NotFound):
            message = f"Cannot handle {resolved_data!r}."
            raise NotImplementedError(message)
        else:
            raise TypeError
        return raw_data

    def make_friendly_visualization(
        self,
        title: str,
        initial_data_html: str,
        *,
        is_image_content: bool,
        content_type: str,
        pushable: bool,
        initial_push_value: str,
        initial_push_contenttype: str,
        streamable: bool,
    ) -> StreamResponse:
        """Make friendly visualization."""
        headers: CIMultiDict[str] = CIMultiDict()
        cbor_source = "https://cdn.jsdelivr.net/npm/cbor-js@0.1.0/cbor.min.js"
        js_yaml_source = (
            "https://cdnjs.cloudflare.com/ajax/libs/js-yaml/4.1.0/"
            "js-yaml.min.js"
        )
        html_index = f"""
        <html lang="en">
        <head>
            <title>{title}</title>
            <link rel="stylesheet" href="/static/style.css">
            <script src="/static/send.js"></script>
            <script src="{cbor_source}"></script>
            <script src="{js_yaml_source}"></script>
        </head>
        <body>
        <h1>{title}</h1>
        <p>This response coming to you in HTML format because you requested it
        in HTML format.</p>
        <p>Content type: <code>{content_type}</code></p>
        """
        if is_image_content:
            html_index += f"""
            <img id="data_field_image"
            src="data:{content_type};base64,{initial_data_html}" alt="image"/>
            """
        else:
            html_index += f"""
            <pre id="data_field"><code>{initial_data_html}</code></pre>
            """
        if pushable:
            html_index += f"""
            <h3>Push to queue</h3>
            <textarea id="myTextAreaContentType">{initial_push_contenttype}
            </textarea>
            <textarea id="myTextArea">{initial_push_value}</textarea>
            <br/>
            <button id="myButton">push</button>
            """
        if streamable:
            html_index += """
            <p>Streaming is available for this topic.</p>
            <pre id="result"></pre>
            """
        return Response(
            body=html_index,
            headers=headers,
            content_type="text/html",
        )

    def visualize_data(
        self,
        request: Request,
        title: str,
        raw_data: RawData | NotAvailableYet,
        headers: CIMultiDict[str],
        *,
        is_streamable: bool,
        is_pushable: bool,
    ) -> StreamResponse:
        """Visualize data."""
        accept_headers = request.headers.get("accept", "")
        accepts_html = "text/html" in accept_headers
        if isinstance(raw_data, RawData):
            if (
                raw_data.content_type != "text/html"
                and accepts_html
                and (
                    is_structure(raw_data.content_type)
                    or is_image(raw_data.content_type)
                )
            ):
                if is_structure(raw_data.content_type):
                    is_image_content = False
                    initial_data_html = raw_data.get_as_yaml()
                else:
                    # Convert to base64
                    is_image_content = True
                    encoded = base64.b64encode(raw_data.content)
                    initial_data_html = encoded.decode("ascii")
                return self.make_friendly_visualization(
                    title,
                    initial_data_html,
                    streamable=is_streamable,
                    pushable=is_pushable,
                    initial_push_value=initial_data_html,
                    initial_push_contenttype=raw_data.content_type,
                    is_image_content=is_image_content,
                    content_type=raw_data.content_type,
                )
            return Response(
                body=raw_data.content,
                content_type=raw_data.content_type,
                headers=headers,
            )
        if isinstance(raw_data, NotAvailableYet):
            if accepts_html:
                html_index = f"""
                <html lang="en">
                <head>
                <style>
                pre {{
                    background-color: #eee;
                    padding: 10px;
                    border: 1px solid #999;
                    border-radius: 5px;
                }}
                </style>
                <title>{title}</title>
                </head>
                <body>
                <h1>{title}</h1>
                <p>There is no data yet to visualize.</p>
                </body>
                </html>
                """
                return Response(
                    body=html_index,
                    content_type="text/html",
                    status=200,
                    headers=headers,
                )
            body = "204 - No data yet."
            return Response(
                body=body,
                content_type="text/plain",
                status=204,
                headers=headers,
            )
        message = f"Cannot handle {raw_data!r}."
        raise AssertionError(message)

    @async_error_catcher
    async def serve_get_proxied(
        self,
        request: Request,
        forwarded_topic: ForwardedTopic,
    ) -> StreamResponse:
        """Return proxied response.

        Reads the response's body and creates a response with the
        proxied request's status and body, forwarding all the headers.
        """
        async with (
            self.client() as client,
            client.my_session(forwarded_topic.forward_url_data) as (
                session,
                use_url,
            ),
            session.get(use_url, headers=request.headers) as resp,
        ):
            headers: CIMultiDict[str] = CIMultiDict()
            multidict_update(headers, resp.headers)
            default_empty: list[str] = []
            headers.popall(HEADER_NO_AVAIL, default_empty)
            headers.popall(HEADER_CONTENT_LOCATION, default_empty)
            headers.add(
                HEADER_DATA_ORIGIN_NODE_ID,
                forwarded_topic.origin_node,
            )
            for resource_reachability in forwarded_topic.reachability:
                if resource_reachability.answering == self.node_id:
                    resource_reachability.benchmark.fill_headers(headers)
            source = self.get_header_alternatives(request)
            multidict_update(headers, source)
            self._add_own_headers(headers)
            response = StreamResponse(status=resp.status, headers=headers)
            await response.prepare(request)
            async for chunk in resp.content.iter_any():
                await response.write(chunk)
            return response

    def _add_own_headers(self, headers: CIMultiDict[str]) -> None:
        default: list[str] = []
        prevnodeids = headers.getall(HEADER_NODE_ID, default)
        if len(prevnodeids) > 1:
            message = (
                f"More than one {HEADER_NODE_ID} header found: {prevnodeids}"
            )
            raise ValueError(message)
        if prevnodeids:
            headers.add(HEADER_NODE_PASSED_THROUGH, prevnodeids[0])
        server_string = f"lib-dtps-http/Python/{__version__}"
        header_server = "Server"
        current_server_strings = headers.getall(header_server, default)
        if header_server not in current_server_strings:
            headers.add(header_server, server_string)
        # Leave our own
        headers.popall(HEADER_NODE_ID, None)
        headers[HEADER_NODE_ID] = self.node_id

    def _headers(self, request: Request) -> CIMultiDict[str]:
        headers: CIMultiDict[str] = CIMultiDict()
        add_nocache_headers(headers)
        self._add_own_headers(headers)
        header_alternatives = self.get_header_alternatives(request)
        multidict_update(headers, header_alternatives)
        return headers

    def _resolve(self, request: Request) -> AbstractSource:
        """Raise `HTTPNotFound`."""
        dash_separated_topic_name = request.match_info["topic"]
        try:
            return self.resolve(dash_separated_topic_name)
        except KeyError as error:
            headers = self._headers(request)
            raise HTTPNotFound(
                text=f"404\n{error}",
                headers=headers,
            ) from error

    @async_error_catcher
    async def serve_post(self, request: Request) -> Response:
        """Serve `POST` request."""
        with self._log_request(request):
            content_type_string = request.headers.get(
                "Content-Type",
                "application/octet-stream",
            )
            data = await request.read()
            content_type = ContentType(content_type_string)
            raw_data = RawData(data, content_type)
            source = self._resolve(request)
            headers = self._headers(request)
            result = await source.publish(request.url.path, self, raw_data)
            if isinstance(result, TransformError):
                return Response(
                    status=result.http_code,
                    text=result.message,
                    headers=headers,
                )
            if isinstance(result, DataReady):
                data_saved = result.as_data_saved()
                data = get_simple_cbor(data_saved)
                for resource_availability in result.availability:
                    headers.add("Location", resource_availability.url)
                return Response(
                    status=201,
                    content_type=CONTENT_TYPE_DTPS_DATAREADY_CBOR,
                    body=data,
                    headers=headers,
                )
            message = f"Cannot handle {result!r} for {source}."
            raise AssertionError(message)

    @contextmanager
    def _log_request(self, request: Request) -> Iterator[None]:
        self.logger.debug("%s %s", request.method, request.url)
        yield

    @async_error_catcher
    async def serve_patch(self, request: Request) -> Response:
        """Serve `PATCH` request."""
        with self._log_request(request):
            headers = self._headers(request)
            dash_separated_topic_name = request.match_info["topic"]
            try:
                source = self.resolve(dash_separated_topic_name)
            except KeyError as error:
                raise HTTPNotFound(
                    text=f"404\n{error.args[0]}",
                    headers=headers,
                ) from error
            topic_name = TopicNameV.from_relative_url(
                dash_separated_topic_name,
            )
            if topic_name.is_root():
                return await self.serve_patch_root(request)
            data = await request.read()
            content_type = request.headers.get(
                "Content-Type",
                "application/json",
            )
            if content_type == CONTENT_TYPE_PATCH_JSON:
                decoded = data.decode("utf-8")
                patch = JsonPatch.from_string(decoded)
            elif content_type == CONTENT_TYPE_PATCH_CBOR:
                p = cbor2.loads(data)
                patch = JsonPatch(p)
            elif content_type == CONTENT_TYPE_PATCH_YAML:
                p = yaml.safe_load(data)
                patch = JsonPatch(p)
            else:
                message = (
                    f"Unsupported content type for patch: {content_type}. I "
                    "can do JSON and CBOR."
                )
                return Response(status=415, text=message)
            result = await source.patch(
                dash_separated_topic_name,
                self,
                patch,
            )
            if isinstance(result, TransformError):
                return Response(
                    status=result.http_code,
                    text=result.message,
                    headers=headers,
                )
            if isinstance(result, DataReady):
                data_saved = result.as_data_saved()
                data = get_simple_cbor(data_saved)
                for resource_availability in result.availability:
                    headers.add("Location", resource_availability.url)
                return Response(
                    status=201,
                    content_type=CONTENT_TYPE_DTPS_DATAREADY_CBOR,
                    body=data,
                    headers=headers,
                )
            message = f"Cannot handle {result!r}"
            raise AssertionError(message)

    @async_error_catcher
    async def serve_patch_root(self, request: Request) -> Response:
        """Serve `PATCH` request."""
        with self._log_request(request):
            data = await request.read()
            content_type = request.headers.get(
                "Content-Type",
                "application/json",
            )
            if content_type == CONTENT_TYPE_PATCH_JSON:
                patch_string = data.decode("utf-8")
                json_patch = JsonPatch.from_string(patch_string)
            elif content_type == CONTENT_TYPE_PATCH_CBOR:
                patch = cbor2.loads(data)
                json_patch = JsonPatch(patch)
            elif content_type == CONTENT_TYPE_PATCH_YAML:
                patch = yaml.safe_load(data)
                json_patch = JsonPatch(patch)
            else:
                message = (
                    f"Unsupported content type for patch: {content_type}. I "
                    "can do JSON and CBOR."
                )
                return Response(status=415, text=message)
            for operation in json_patch._ops:
                if isinstance(operation, RemoveOperation):
                    topic_name = topic_name_from_json_pointer(
                        operation.location,
                    )
                    if topic_name.is_root():
                        message = (
                            "Cannot create root topic "
                            f"(path = {operation.path!r})."
                        )
                        raise ValueError(message)
                    dash_separated_topic_name = topic_name.as_dash_sep()
                    self.logger.info(
                        "deleting topic: '%s'",
                        dash_separated_topic_name,
                    )
                    await self.remove_object_queue(topic_name)
                elif isinstance(operation, AddOperation):
                    topic_name = topic_name_from_json_pointer(
                        operation.location,
                    )
                    if topic_name.is_root():
                        message = (
                            "Cannot create root topic "
                            f"(path = {operation.path!r})"
                        )
                        raise ValueError(message)
                    value = operation.operation["value"]
                    topic_reference = TopicRefAdd.from_json(value)
                    await self.create_object_queue(
                        topic_name,
                        topic_reference.content_info,
                        topic_properties=topic_reference.properties,
                        bounds=topic_reference.bounds,
                        app_data=topic_reference.app_data,
                    )
                    dash_separated_topic_name = topic_name.as_dash_sep()
                    self.logger.info(
                        "Created new topic: '%s'",
                        dash_separated_topic_name,
                    )
                elif isinstance(
                    operation,
                    ReplaceOperation
                    | MoveOperation
                    | TestOperation
                    | CopyOperation,
                ):
                    return Response(status=405)
                else:
                    message = f"Cannot handle {operation!r}."
                    raise NotImplementedError(message)
            headers = self._headers(request)
            return Response(status=200, headers=headers)

    @async_error_catcher
    async def serve_blob(self, request: Request) -> Response:
        """Serve blob."""
        headers: CIMultiDict[str] = CIMultiDict()
        header_alternatives = self.get_header_alternatives(request)
        multidict_update(headers, header_alternatives)
        self._add_own_headers(headers)
        digest = Digest(request.match_info["digest"])
        content_type_base64 = request.match_info["content_type_base64"]
        encoded_content_type = content_type_base64.encode()
        urlsafe_content_type = base64.urlsafe_b64decode(encoded_content_type)
        content_type = urlsafe_content_type.decode("ascii")
        if digest in self.blob_manager.blobs:
            blob = self.blob_manager.blobs[digest]
            return Response(
                body=blob.content,
                headers=headers,
                content_type=content_type,
            )
        message = f"Cannot resolve blob: {request.url}\ndigest: {digest!r}"
        self.logger.exception(message)
        raise HTTPNotFound(text=message, headers=headers)

    @async_error_catcher
    async def serve_events(
        self,
        request: Request,
    ) -> WebSocketResponse:
        """Serve events."""
        send_data = SEND_DATA_ARGNAME in request.query
        dash_separated_topic_name = request.match_info["topic"]
        topic_name = TopicNameV.from_relative_url(dash_separated_topic_name)
        if (
            topic_name not in self.object_queues
            and topic_name not in self.forwarded
        ):
            headers: CIMultiDict[str] = CIMultiDict()
            self._add_own_headers(headers)
            message = (
                f"Cannot resolve topic: {request.url}\ntopic: "
                f"{dash_separated_topic_name!r}"
            )
            raise HTTPNotFound(text=message, headers=headers)
        headers_proxy = request.headers
        if HEADER_MAX_FREQUENCY in headers_proxy:
            max_frequency_string = headers_proxy[HEADER_MAX_FREQUENCY]
            try:
                max_frequency = float(max_frequency_string)
            except ValueError as error:
                message = (
                    f"Cannot interpret header {HEADER_MAX_FREQUENCY} set to "
                    f"{max_frequency_string:r}."
                )
                raise HTTPBadRequest(text=message) from error
        else:
            max_frequency = None
        self.logger.debug("serve_events: max_frequency=%s", max_frequency)
        websocket = WebSocketResponse()
        header_alternatives = self.get_header_alternatives(request)
        multidict_update(websocket.headers, header_alternatives)
        self._add_own_headers(websocket.headers)
        if topic_name in self.forwarded:
            forwarded_topic = self.forwarded[topic_name]
            for resource_reachability in forwarded_topic.reachability:
                if resource_reachability.answering == self.node_id:
                    resource_reachability.benchmark.fill_headers(
                        websocket.headers,
                    )
            if forwarded_topic.forward_url_events is None:
                message = (
                    f"Forwarding for topic {topic_name!r} is not enabled."
                )
                raise HTTPBadRequest(reason=message)
            await websocket.prepare(request)
            await self.serve_events_forwarder(
                websocket,
                forwarded_topic,
                max_frequency,
                inline_data=send_data,
            )
            return websocket
        queue = self.object_queues[topic_name]
        websocket.headers[HEADER_DATA_UNIQUE_ID] = (
            queue.topic_reference.unique_id
        )
        websocket.headers[HEADER_DATA_ORIGIN_NODE_ID] = (
            queue.topic_reference.origin_node
        )
        await websocket.prepare(request)
        self.logger.debug(
            "serve_events: %s topic_name=%s send_data=%s",
            request.url,
            dash_separated_topic_name,
            send_data,
        )
        exit_event = Event()
        channel_info = queue.get_channel_info()
        self.logger.debug(
            "serve_events: %s sending %s",
            request.url,
            channel_info,
        )
        data = get_tagged_cbor(channel_info)
        await websocket.send_bytes(data)
        period = 1 / max_frequency if max_frequency is not None else 0
        every_once_in_a_while = EveryOnceInAWhile(period)
        self.number_sent = 0
        coroutine = self._read_message(websocket, exit_event)
        task = asyncio.create_task(coroutine)
        self.tasks.append(task)
        coroutine = self._serve(
            queue,
            websocket,
            exit_event,
            every_once_in_a_while,
            max_frequency,
            task,
            send_data=send_data,
        )
        task = asyncio.create_task(coroutine)
        self.tasks.append(task)
        await task
        return websocket

    @async_error_catcher
    async def _serve(
        self,
        queue: ObjectQueue,
        websocket: WebSocketResponse,
        exit_event: Event,
        every_once_in_a_while: EveryOnceInAWhile,
        max_frequency: float | None,
        task: Task[None],
        *,
        send_data: bool,
    ) -> None:
        try:
            send_message = self._get_send_message(
                websocket,
                exit_event,
                every_once_in_a_while,
                send_data=send_data,
            )
            async with queue.subscribe_context(
                send_message,
                max_frequency=max_frequency,
            ):
                if queue.stored:
                    last = queue.last()
                    last_data = queue.last_data()
                    insert_notification = InsertNotification(last, last_data)
                    await send_message(queue, insert_notification)
                await exit_event.wait()
        finally:
            try:
                await websocket.close()
            except Exception:
                self.logger.exception(
                    "serve_events: could not close websocket.",
                )
            task.cancel()

    @staticmethod
    @async_error_catcher
    async def _read_message(
        websocket: WebSocketResponse,
        exit_event: Event,
    ) -> None:
        while True:
            if websocket.closed:
                break
            websocket_message = await websocket.receive()
            if websocket_message.type in (
                WSMsgType.CLOSE,
                WSMsgType.CLOSED,
                WSMsgType.CLOSING,
            ):
                exit_event.set()
                break

    def _get_send_message(
        self,
        websocket: WebSocketResponse,
        exit_event: Event,
        every_once_in_a_while: EveryOnceInAWhile,
        *,
        send_data: bool,
    ) -> Any:
        @async_error_catcher
        async def send_message(
            queue: ObjectQueue,
            insert_notification: InsertNotification,
        ) -> None:
            """Send message."""
            data_saved = insert_notification.data_saved
            digest = data_saved.digest
            self.number_sent += 1
            data_ready = queue.get_data_ready(
                data_saved,
                insert_notification.raw_data.content,
                inline_data=send_data,
            )
            if websocket.closed:
                exit_event.set()
                return
            if every_once_in_a_while.now():
                try:
                    data = get_tagged_cbor(data_ready)
                    await websocket.send_bytes(data)
                except ConnectionResetError:
                    exit_event.set()
                if send_data:
                    chunk = Chunk(
                        digest,
                        0,
                        1,
                        0,
                        insert_notification.raw_data.content,
                    )
                    try:
                        data = get_tagged_cbor(chunk)
                        await websocket.send_bytes(data)
                    except ConnectionResetError:
                        exit_event.set()

        return send_message

    @async_error_catcher
    async def serve_push_stream(
        self,
        request: Request,
    ) -> WebSocketResponse:
        """Serve `PUSH` request."""
        headers: CIMultiDict[str] = CIMultiDict()
        self._add_own_headers(headers)
        dash_separated_topic_name = request.match_info["topic"]
        try:
            source = self.resolve(dash_separated_topic_name)
        except KeyError as error:
            raise HTTPNotFound(text=f"{error.args[0]}") from error
        if isinstance(source, OurQueue):
            return await self.serve_push_stream_object_queue(request, source)
        text = f"Topic {dash_separated_topic_name} is not a queue."
        raise HTTPBadRequest(text=text)

    @async_error_catcher
    async def serve_push_stream_object_queue(
        self,
        request: Request,
        object_queue: OurQueue,
    ) -> WebSocketResponse:
        """Serve `PUSH` stream request."""
        websocket = WebSocketResponse()
        alternatives = self.get_header_alternatives(request)
        multidict_update(websocket.headers, alternatives)
        self._add_own_headers(websocket.headers)
        await websocket.prepare(request)
        queue = self.object_queues[object_queue.topic_name]
        # TODO: Respect on_shutdown
        while True:
            websocket_message = await websocket.receive()
            if websocket_message.type in (
                WSMsgType.CLOSE,
                WSMsgType.CLOSED,
                WSMsgType.CLOSING,
            ):
                break
            if websocket_message.type == WSMsgType.BINARY:
                # Read CBOR
                try:
                    cbor = cbor2.loads(websocket_message.data)
                except CBORDecodeError:
                    message = f"Cannot decode {websocket_message.data!r}."
                    self.logger.exception(message)
                    result = PushResult(result=False, message=message)
                    data = get_tagged_cbor(result)
                    await websocket.send_bytes(data)
                    continue
                # Interpret as RawData
                if not isinstance(cbor, dict):
                    message = f"Cannot handle {cbor!r}."
                    self.logger.exception(message)
                    result = PushResult(result=False, message=message)
                    data = get_tagged_cbor(result)
                    await websocket.send_bytes(data)
                    continue
                if RawData.__name__ in cbor:
                    inside = cbor[RawData.__name__]
                    raw_data = RawData(
                        inside["content"],
                        inside["content_type"],
                    )
                    await queue.publish(raw_data)
                    result = PushResult(result=True, message="")
                    data = get_tagged_cbor(result)
                    try:
                        await websocket.send_bytes(data)
                    except ConnectionResetError:
                        self.logger.info("Client terminated connection.")
                        break
                else:
                    message = f"Cannot handle {cbor!r}."
                    self.logger.exception(message)
                    result = PushResult(result=False, message=message)
                    data = get_tagged_cbor(result)
                    await websocket.send_bytes(data)
                    continue
            else:
                message = (
                    f"Cannot handle message type {websocket_message.type!r}."
                )
                self.logger.exception(message)
                result = PushResult(result=False, message=message)
                data = get_tagged_cbor(result)
                await websocket.send_bytes(data)
        await websocket.close()
        return websocket

    @async_error_catcher
    async def _serve_events_forward(
        self,
        websocket: WebSocketResponse,
        forwarded_topic: ForwardedTopic,
        max_frequency: float | None,
        *,
        inline_data: bool,
    ) -> None:
        url = forwarded_topic.forward_url_events_inline_data
        if inline_data:
            if url is not None:
                await self.serve_events_forward_simple(websocket, url)
            elif (url := forwarded_topic.forward_url_events) is not None:
                await self.serve_events_forwarder_one(
                    websocket,
                    url,
                    inline_data_send=inline_data,
                    inline_data_receive=False,
                    max_frequency=max_frequency,
                )
            else:
                message = "Events not supported."
                raise ValueError(message)
        else:
            if url is not None:
                inline_data_receive = True
            elif (url := forwarded_topic.forward_url_events) is not None:
                inline_data_receive = False
            else:
                message = "Events not supported."
                raise ValueError(message)
            await self.serve_events_forwarder_one(
                websocket,
                url,
                inline_data_send=inline_data,
                inline_data_receive=inline_data_receive,
                max_frequency=max_frequency,
            )

    @async_error_catcher
    async def serve_events_forwarder(
        self,
        websocket: WebSocketResponse,
        forwarded_topic: ForwardedTopic,
        max_frequency: float | None,
        *,
        inline_data: bool,
    ) -> None:
        """Serve events forwarder."""
        while not self.shutdown_event.is_set():
            if websocket.closed:
                break
            try:
                await self._serve_events_forward(
                    websocket,
                    forwarded_topic,
                    max_frequency,
                    inline_data=inline_data,
                )
            except CancelledError:
                raise
            except Exception:
                formated_traceback = traceback.format_exc()
                self.logger.exception(
                    "Exception in serve_events_forwarder_one: %s",
                    formated_traceback,
                )
                await asyncio.sleep(1)

    if TYPE_CHECKING:

        def client(
            self,
            nickname: str | None = None,
        ) -> AbstractAsyncContextManager[DTPSClient]:
            """Client."""
    else:

        @asynccontextmanager
        async def client(
            self,
            nickname: str | None = None,
        ) -> AsyncIterator[DTPSClient]:
            """Client."""
            async with DTPSClient.create(
                nickname=nickname,
                shutdown_event=self.shutdown_event,
            ) as client:
                yield client

    @async_error_catcher
    async def serve_events_forward_simple(
        self,
        ws_to_write: WebSocketResponse,
        url: URL,
    ) -> None:
        """Iterate using direct data in websocket."""
        self.logger.debug(
            "serve_events_forward_simple: %s [no overhead forwarding]",
            url,
        )
        async with (
            self.client() as client,
            client.my_session(url) as (session, use_url),
            session.ws_connect(use_url) as websocket,
        ):
            async for message in websocket:
                if message.type in (
                    WSMsgType.CLOSE,
                    WSMsgType.CLOSED,
                    WSMsgType.CLOSING,
                ):
                    break
                if message.type == WSMsgType.TEXT:
                    await ws_to_write.send_str(message.data)
                elif message.type == WSMsgType.BINARY:
                    await ws_to_write.send_bytes(message.data)
                else:
                    self.logger.warning(
                        "Unknown message type %s",
                        message.type,
                    )

    @async_error_catcher
    async def serve_events_forwarder_one(
        self,
        websocket: WebSocketResponse,
        url_websockets: URLWS,
        *,
        inline_data_receive: bool,
        inline_data_send: bool,
        max_frequency: float | None,
    ) -> None:
        """Serve events forwarder one."""
        if not isinstance(url_websockets, URL):
            raise TypeError
        self.logger.debug(
            "serve_events_forwarder_one: %s inline_data_receive=%s "
            "inline_data_send=%s",
            url_websockets,
            inline_data_receive,
            inline_data_send,
        )
        async with self.client() as client:
            callback = self._get_callback(
                websocket,
                url_websockets,
                inline_data_send=inline_data_send,
            )
            listen_data_interface = await client.listen_url_events3(
                url_websockets=url_websockets,
                inline_data=inline_data_receive,
                raise_on_error=False,
                add_silence=None,
                max_frequency=max_frequency,
                callback=callback,
            )
            await listen_data_interface.wait_for_done_or_stop_on_event(
                self.shutdown_event,
            )


def add_nocache_headers(headers: CIMultiDict[str]) -> None:
    headers.update(HEADER_NO_CACHE)
    monotonic_ns = time.monotonic_ns()
    headers["Cookie"] = f"help-no-cache={monotonic_ns}"


def get_unique_id(node_id: NodeID, topic_name: TopicNameV) -> SourceID:
    if topic_name.is_root():
        return cast(SourceID, node_id)
    url = topic_name.as_relative_url()
    return cast(SourceID, f"{node_id}:{url}")


def put_meta_headers(
    headers: CIMultiDict[str],
    topic_properties: TopicProperties,
) -> None:
    if topic_properties.streamable:
        put_link_header(
            headers,
            f"{EVENTS_SUFFIX}/",
            REL_EVENTS_NODATA,
            "websocket",
        )
        put_link_header(
            headers,
            f"{EVENTS_SUFFIX}/?send_data=1",
            REL_EVENTS_DATA,
            "websocket",
        )
    if topic_properties.pushable:
        put_link_header(
            headers,
            f"{REL_STREAM_PUSH_SUFFIX}/",
            REL_STREAM_PUSH,
            "websocket",
        )
    put_link_header(
        headers,
        f"{REL_URL_META}/",
        REL_META,
        CONTENT_TYPE_DTPS_INDEX_CBOR,
    )
    if topic_properties.has_history:
        put_link_header(
            headers,
            f"{REL_URL_HISTORY}/",
            REL_HISTORY,
            CONTENT_TYPE_TOPIC_HISTORY_CBOR,
        )


@async_error_catcher
async def update_clock(
    server: DTPSServer,
    topic_name: TopicNameV,
    interval: float,
    initial_delay: float,
) -> None:
    await asyncio.sleep(initial_delay)
    url = topic_name.as_relative_url()
    server.logger.info("Starting clock %s with interval %s...", url, interval)
    queue = server.get_object_queue(topic_name)
    while True:
        current_time = time.time_ns()
        current_time_string = str(current_time)
        encoded_current_time_string = current_time_string.encode()
        raw_data = RawData(encoded_current_time_string, MIME_JSON)
        await queue.publish(raw_data)
        try:
            await asyncio.sleep(interval)
        except CancelledError:
            url = topic_name.as_relative_url()
            server.logger.info("Clock %s cancelled.", url)
            raise


def get_simple_cbor(object_: Any) -> bytes:
    """Return simple CBOR."""
    dictionary = asdict(object_)
    return cbor2.dumps(dictionary)


def get_tagged_cbor(object_: Any) -> bytes:
    """Return tagged CBOR."""
    dictionary = asdict(object_)
    data = {
        object_.__class__.__name__: dictionary,
    }
    return cbor2.dumps(data)


def removeprefix(string: str, prefix: str) -> str:
    """Remove prefix."""
    if string.startswith(prefix):
        prefix_length = len(prefix)
        return string[prefix_length:]
    return string[:]


def topic_name_from_json_pointer(path: str) -> TopicNameV:
    """Return topic name from JSON pointer."""
    path = unescape_json_pointer(path)
    components: list[str] = []
    for component in path.split("/"):
        if not component:
            continue
        components.append(component)
    return TopicNameV.from_components(components)
