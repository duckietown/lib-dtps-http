import asyncio
import base64
import pathlib
import time
import traceback
import uuid
from asyncio import CancelledError
from contextlib import asynccontextmanager, contextmanager
from dataclasses import asdict, dataclass as original_dataclass, replace
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    cast,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    TYPE_CHECKING,
    Union,
)

import cbor2
import yaml
from aiohttp import web, WSMsgType
from aiopubsub import Hub
from jsonpatch import (
    AddOperation,
    CopyOperation,
    JsonPatch,
    MoveOperation,
    RemoveOperation,
    ReplaceOperation,
    TestOperation,
)
from multidict import CIMultiDict
from pydantic.dataclasses import dataclass

from . import __version__, logger as logger0
from .client import DTPSClient, FoundMetadata, unescape_json_pointer
from .constants import (
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
from .link_headers import put_link_header
from .object_queue import (
    ObjectQueue,
    ObjectTransformFunction,
    PostResult,
    transform_identity,
    TransformError,
)
from .structures import (
    ChannelMsgs,
    Chunk,
    ConnectionEstablished,
    ContentInfo,
    DataReady,
    Digest,
    ErrorMsg,
    FinishedMsg,
    get_digest,
    InsertNotification,
    is_image,
    is_structure,
    LinkBenchmark,
    ListenURLEvents,
    ProxyJob,
    PushResult,
    RawData,
    Registration,
    ResourceAvailability,
    SilenceMsg,
    TopicProperties,
    TopicReachability,
    TopicRef,
    TopicRefAdd,
    TopicsIndex,
    TopicsIndexWire,
    WarningMsg,
)
from .types import ContentType, NodeID, SourceID, TopicNameV, URLString
from .types_of_source import (
    ForwardedQueue,
    Native,
    NotAvailableYet,
    NotFound,
    OurQueue,
    Source,
    SourceComposition,
)
from .urls import parse_url_unescape, URL, URLIndexer, URLWS
from .utils import async_error_catcher, multidict_update

SEND_DATA_ARGNAME = "send_data"
ROOT = TopicNameV.root()

__all__ = [
    "DTPSServer",
    "ForwardedTopic",
]


@dataclass
class ForwardedTopic:
    unique_id: SourceID  # unique id for the stream
    origin_node: NodeID  # unique id of the node that created the stream
    app_data: Dict[str, bytes]
    forward_url_data: URL
    forward_url_events: Optional[URLWS]
    forward_url_events_inline_data: Optional[URLWS]
    reachability: List[TopicReachability]
    properties: TopicProperties
    content_info: ContentInfo


@original_dataclass
class ForwardInfoEstablished:
    best_url: URL
    md: FoundMetadata
    index_internal: TopicsIndex


@original_dataclass
class ForwardInfo:
    urls: List[URLString]
    expect_node_id: Optional[NodeID]

    established: Optional[ForwardInfoEstablished]
    mask_origin: bool
    task: "asyncio.Task[Any]"

    def __post_init__(self):
        for u in self.urls:
            parse_url_unescape(u)


def get_static_dir() -> str:
    options = [
        pathlib.Path(__file__).parent / "static",
        pathlib.Path(__file__).parent.parent / "static",
        pathlib.Path(__file__).parent.parent.parent / "static",
    ]
    for o in options:
        if o.exists():
            return str(o)

    msg = f"Static directory not found: {options}."
    raise FileNotFoundError(msg)


from dataclasses import dataclass
from typing import Set


@dataclass
class SavedBlob:
    content: bytes
    who_needs_it: Set[str]
    deadline: float


class BlobManager:
    blobs: Dict[Digest, SavedBlob]
    blobs_forgotten: Dict[Digest, float]

    def __init__(self):
        self.blobs = {}
        self.blobs_forgotten = {}

    def cleanup_blobs(self) -> None:
        now = time.time()
        todrop = []

        for digest, sb in list(self.blobs.items()):
            no_one_needs_it = len(sb.who_needs_it) == 0
            deadline_passed = now > sb.deadline
            if no_one_needs_it and deadline_passed:
                todrop.append(digest)

        for digest in todrop:
            # print(f"Dropping blob {digest} because deadline passed")
            self.blobs.pop(digest, None)
            self.blobs_forgotten[digest] = now

    def release_blob(self, digest: Digest, who_needs_it: str):
        if digest not in self.blobs:
            return
        sb = self.blobs[digest]
        sb.who_needs_it.remove(who_needs_it)
        if len(sb.who_needs_it) == 0:
            deadline_passed = time.time() > sb.deadline
            if deadline_passed:
                self.blobs.pop(digest, None)
                self.blobs_forgotten[digest] = time.time()

    def save_blob(self, content: bytes, who_needs_it: str) -> Digest:
        digest = get_digest(content)
        if digest not in self.blobs:
            self.blobs[digest] = SavedBlob(
                content=content,
                who_needs_it={who_needs_it},
                deadline=time.time(),
            )
        else:
            sb = self.blobs[digest]
            sb.who_needs_it.add(who_needs_it)
        return digest

    def save_blob_deadline(self, content: bytes, deadline: float) -> Digest:
        digest = get_digest(content)
        if digest not in self.blobs:
            self.blobs[digest] = SavedBlob(
                content=content,
                who_needs_it=set(),
                deadline=deadline,
            )
        else:
            sb = self.blobs[digest]
            sb.deadline = max(deadline, sb.deadline)
        return digest


class DTPSServer:
    node_id: NodeID

    # set when we have been going through the startup process
    started: asyncio.Event

    _oqs: Dict[TopicNameV, ObjectQueue]
    _mount_points: Dict[TopicNameV, ForwardInfo]
    _forwarded: Dict[TopicNameV, ForwardedTopic]

    tasks: "List[asyncio.Task[Any]]"
    digest_to_urls: Dict[str, List[URL]]
    node_app_data: Dict[str, bytes]
    registrations: List[Registration]
    available_urls: "List[URLString]"
    nickname: str

    blob_manager: BlobManager

    @classmethod
    def create(
        cls,
        on_startup: "Sequence[Callable[[DTPSServer], Awaitable[None]]]" = (),
        nickname: Optional[str] = None,
    ) -> "DTPSServer":
        return cls(on_startup=on_startup, nickname=nickname)

    def __init__(
        self,
        *,
        on_startup: "Sequence[Callable[[DTPSServer], Awaitable[None]]]" = (),
        nickname: Optional[str] = None,
    ) -> None:
        if nickname is None:
            nickname = str(id(self))
        self.nickname = nickname
        self.logger = logger0.getChild(nickname)

        self.app = web.Application()

        self.node_app_data = {}
        self.node_started = time.time_ns()

        routes = web.RouteTableDef()
        self._more_on_startup = on_startup
        self.app.on_startup.append(self.on_startup)
        # self.app.on_shutdown.append(self.on_shutdown)
        routes.get("/{ignore:.*}/:blobs/{digest}/{content_type_base64:.*}")(self.serve_blob)

        routes.get("/{topic:.*}" + EVENTS_SUFFIX + "/")(self.serve_events)
        routes.get("/{topic:.*}" + REL_URL_META + "/")(self.serve_meta)
        routes.get("/{topic:.*}" + REL_URL_HISTORY + "/")(self.serve_history)
        routes.get("/{topic:.*}" + REL_STREAM_PUSH_SUFFIX + "/")(self.serve_push_stream)

        routes.get("/{topic:.*}/data/{digest}/")(self.serve_data_get)
        routes.get("/data/{digest}/")(self.serve_data_get)
        routes.post("/{topic:.*}")(self.serve_post)
        routes.patch("/{topic:.*}")(self.serve_patch)

        routes.get("/{topic:.*}")(self.serve_get)
        routes.delete("/{topic:.*}")(self.serve_delete)

        self.blob_manager = BlobManager()

        # mount a static directory for the web interface
        static_dir = get_static_dir()
        self.logger.debug(f"Using static dir: {static_dir}")
        self.app.add_routes([web.static("/static", static_dir)])
        self.app.add_routes(routes)

        self.hub = Hub()
        self._oqs = {}
        self._mount_points = {}
        self._forwarded = {}
        self.tasks = []
        self.available_urls = []
        self.node_id = NodeID(f"{self.nickname}-{str(uuid.uuid4())[:8]}")

        self.digest_to_urls = {}

        self.registrations = []

        self.started = asyncio.Event()
        self.shutdown_event = asyncio.Event()

    def add_registrations(self, registrations: Sequence[Registration]) -> None:
        self.registrations.extend(registrations)

    def has_forwarded(self, topic_name: TopicNameV) -> bool:
        return topic_name in self._forwarded

    def get_headers_alternatives(self, request: web.Request) -> CIMultiDict[str]:
        original_url = str(request.url)

        # noinspection PyProtectedMember
        sock = request.transport._sock  # type: ignore
        sockname = sock.getsockname()
        if isinstance(sockname, str):
            path = sockname.replace("/", "%2F")
            use_url = original_url.replace("http://", "http+unix://").replace("localhost", path)
        else:
            use_url = original_url

        res: CIMultiDict[str] = CIMultiDict()
        if not self.available_urls:
            res[HEADER_NO_AVAIL] = "No alternative URLs available"
            return res

        alternatives: List[str] = []
        url = use_url

        for a in self.available_urls + [
            f"http://127.0.0.1:{request.url.port}/",
            f"http://localhost:{request.url.port}/",
        ]:
            if url.startswith(a):
                for b in self.available_urls:
                    if a == b:
                        continue
                    alternative = b + removeprefix(url, a)
                    alternatives.append(alternative)

        url_URL = parse_url_unescape(URLString(url))
        if url_URL.path == "/":
            for b in self.available_urls:
                alternatives.append(b)

        for a in sorted(set(alternatives)):
            res.add(HEADER_CONTENT_LOCATION, a)
        if not alternatives:
            res[HEADER_NO_AVAIL] = f"Nothing matched {url} of {self.available_urls}"
        else:
            res.popall(HEADER_NO_AVAIL, None)
        return res

    async def add_available_url(self, url: URLString) -> None:
        if url in self.available_urls:
            return
        parse_url_unescape(url)

        self.available_urls.append(url)
        self.available_urls = sorted(list(set(self.available_urls)))
        oq = self.get_oq(TOPIC_AVAILABILITY)
        await oq.publish_json(self.available_urls)

    def remember_task(self, task: "asyncio.Task[Any]") -> None:
        """Add a task to the list of tasks to be cancelled on shutdown"""
        self.tasks.append(task)

    async def _update_lists(self):
        if TOPIC_LIST not in self._oqs:
            raise AssertionError(f"Topic {TOPIC_LIST.as_relative_url()} not found")
        if ROOT not in self._oqs:
            raise AssertionError(f"Topic {ROOT.as_relative_url()} not found")
        topics: List[TopicNameV] = []
        topics.extend(self._oqs.keys())
        topics.extend(self._forwarded.keys())
        urls = sorted([_.as_relative_url() for _ in topics])
        await self._oqs[TOPIC_LIST].publish_json(urls)

        index = self.create_root_index()
        index_wire = index.to_wire()
        await self._oqs[ROOT].publish_cbor(asdict(index_wire), CONTENT_TYPE_DTPS_INDEX_CBOR)

    async def remove_oq(self, name: TopicNameV) -> None:
        if name in self._oqs:
            self._oqs.pop(name)
            await self._update_lists()

    async def remove_forward(self, name: TopicNameV) -> None:
        if name in self._forwarded:
            self._forwarded.pop(name)
            await self._update_lists()

    async def _add_proxied_mountpoint(
        self, name: TopicNameV, node_id: Optional[NodeID], urls: List[URLString], mask_origin: bool
    ) -> None:
        if name in self._mount_points or name in self._oqs:
            raise ValueError(f"Topic {name} already exists")

        finfo = ForwardInfo(
            urls=urls,
            expect_node_id=node_id,
            mask_origin=mask_origin,
            task=None,  # type: ignore
            established=None,
        )
        self._mount_points[name] = finfo
        finfo.task = asyncio.create_task(self._ask_for_topics_continuous(name, finfo))
        self.remember_task(finfo.task)

    @async_error_catcher
    async def _ask_for_topics_continuous(self, name: TopicNameV, finfo: ForwardInfo) -> None:
        nickname = f"{self.nickname}:proxyreader({name.as_dash_sep()})"
        self.logger.debug(f"Starting {nickname} finfo = {finfo}")
        try:
            async with self._client(nickname) as dtpsclient:
                url = URLIndexer(parse_url_unescape(finfo.urls[0]))

                md = await dtpsclient.get_metadata(url)
                if md.answering is not None and finfo.expect_node_id is not None:
                    if md.answering != finfo.expect_node_id:
                        self.logger.error(f"Node {finfo.expect_node_id} expected but {md.answering} found")
                        await asyncio.sleep(1.0)
                        # continue
                # TODO: check node id
                best_url = url
                ti = await dtpsclient.ask_index(url)
                finfo.established = ForwardInfoEstablished(
                    best_url,
                    md=md,
                    index_internal=TopicsIndex({}),
                )
                await self._process_change_topics(dtpsclient, name, ti, mask_origin=finfo.mask_origin)

                async def on_data(rd: RawData) -> None:
                    od = rd.get_as_native_object()
                    ti2_ = TopicsIndexWire.from_json(od)
                    ti2 = ti2_.to_internal([best_url])
                    await self._process_change_topics(dtpsclient, name, ti2, mask_origin=finfo.mask_origin)

                ldi = await dtpsclient.listen_url(url, on_data, inline_data=True, raise_on_error=False)
                try:
                    condition = asyncio.create_task(self.shutdown_event.wait())
                    waiting = asyncio.create_task(ldi.wait_for_done())
                    await asyncio.wait([condition, waiting], return_when=asyncio.FIRST_COMPLETED)
                    # await ldi.wait_for_done()
                except:
                    await ldi.stop()
                    raise

        except Exception as e:
            self.logger.error(f"Error in _ask_for_topics_continuous: {e}")
            # await asyncio.sleep(1.0)
            raise

    async def _process_change_topics(
        self, dtpsclient: DTPSClient, prefix: TopicNameV, ti: TopicsIndex, mask_origin: bool
    ) -> None:
        info = self._mount_points[prefix]
        if info.established is None:
            raise AssertionError(f"Established is None for {prefix}")
        previous = list(info.established.index_internal.topics)
        current = list(ti.topics)
        removed = set(previous) - set(current)
        added = set(current) - set(previous)
        self.logger.debug(f"added={added!r} removed={removed!r}")

        for topic_name in removed:
            new_topic = prefix + topic_name
            if self.has_forwarded(new_topic):
                self.logger.debug("removing topic %s", new_topic)
                await self.remove_forward(new_topic)

        for topic_name in added:
            tr = ti.topics[topic_name]
            new_topic = prefix + topic_name

            if self.has_forwarded(new_topic):
                self.logger.debug("already have topic %s", new_topic)
                continue

            # self.logger.info(f"adding topic {tr}")
            possible: List[TopicReachability] = []
            for reachability in tr.reachability:
                # urlhere = new_topic.as_relative_url()
                # rurl = parse_url_unescape(reachability.url)
                metadata0 = await dtpsclient.get_metadata(parse_url_unescape(reachability.url))
                for m in metadata0.alternative_urls:  # + [rurl]:
                    reach_with_me = await dtpsclient.compute_with_hop(
                        self.node_id,
                        connects_to=m,
                        expects_answer_from=reachability.answering,
                        forwarders=reachability.forwarders,
                    )
                    if reach_with_me is not None:
                        # try:
                        #     # xx = join(m, reach_with_me.url)
                        #     xx = m
                        # except Exception:
                        #     self.logger.error(f"Could not parse {reach_with_me.url!r}")
                        #     continue
                        # else:
                        possible.append(reach_with_me)
                    else:
                        pass  # logger.info(f"Could not proxy {new_topic!r} as {urlbase} {topic_name}
                        # -> {m}")

            if not possible:
                self.logger.error(f"Topic {topic_name} cannot be reached")
                continue

            def choose_key(x: TopicReachability) -> Tuple[int, float, float]:
                return x.benchmark.complexity, x.benchmark.latency_ns, -x.benchmark.bandwidth

            possible.sort(key=choose_key)
            r = possible[0]
            url_to_use = parse_url_unescape(r.url)
            assert isinstance(url_to_use, URL), url_to_use

            metadata = await dtpsclient.get_metadata(url_to_use)

            if mask_origin:
                tr2 = replace(tr, reachability=[r])
            else:
                tr2 = replace(tr, reachability=tr.reachability + [r])

            # self.logger.info(f"adding topic {new_topic} -> {repr(url_to_use)}")

            # metadata_to_use = await dtpsclient.get_metadata(url_to_use)
            fd = ForwardedTopic(
                unique_id=tr2.unique_id,
                origin_node=tr2.origin_node,
                app_data=tr2.app_data,
                reachability=tr2.reachability,
                forward_url_data=metadata.origin,
                forward_url_events=metadata.events_url,
                forward_url_events_inline_data=metadata.events_data_inline_url,
                content_info=tr2.content_info,  # FIXME: content info
                properties=tr2.properties,
            )
            #
            # self.logger.info(
            #     f"Proxying {new_topic} as    {topic_name}  with metadata = {metadata!r} \n"
            #     # "->  \n"
            #     # f" available at\n: {json.dumps(asdict(tr), indent=2)} \n"
            #     # f" proxied at\n: {json.dumps(asdict(fd), indent=2)} \n"
            # )

            await self._add_forwarded(new_topic, fd)

    async def expose(
        self, name: TopicNameV, expect_node_id: Optional[NodeID], urls: Sequence[URLString], mask_origin: bool
    ) -> None:
        oq = self.get_oq(TOPIC_PROXIED)
        x = oq.last_data()
        d = cast(dict, x.get_as_native_object())
        urls = sorted(list(urls))
        p = ProxyJob(node_id=expect_node_id, urls=urls, mask_origin=mask_origin)
        d[name.as_dash_sep()] = asdict(p)
        await oq.publish_json(d)

        while True:
            if name in self._mount_points:
                self.logger.debug(f"Found {name} in mountpoints")
                if self._mount_points[name].established is not None:
                    self.logger.debug(f"Found {name} in mountpoints and established is not None")
                    break

            await asyncio.sleep(0.1)

    async def _add_forwarded(self, name: TopicNameV, forwarded: ForwardedTopic) -> None:
        if name in self._forwarded or name in self._oqs:
            raise ValueError(f"Topic {name} already exists")
        self._forwarded[name] = forwarded
        await self._update_lists()

    def get_oq(self, name: TopicNameV) -> ObjectQueue:
        if name in self._forwarded:
            raise ValueError(f"Topic {name.as_dash_sep()} is a forwarded one")

        return self._oqs[name]

    async def create_oq(
        self,
        name: TopicNameV,
        content_info: ContentInfo,
        tp: Optional[TopicProperties] = None,
        max_history: Optional[int] = None,
        transform: ObjectTransformFunction = transform_identity,
    ) -> ObjectQueue:
        if name in self._forwarded:
            raise ValueError(f"Topic '{name.as_dash_sep()}' is a forwarded one")
        if name in self._oqs:
            return self._oqs[name]

        unique_id = get_unique_id(self.node_id, name)

        treach = TopicReachability(
            url=name.as_relative_url(),
            answering=self.node_id,
            forwarders=[],
            benchmark=LinkBenchmark.identity(),
        )
        reachability: List[TopicReachability] = [treach]
        if tp is None:
            tp = TopicProperties.streamable_readonly()

        tr = TopicRef(
            unique_id=unique_id,
            origin_node=self.node_id,
            app_data={},
            reachability=reachability,
            created=time.time_ns(),
            properties=tp,
            content_info=content_info,
        )

        self._oqs[name] = ObjectQueue(self.hub, name, tr, max_history=max_history, transform=transform)
        await self._update_lists()
        return self._oqs[name]

    @async_error_catcher
    async def on_startup(self, _: web.Application) -> None:
        # self.logger.info("on_startup")
        content_info = ContentInfo.simple(CONTENT_TYPE_DTPS_INDEX_CBOR)

        tr = TopicRef(
            unique_id=get_unique_id(self.node_id, ROOT),
            origin_node=self.node_id,
            app_data={},
            reachability=[],
            content_info=content_info,
            properties=TopicProperties.streamable_readonly(),
            created=time.time_ns(),
        )
        self._oqs[ROOT] = ObjectQueue(self.hub, ROOT, tr, max_history=5)
        index = self.create_root_index()
        wire = index.to_wire()
        as_cbor = cbor2.dumps(asdict(wire))
        await self._oqs[ROOT].publish(RawData(content=as_cbor, content_type=CONTENT_TYPE_DTPS_INDEX_CBOR))

        content_info = ContentInfo.simple(MIME_JSON)
        tr = TopicRef(
            unique_id=get_unique_id(self.node_id, TOPIC_LIST),
            origin_node=self.node_id,
            app_data={},
            reachability=[],
            content_info=content_info,
            properties=TopicProperties.streamable_readonly(),
            created=time.time_ns(),
        )
        self._oqs[TOPIC_LIST] = ObjectQueue(self.hub, TOPIC_LIST, tr, max_history=100)

        await self.create_oq(TOPIC_LOGS, content_info=ContentInfo.simple(MIME_JSON), max_history=100)
        await self.create_oq(TOPIC_CLOCK, content_info=ContentInfo.simple(MIME_JSON), max_history=10)
        await self.create_oq(TOPIC_AVAILABILITY, content_info=ContentInfo.simple(MIME_JSON), max_history=10)
        await self.create_oq(TOPIC_STATE_SUMMARY, content_info=ContentInfo.simple(MIME_JSON), max_history=10)

        oq = await self.create_oq(TOPIC_PROXIED, content_info=ContentInfo.simple(MIME_JSON), max_history=10)
        rd = RawData(content=b"{}", content_type=MIME_JSON)
        await oq.publish(rd)
        oq.subscribe(self.on_proxied_changed)

        await self.create_oq(
            TOPIC_STATE_NOTIFICATION, content_info=ContentInfo.simple(MIME_CBOR), max_history=10
        )

        self.remember_task(asyncio.create_task(update_clock(self, TOPIC_CLOCK, 1.0, 0.0)))

        for f in self._more_on_startup:
            await f(self)

        for registration in self.registrations:
            self.remember_task(asyncio.create_task(self._register(registration)))

        self.started.set()

    async def on_proxied_changed(self, oq: ObjectQueue, _: int) -> None:
        x = cast(dict, oq.last_data().get_as_native_object())
        current = list(self._forwarded)
        topics = list(TopicNameV.from_dash_sep(_) for _ in x)

        added = set(topics) - set(current)
        removed = set(current) - set(topics)
        for r in removed:
            await self.remove_forward(r)  # XXX
        for a in added:
            proxy_job = ProxyJob.from_json(x[a.as_dash_sep()])
            await self._add_proxied_mountpoint(
                a, proxy_job.node_id, proxy_job.urls, mask_origin=proxy_job.mask_origin
            )

    async def aclose(self) -> None:
        # self.logger.info("aclose: shutting down")
        self.shutdown_event.set()
        for t in self.tasks:
            t.cancel()
        for q in self._oqs.values():
            await q.aclose()
        # await asyncio.gather(*self.tasks, return_exceptions=True)

    @async_error_catcher
    async def _register(self, r: Registration) -> None:
        n = 0
        while True:
            try:
                changes = await self._try_register(r)

            except Exception as e:
                self.logger.error(f"Error while registering {r}: {e}")
                await asyncio.sleep(1.0)
            else:
                if n == 0:
                    self.logger.debug(f"Registered as {r.topic.as_dash_sep()} on {r.switchboard_url}")
                else:
                    if changes:
                        self.logger.debug(f"Re-registered as {r.topic.as_dash_sep()} on {r.switchboard_url}")

                n += 1
                # TODO: DTSW-4782: just open a websocket connection and see when it closes
                await asyncio.sleep(10.0)

    @async_error_catcher
    async def _try_register(self, r: Registration) -> bool:
        async with self._client() as client:
            # url = parse_url_unescape(r.switchboard_url)
            if not self.available_urls:
                msg = f"Cannot register {r} because no available URLs"
                self.logger.error(msg)
                raise ValueError(msg)
            postfix = r.namespace.as_relative_url()
            urls = [cast(URLString, _ + postfix) for _ in self.available_urls]
            return await client.add_proxy(r.switchboard_url, r.topic, self.node_id, urls, mask_origin=False)

    # @async_error_catcher
    # async def on_shutdown(self, _: web.Application) -> None:
    #     self.logger.debug("DTPSServer: on_shutdown")
    #     for t in self.tasks:
    #         t.cancel()
    #     self.logger.debug("DTPSServer: on_shutdown done")

    def create_root_index(self) -> TopicsIndex:
        topics: Dict[TopicNameV, TopicRef] = {}
        for topic_name, oqs in self._oqs.items():
            qual_topic_name = topic_name
            reach = TopicReachability(
                url=qual_topic_name.as_relative_url(),
                answering=self.node_id,
                forwarders=[],
                benchmark=LinkBenchmark.identity(),
            )
            topic_ref = replace(oqs.tr, reachability=[reach])
            topics[qual_topic_name] = topic_ref

        for topic_name, fd in self._forwarded.items():
            qual_topic_name = topic_name

            tr = TopicRef(
                unique_id=fd.unique_id,
                origin_node=fd.origin_node,
                app_data={},
                reachability=fd.reachability,
                properties=fd.properties,
                created=time.time_ns(),
                content_info=fd.content_info,
            )
            topics[qual_topic_name] = tr

        for topic_name in list(topics):
            for x in topic_name.nontrivial_prefixes():
                if x not in topics:
                    reachability = [
                        TopicReachability(
                            url=x.as_relative_url(),
                            answering=self.node_id,
                            forwarders=[],
                            benchmark=LinkBenchmark.identity(),
                        ),
                    ]
                    topics[x] = TopicRef(
                        unique_id=get_unique_id(self.node_id, x),
                        origin_node=self.node_id,
                        app_data={},
                        reachability=reachability,
                        properties=TopicProperties.streamable_readonly(),
                        created=time.time_ns(),
                        content_info=ContentInfo.simple(CONTENT_TYPE_DTPS_INDEX_CBOR),
                    )

        index_internal = TopicsIndex(topics=topics)
        return index_internal

    @async_error_catcher
    async def serve_index(self, request: web.Request) -> web.Response:
        headers_s = "".join(f"{k}: {v}\n" for k, v in request.headers.items())
        # self.logger.debug(f"serve_index: {request.url} \n {headers_s}")

        index_internal = self.create_root_index()
        index_wire = index_internal.to_wire()

        headers: CIMultiDict[str] = CIMultiDict()

        add_nocache_headers(headers)
        multidict_update(headers, self.get_headers_alternatives(request))
        self._add_own_headers(headers)

        properties = self._oqs[ROOT].tr.properties
        put_meta_headers(headers, properties)
        json_data = asdict(index_wire)

        put_link_header(headers, TOPIC_PROXIED.as_relative_url(), REL_PROXIED, CONTENT_TYPE_DTPS_INDEX_CBOR)

        headers.add(HEADER_DATA_ORIGIN_NODE_ID, self.node_id)

        # get all the accept headers
        accept = []
        for _ in request.headers.getall("accept", []):
            accept.extend(_.split(","))

        if "application/cbor" not in accept and CONTENT_TYPE_DTPS_INDEX_CBOR not in accept:
            if "text/html" in accept:
                topics_html = "<ul>"
                for topic_name in sorted(list(index_internal.topics)):
                    # topic_ref = index_internal.topics[topic_name]
                    if topic_name.is_root():
                        continue
                    topics_html += (
                        f"<li><a href='{topic_name.as_relative_url()}'><code>"
                        f"{topic_name.as_relative_url()}</code></a></li>\n"
                    )
                topics_html += "</ul>"
                # language=html
                html_index = f"""
                <html lang="en">
                <head>
                <style> 
                </style>
                <link rel="stylesheet" href="/static/style.css">
                
                <script src="https://cdn.jsdelivr.net/npm/cbor-js@0.1.0/cbor.min.js"></script>
                <script src="https://cdnjs.cloudflare.com/ajax/libs/js-yaml/4.1.0/js-yaml.min.js"></script>
                <script src="/static/send.js"></script>
                <title>DTPS server</title>
                </head>
                <body>
                <h1>DTPS server</h1>

                <p> This response coming to you in HTML format because you requested it in HTML format.</p>

                <p>Node ID: <code>{self.node_id}</code></p>
                <p>Node App Data:</p>
                <pre><code>{yaml.dump(self.node_app_data, indent=3)}</code></pre>

                <h2>Topics</h2>
                {topics_html}
                <h2>Index answer presented in YAML</h2>
                <pre><code>{yaml.dump(json_data, indent=3)}</code></pre>


                <h2>Your request headers</h2>
                <pre><code>{headers_s}</code></pre>
                </body>
                </html>
                """
                return web.Response(body=html_index, content_type="text/html", headers=headers)

        as_cbor = cbor2.dumps(json_data)

        return web.Response(body=as_cbor, content_type=CONTENT_TYPE_DTPS_INDEX_CBOR, headers=headers)

    @async_error_catcher
    async def serve_history(self, request: web.Request) -> web.StreamResponse:
        headers: CIMultiDict[str] = CIMultiDict()
        add_nocache_headers(headers)
        multidict_update(headers, self.get_headers_alternatives(request))
        self._add_own_headers(headers)

        topic_name_s = request.match_info["topic"]
        try:
            source = self.resolve(topic_name_s)
        except KeyError as e:
            raise web.HTTPNotFound(text=f"{e}", headers=headers) from e

        if not isinstance(source, OurQueue):
            raise web.HTTPNotFound(text=f"Topic {topic_name_s} is not a queue", headers=headers)

        oq = self.get_oq(source.topic_name)
        presented_as = request.url.path
        history = {}
        for i in oq.stored:
            ds = oq.saved[i]
            a = oq.get_data_ready(ds, presented_as, inline_data=False)
            history[a.index] = asdict(a)

        cbor = cbor2.dumps(history)
        rd = RawData(content=cbor, content_type=CONTENT_TYPE_TOPIC_HISTORY_CBOR)
        title = f"History for {topic_name_s}"
        return self.visualize_data(request, title, rd, headers, is_streamable=False, is_pushable=False)

    @async_error_catcher
    async def serve_meta(self, request: web.Request) -> web.StreamResponse:
        headers: CIMultiDict[str] = CIMultiDict()
        add_nocache_headers(headers)
        multidict_update(headers, self.get_headers_alternatives(request))
        self._add_own_headers(headers)

        topic_name_s = request.match_info["topic"]
        try:
            source = self.resolve(topic_name_s)
        except KeyError as e:
            raise web.HTTPNotFound(text=f"{e}", headers=headers)

        # logger.debug(f"serve_meta: {request.url!r} -> {source!r}")

        index_internal = await source.get_meta_info(request.url.path, self)

        index_wire = index_internal.to_wire()
        rd = RawData(content=cbor2.dumps(asdict(index_wire)), content_type=CONTENT_TYPE_DTPS_INDEX_CBOR)
        title = f"Meta for {topic_name_s}"
        return self.visualize_data(request, title, rd, headers, is_streamable=False, is_pushable=False)

    def resolve(self, url0: str) -> Source:
        after: Optional[str]
        url = url0
        if url and not url.endswith("/"):
            url, _, after = url.rpartition("/")
            url += "/"
        else:
            after = None

        # logger.debug(f"resolve({url0!r}) - url: {url!r} after: {after!r}")
        tn = TopicNameV.from_relative_url(url)
        return self._resolve_tn(tn, url0, after)

    def _resolve_tn(self, tn: TopicNameV, url0: str, after: Optional[str] = None) -> Source:
        sources = self.iterate_sources()

        subtopics: List[Tuple[TopicNameV, Sequence[str], Sequence[str], Source]] = []

        for k, source in sources.items():
            if k.is_root() and not tn.is_root():
                continue
            if k == tn:
                if after is not None:
                    return source.get_inside_after(after)
                else:
                    return source

            if (ispref := k.is_prefix_of(tn)) is not None:
                matched, rest = ispref
                return source.resolve_extra(rest, after)

            if (ispref2 := tn.is_prefix_of(k)) is not None:
                matched, rest = ispref2
                subtopics.append((k, matched, rest, source))

        if not subtopics:
            for k, source in self._mount_points.items():
                if source.established is None and (k.is_prefix_of(tn) is not None):
                    msg = f"This topic is on the mount point {k} but the connection is not established yet.\n"
                    raise KeyError(msg)

            msg = f"Cannot find a matching topic for {url0!r}.\n"
            msg += f"| topic name: {tn.as_dash_sep()}\n"
            msg += f"| sources: \n"
            for k, source in sources.items():
                msg += f"| {k.as_dash_sep()!r}: {type(source).__name__}\n"
            # self.logger.debug(msg)

            if self._mount_points:
                msg += f"| mount points: \n"
                for k, source in self._mount_points.items():
                    msg += (
                        f"| {k.as_dash_sep()!r}: {type(source).__name__} established = "
                        f"{source.established is not None}\n"
                    )
            else:
                msg += f"| no mount points\n"
            raise KeyError(msg)

        origin_node = self.node_id
        if tn in self._mount_points:
            if (established := self._mount_points[tn].established) is not None:
                origin_node = established.md.answering
                if origin_node is None:
                    origin_node = self.node_id
            # else:
            #     raise KeyError(f"Mount point {tn} is not established yet")

        unique_id = get_unique_id(origin_node, tn)
        subsources = {}
        for _, _, rest, source in subtopics:
            subsources[TopicNameV.from_components(rest)] = source

        sc = SourceComposition(
            topic_name=tn,
            sources=subsources,
            unique_id=unique_id,
            origin_node=origin_node,
        )

        if after is not None:
            return sc.get_inside_after(after)
        else:
            return sc

    def iterate_sources(self) -> Dict[TopicNameV, Source]:
        res: Dict[TopicNameV, Source] = {}
        for topic_name, x in self._forwarded.items():
            sb = ForwardedQueue(topic_name)
            res[topic_name] = sb
        for topic_name, x in self._oqs.items():
            sb = OurQueue(topic_name)
            res[topic_name] = sb

        ordered = sorted(res.items(), key=lambda y: len(y[0].components), reverse=True)
        return {k: v for k, v in ordered}

    @async_error_catcher
    async def serve_delete(self, request: web.Request) -> web.StreamResponse:
        headers: CIMultiDict[str] = CIMultiDict()
        self._add_own_headers(headers)
        add_nocache_headers(headers)

        topic_name_s = request.match_info["topic"]

        try:
            source = self.resolve(topic_name_s)
        except KeyError as e:
            msg = f"404: {request.url!r}\nCannot find topic '{topic_name_s}':\n{e.args[0]}"
            # logger.error()
            # text = f'404: Cannot find topic "{topic_name_s}"'
            return web.HTTPNotFound(text=msg, headers=headers)

        if isinstance(source, OurQueue):
            await self.remove_oq(source.topic_name)
            msg = f"{request.url!r}\nDeletd topic '{topic_name_s}'."
            return web.Response(text=msg, headers=headers, status=200)
        else:
            msg = f"{request.url!r}\nCannot delete topic '{topic_name_s}'."
            return web.HTTPServerError(text=msg, headers=headers)

    @async_error_catcher
    async def serve_get(self, request: web.Request) -> web.StreamResponse:
        with self._log_request(request):
            headers: CIMultiDict[str] = CIMultiDict()
            self._add_own_headers(headers)
            add_nocache_headers(headers)

            topic_name_s = request.match_info["topic"]

            if topic_name_s == "":
                return await self.serve_index(request)

            # self.logger.info(f"serve_get: {request.url!r} -> {topic_name_s!r}")

            try:
                source = self.resolve(topic_name_s)
            except KeyError as e:
                msg = f"404: {request.url!r}\nCannot find topic '{topic_name_s}':\n{e.args[0]}"
                # self.logger.error(msg)
                # text = f'404: Cannot find topic "{topic_name_s}"'
                return web.HTTPNotFound(text=msg, headers=headers)

            multidict_update(headers, self.get_headers_alternatives(request))
            properties = source.get_properties(self)
            # self.logger.info(f"serve_get: {request.url!r} -> {topic_name_s!r} -> {properties!r}")
            put_meta_headers(headers, properties)

            origin_node = await source.get_source_node_id(self)
            if origin_node is not None:
                headers.add(HEADER_DATA_ORIGIN_NODE_ID, origin_node)

            # logger.debug(f"serve_get: {request.url!r} -> {source!r}")

            if isinstance(source, ForwardedQueue):
                # Optimization: streaming
                return await self.serve_get_proxied(request, self._forwarded[source.topic_name])

            # if topic_name not in self._oqs:
            #     msg = f'Cannot find topic "{topic_name.as_dash_sep()}"'
            #     raise web.HTTPNotFound(text=msg, headers=headers)

            title = topic_name_s
            url = topic_name_s
            # logger.info(f"url: {topic_name_s!r} source: {source!r}")
            try:
                rs = await source.get_resolved_data(url, self)
            except KeyError as e:
                self.logger.error(f"serve_get: {request.url!r} -> {topic_name_s!r} -> {e}")
                raise web.HTTPNotFound(text=f"404\n{e}", headers=headers) from e

            rd: Union[RawData, NotAvailableYet]
            if isinstance(rs, RawData):
                rd = rs
            elif isinstance(rs, Native):
                # logger.info(f"Native: {rs}")
                rd = RawData.cbor_from_native_object(rs.ob)
            elif isinstance(rs, NotAvailableYet):
                rd = rs
            elif isinstance(rs, NotFound):
                raise NotImplementedError(f"Cannot handle {rs!r}")
            else:
                raise AssertionError

            # pprint(properties)
            return self.visualize_data(
                request,
                title,
                rd,
                headers,
                is_streamable=properties.streamable,
                is_pushable=properties.pushable,
            )

    def make_friendly_visualization(
        self,
        title: str,  # rd: RawData,
        initial_data_html: str,
        *,
        is_image_content: bool,
        content_type: str,
        pushable: bool,
        initial_push_value: str,
        initial_push_contenttype: str,
        streamable: bool,
    ) -> web.StreamResponse:
        headers: CIMultiDict[str] = CIMultiDict()

        # language=html
        html_index = f"""\
<html lang="en">
<head>
    <title>{title}</title>
    <link rel="stylesheet" href="/static/style.css">
    <script src="/static/send.js"></script>

    <script src="https://cdn.jsdelivr.net/npm/cbor-js@0.1.0/cbor.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/js-yaml/4.1.0/js-yaml.min.js"></script>

</head>
<body>
<h1>{title}</h1>

<p>This response coming to you in HTML format because you requested it in HTML format.</p>

<p>Content type: <code>{content_type}</code></p>

        """
        if is_image_content:
            # language=html
            html_index += f"""
                <img id="data_field_image" src="data:{content_type};base64,{initial_data_html}" alt="image"/>
            
            """
        else:
            # language=html
            html_index += f"""
                <pre id="data_field"><code>{initial_data_html}</code></pre>
            """
        if pushable:
            # language=html
            html_index += f"""
            <h3>Push to queue</h3>
            <textarea id="myTextAreaContentType">{initial_push_contenttype}</textarea>
            
            <textarea id="myTextArea">{initial_push_value}</textarea>
            <br/>
            <button id="myButton">push</button>
            """

        if streamable:
            # language=html
            html_index += """
            <p>Streaming is available for this topic.</p>
            <pre id="result"></pre>
            """
        return web.Response(body=html_index, content_type="text/html", headers=headers)

    def visualize_data(
        self,
        request: web.Request,
        title: str,
        rd: Union[RawData, NotAvailableYet],
        headers: CIMultiDict[str],
        *,
        is_streamable: bool,
        is_pushable: bool,
    ) -> web.StreamResponse:
        accept_headers = request.headers.get("accept", "")

        accepts_html = "text/html" in accept_headers
        if isinstance(rd, RawData):
            if (
                rd.content_type != "text/html"
                and accepts_html
                and (is_structure(rd.content_type) or is_image(rd.content_type))
            ):
                if is_structure(rd.content_type):
                    is_image_content = False
                    initial_data_html = rd.get_as_yaml()
                else:
                    # convert to base64
                    is_image_content = True
                    encoded: bytes = base64.b64encode(rd.content)

                    initial_data_html: str = encoded.decode("ascii")
                return self.make_friendly_visualization(
                    title,
                    initial_data_html,
                    streamable=is_streamable,
                    pushable=is_pushable,
                    initial_push_value=initial_data_html,
                    initial_push_contenttype=rd.content_type,
                    is_image_content=is_image_content,
                    content_type=rd.content_type,
                )
            else:
                return web.Response(body=rd.content, content_type=rd.content_type, headers=headers)
        elif isinstance(rd, NotAvailableYet):
            if accepts_html:
                # language=html
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
                return web.Response(body=html_index, content_type="text/html", status=200, headers=headers)
            else:
                body = "204 - No data yet."
                return web.Response(body=body, content_type="text/plain", status=204, headers=headers)

        else:
            raise AssertionError(f"Cannot handle {rd!r}")

    @async_error_catcher
    async def serve_get_proxied(self, request: web.Request, fd: ForwardedTopic) -> web.StreamResponse:
        async with self._client() as client:
            async with client.my_session(fd.forward_url_data) as (session, use_url):
                # Create the proxied request using the original request's headers
                async with session.get(use_url, headers=request.headers) as resp:
                    # Read the response's body

                    # Create a response with the proxied request's status and body,
                    # forwarding all the headers
                    headers: CIMultiDict[str] = CIMultiDict()
                    multidict_update(headers, resp.headers)
                    headers.popall(HEADER_NO_AVAIL, [])
                    headers.popall(HEADER_CONTENT_LOCATION, [])

                    headers.add(HEADER_DATA_ORIGIN_NODE_ID, fd.origin_node)

                    for r in fd.reachability:
                        if r.answering == self.node_id:
                            r.benchmark.fill_headers(headers)

                    # headers.add('X-DTPS-Forwarded-node', resp.headers.get(HEADER_NODE_ID, '???'))
                    multidict_update(headers, self.get_headers_alternatives(request))
                    self._add_own_headers(headers)

                    response = web.StreamResponse(status=resp.status, headers=headers)

                    await response.prepare(request)
                    async for chunk in resp.content.iter_any():
                        await response.write(chunk)

                    return response

    def _add_own_headers(self, headers: CIMultiDict[str]) -> None:
        # passed_already = headers.get(HEADER_NODE_PASSED_THROUGH, [])
        default: List[str] = []
        prevnodeids = headers.getall(HEADER_NODE_ID, default)
        if len(prevnodeids) > 1:
            raise ValueError(f"More than one {HEADER_NODE_ID} header found: {prevnodeids}")

        if prevnodeids:
            headers.add(HEADER_NODE_PASSED_THROUGH, prevnodeids[0])

        server_string = f"lib-dtps-http/Python/{__version__}"
        HEADER_SERVER = "Server"

        current_server_strings = headers.getall(HEADER_SERVER, default)
        # logger.info(f'current_server_strings: {current_server_strings} cur = {headers}')
        if HEADER_SERVER not in current_server_strings:
            headers.add(HEADER_SERVER, server_string)
        # leave our own
        headers.popall(HEADER_NODE_ID, None)
        headers[HEADER_NODE_ID] = self.node_id

    def _headers(self, request: web.Request) -> CIMultiDict[str]:
        headers: CIMultiDict[str] = CIMultiDict()
        add_nocache_headers(headers)
        self._add_own_headers(headers)
        multidict_update(headers, self.get_headers_alternatives(request))
        return headers

    def _resolve(self, request: web.Request) -> Source:
        """Raises HTTPNotFound"""
        topic_name_s: str = request.match_info["topic"]
        # topic_name = TopicNameV.from_relative_url(topic_name_s)
        try:
            return self.resolve(topic_name_s)
        except KeyError as e:
            # self.logger.error(f"serve_get: {request.url!r} -> {topic_name_s!r} -> {e}")
            raise web.HTTPNotFound(text=f"404\n{e}", headers=self._headers(request)) from e

    @async_error_catcher
    async def serve_post(self, request: web.Request) -> web.Response:
        with self._log_request(request):
            content_type = request.headers.get("Content-Type", "application/octet-stream")
            data = await request.read()
            rd = RawData(content=data, content_type=ContentType(content_type))

            source: Source = self._resolve(request)

            headers = self._headers(request)

            presented_as = request.url.path
            pr: PostResult = await source.publish(presented_as, self, rd)

            if isinstance(pr, TransformError):
                return web.Response(status=pr.http_code, text=pr.message, headers=headers)
            elif isinstance(pr, DataReady):
                data = get_simple_cbor(pr.as_data_saved())
                for r in pr.availability:
                    headers.add("Location", r.url)

                return web.Response(
                    status=201,
                    content_type=CONTENT_TYPE_DTPS_DATAREADY_CBOR,
                    body=data,
                    headers=headers,  # content_type=otr.content_type, body=otr.content
                )
            else:
                raise AssertionError(f"Cannot handle {pr!r} for {source}")

    @contextmanager
    def _log_request(self, request: web.Request) -> Iterator[None]:
        self.logger.debug(f"{request.method} {request.url}")
        try:
            yield
        except Exception:
            # logger.error(f"{request.method} {request.url}: response {e}")
            raise

    @async_error_catcher
    async def serve_patch(self, request: web.Request) -> web.Response:
        with self._log_request(request):
            presented_as = request.url.path

            headers = self._headers(request)

            topic_name_s: str = request.match_info["topic"]

            try:
                source = self.resolve(topic_name_s)
            except KeyError as e:
                # self.logger.error(f"serve_get: {request.url!r} -> {topic_name_s!r} -> {e}")
                raise web.HTTPNotFound(text=f"404\n{e.args[0]}", headers=headers) from e

            topic_name = TopicNameV.from_relative_url(topic_name_s)
            if topic_name.is_root():
                return await self.serve_patch_root(request)
            # elif topic_name == TOPIC_PROXIED:
            #     return await self.serve_patch_proxied(request)

            data = await request.read()

            content_type = request.headers.get("Content-Type", "application/json")
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
                msg = "Unsupported content type for patch: {content_type}. I can do json and cbor"
                return web.Response(status=415, text=msg)

            otr = await source.patch(presented_as, self, patch)

            if isinstance(otr, TransformError):
                return web.Response(status=otr.http_code, text=otr.message, headers=headers)
            elif isinstance(otr, DataReady):
                data = get_simple_cbor(otr.as_data_saved())
                for r in otr.availability:
                    headers.add("Location", r.url)

                return web.Response(
                    status=201,
                    content_type=CONTENT_TYPE_DTPS_DATAREADY_CBOR,
                    body=data,
                    headers=headers,  # content_type=otr.content_type, body=otr.content
                )
            else:
                raise AssertionError(f"Cannot handle {otr!r}")

    @async_error_catcher
    async def serve_patch_root(self, request: web.Request) -> web.Response:
        with self._log_request(request):
            data = await request.read()

            content_type = request.headers.get("Content-Type", "application/json")
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
                msg = "Unsupported content type for patch: {content_type}. I can do json and cbor"
                return web.Response(status=415, text=msg)
            # logger.info(f"decoded: {decoded} patch: {patch}")

            for operation in patch._ops:
                if isinstance(operation, RemoveOperation):
                    raise NotImplementedError(
                        f"Cannot handle {operation!r}"
                    )  # TODO: remove topics not supported
                elif isinstance(operation, AddOperation):
                    # logger.info(f"op: {operation.__dict__}, {operation.pointer.parts}")
                    topic = topic_name_from_json_pointer(operation.location)

                    if topic.is_root():
                        raise ValueError(f"Cannot create root topic (path = {operation.path!r})")

                    value = operation.operation["value"]
                    trf = TopicRefAdd.from_json(value)
                    await self.create_oq(topic, trf.content_info, trf.properties)
                    self.logger.info(f"created new topic: '{topic.as_dash_sep()}'")

                elif isinstance(operation, (ReplaceOperation, MoveOperation, TestOperation, CopyOperation)):
                    return web.Response(status=405)
                else:
                    raise NotImplementedError(f"Cannot handle {operation!r}")

            headers = self._headers(request)
            return web.Response(status=200, headers=headers)

    @async_error_catcher
    async def serve_blob(self, request: web.Request) -> web.Response:
        headers: CIMultiDict[str] = CIMultiDict()
        multidict_update(headers, self.get_headers_alternatives(request))
        self._add_own_headers(headers)

        digest = request.match_info["digest"]
        content_type_base64 = request.match_info["content_type_base64"]
        content_type = base64.urlsafe_b64decode(content_type_base64.encode()).decode("ascii")

        if digest in self.blob_manager.blobs:
            blob = self.blob_manager.blobs[digest]
            return web.Response(body=blob.content, headers=headers, content_type=content_type)
        else:
            msg = f"Cannot resolve blob: {request.url}\ndigest: {digest!r}"
            self.logger.error(msg)
            raise web.HTTPNotFound(text=msg, headers=headers)

    # routes.get("/{ignore:.*}/:blobs/{digest}/{content_type}")(self.serve_blob)

    def _store_data(self, rd: RawData, available_for: float, presented_as: str) -> Tuple[URLString, float]:
        now = time.time()
        deadline = now + available_for
        digest = self.blob_manager.save_blob_deadline(rd.content, deadline)

        b64 = base64.urlsafe_b64encode(rd.content_type.encode()).decode("ascii")

        url = URLString(f"./:blobs/{digest}/{b64}")
        return url, now + available_for

    @async_error_catcher
    async def serve_data_get(self, request: web.Request) -> web.Response:
        headers: CIMultiDict[str] = CIMultiDict()
        multidict_update(headers, self.get_headers_alternatives(request))
        self._add_own_headers(headers)
        if "topic" not in request.match_info:
            topic_name_s = ""
        else:
            topic_name_s = request.match_info["topic"] + "/"
        topic_name = TopicNameV.from_relative_url(topic_name_s)
        digest = request.match_info["digest"]
        if topic_name not in self._oqs:
            msg = f"Cannot resolve topic: {request.url}\ntopic: {topic_name_s!r}"
            self.logger.error(msg)
            raise web.HTTPNotFound(text=msg, headers=headers)
        oq = self._oqs[topic_name]
        data = oq.get(digest)
        headers[HEADER_DATA_UNIQUE_ID] = oq.tr.unique_id
        headers[HEADER_DATA_ORIGIN_NODE_ID] = oq.tr.origin_node

        return web.Response(body=data.content, headers=headers, content_type=data.content_type)

    @async_error_catcher
    async def serve_events(self, request: web.Request) -> web.WebSocketResponse:
        presented_as = request.url.path

        if SEND_DATA_ARGNAME in request.query:
            send_data = True
        else:
            send_data = False

        topic_name_s = request.match_info["topic"]

        # logger.info(f"serve_events: {request} topic_name={topic_name_s} send_data={send_data}")
        topic_name = TopicNameV.from_relative_url(topic_name_s)
        if topic_name not in self._oqs and topic_name not in self._forwarded:
            headers: CIMultiDict[str] = CIMultiDict()

            self._add_own_headers(headers)
            msg = f"Cannot resolve topic: {request.url}\ntopic: {topic_name_s!r}"
            raise web.HTTPNotFound(text=msg, headers=headers)

        ws = web.WebSocketResponse()
        multidict_update(ws.headers, self.get_headers_alternatives(request))
        self._add_own_headers(ws.headers)

        if topic_name in self._forwarded:
            fd = self._forwarded[topic_name]

            for r in fd.reachability:
                if r.answering == self.node_id:
                    r.benchmark.fill_headers(ws.headers)

            if fd.forward_url_events is None:
                msg = f"Forwarding for topic {topic_name!r} is not enabled"
                raise web.HTTPBadRequest(reason=msg)

            await ws.prepare(request)

            # self.logger.info(f"serve_events_forwarder: {request.url} topic_name={topic_name_s} fd={fd}")
            await self.serve_events_forwarder(ws, presented_as, fd, send_data)
            return ws

        oq_ = self._oqs[topic_name]
        ws.headers[HEADER_DATA_UNIQUE_ID] = oq_.tr.unique_id
        ws.headers[HEADER_DATA_ORIGIN_NODE_ID] = oq_.tr.origin_node
        await ws.prepare(request)
        self.logger.debug(f"serve_events: {request.url} topic_name={topic_name_s} send_data={send_data}")

        exit_event = asyncio.Event()
        ci = oq_.get_channel_info()

        self.logger.debug(f"serve_events: {request.url} sending {ci}")
        await ws.send_bytes(get_tagged_cbor(ci))

        @async_error_catcher
        async def send_message(_: ObjectQueue, i: int) -> None:
            self.logger.debug(f"serve_events: new message {i}")
            mdata = oq_.saved[i]
            digest = mdata.digest

            presented_as = request.url.path
            data = oq_.get_data_ready(mdata, presented_as, inline_data=send_data)

            if ws.closed:
                exit_event.set()
                return

            try:
                await ws.send_bytes(get_tagged_cbor(data))
            except ConnectionResetError:
                exit_event.set()
                pass

            if send_data:
                the_bytes = oq_.get(digest).content
                chunk = Chunk(digest=digest, i=0, n=1, index=0, data=the_bytes)

                try:
                    await ws.send_bytes(get_tagged_cbor(chunk))
                except ConnectionResetError:
                    exit_event.set()
                    pass

        s = oq_.subscribe(send_message)

        if oq_.stored:
            last = oq_.last()

            await send_message(oq_, last.index)

        try:
            await exit_event.wait()
            await ws.close()
        finally:
            await oq_.unsubscribe(s)

        return ws

    @async_error_catcher
    async def serve_push_stream(self, request: web.Request) -> web.WebSocketResponse:
        headers: CIMultiDict[str] = CIMultiDict()
        self._add_own_headers(headers)

        topic_name_s = request.match_info["topic"]
        try:
            source = self.resolve(topic_name_s)
        except KeyError as e:
            raise web.HTTPNotFound(text=f"{e.args[0]}") from e

        if isinstance(source, OurQueue):
            return await self.serve_push_stream_oq(request, source)
        else:
            raise web.HTTPBadRequest(text=f"Topic {topic_name_s} is not a queue")

    async def serve_push_stream_oq(self, request: web.Request, oq: OurQueue) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        multidict_update(ws.headers, self.get_headers_alternatives(request))
        self._add_own_headers(ws.headers)

        await ws.prepare(request)

        oq_ = self._oqs[oq.topic_name]

        while True:  # TODO: respect on_shutdown
            msg = await ws.receive()
            # self.logger.debug(f"serve_push_stream_oq: received {msg}")

            if msg.type == WSMsgType.CLOSE:
                break

            elif msg.type == WSMsgType.BINARY:
                # read cbor
                try:
                    data = cbor2.loads(msg.data)
                except:
                    msg = f"Cannot decode {msg.data!r}"
                    self.logger.error(msg)
                    result = PushResult(False, msg)
                    await ws.send_bytes(get_tagged_cbor(result))
                    continue
                # logger.info(f"received: {data}")
                # interpret as RawData
                if not isinstance(data, dict):
                    msg = f"Cannot handle {data!r}"
                    self.logger.error(msg)
                    result = PushResult(False, msg)
                    await ws.send_bytes(get_tagged_cbor(result))
                    continue

                if RawData.__name__ in data:
                    inside = data[RawData.__name__]
                    rd = RawData(inside["content"], inside["content_type"])
                    await oq_.publish(rd)

                    result = PushResult(True, "")
                    await ws.send_bytes(get_tagged_cbor(result))

                else:
                    msg = f"Cannot handle {data!r}"
                    self.logger.error(msg)
                    result = PushResult(False, msg)
                    await ws.send_bytes(get_tagged_cbor(result))

                    continue

            else:
                msg = f"Cannot handle message type {msg!r}"
                self.logger.error(msg)
                result = PushResult(False, msg)
                await ws.send_bytes(get_tagged_cbor(result))

        await ws.close()

        return ws

    @async_error_catcher
    async def serve_events_forwarder(
        self,
        ws: web.WebSocketResponse,
        presented_as: str,
        fd: ForwardedTopic,
        inline_data: bool,
    ) -> None:
        # assert fd.forward_url_events is not None
        while not self.shutdown_event.is_set():
            if ws.closed:
                break
            try:
                if inline_data:
                    if (url := fd.forward_url_events_inline_data) is not None:
                        await self.serve_events_forward_simple(ws, url)
                    elif (url := fd.forward_url_events) is not None:
                        await self.serve_events_forwarder_one(
                            ws, presented_as, url, inline_data_send=inline_data, inline_data_receive=False
                        )
                    else:
                        raise ValueError(f"Events not supported")

                        # await self.serve_events_forwarder_one(ws,  True)
                else:
                    if (url := fd.forward_url_events_inline_data) is not None:
                        inline_data_receive = True
                    elif (url := fd.forward_url_events) is not None:
                        inline_data_receive = False
                    else:
                        raise ValueError(f"Events not supported")

                    await self.serve_events_forwarder_one(
                        ws,
                        presented_as,
                        url,
                        inline_data_send=inline_data,
                        inline_data_receive=inline_data_receive,
                    )

            except CancelledError:
                raise
            except:
                self.logger.error(f"Exception in serve_events_forwarder_one: {traceback.format_exc()}")
                await asyncio.sleep(1)

    if TYPE_CHECKING:

        def _client(self, nickname: Optional[str] = None) -> AsyncContextManager[DTPSClient]:
            ...

    else:

        @asynccontextmanager
        async def _client(self, nickname: Optional[str] = None) -> AsyncIterator[DTPSClient]:
            async with DTPSClient.create(nickname=nickname, shutdown_event=self.shutdown_event) as client:
                yield client

    async def serve_events_forward_simple(self, ws_to_write: web.WebSocketResponse, url: URL) -> None:
        """Iterates using direct data in websocket."""
        self.logger.debug(f"serve_events_forward_simple: {url} [no overhead forwarding]")

        async with self._client() as client:
            async with client.my_session(url) as (session, use_url):
                async with session.ws_connect(use_url) as ws:
                    # logger.debug(f"websocket to {use_url} ready")
                    async for msg in ws:
                        if msg.type == WSMsgType.CLOSE:
                            break
                        if msg.type == WSMsgType.TEXT:
                            # self.logger.warning(f"serve_events_forward_simple: forwarding text {msg}")
                            await ws_to_write.send_str(msg.data)  # OK
                        elif msg.type == WSMsgType.BINARY:
                            await ws_to_write.send_bytes(msg.data)  # OK
                        else:
                            self.logger.warning(f"Unknown message type {msg.type}")

    @async_error_catcher
    async def serve_events_forwarder_one(
        self,
        ws: web.WebSocketResponse,
        presented_as: str,
        url: URLWS,
        inline_data_receive: bool,
        inline_data_send: bool,
    ) -> None:
        available_for = 60.0
        assert isinstance(url, URL)
        self.logger.debug(f"serve_events_forwarder_one: {url} {inline_data_receive=} {inline_data_send=}")

        async with self._client() as client:

            @async_error_catcher
            async def callback(lue: ListenURLEvents) -> None:
                async def send(m: ChannelMsgs):
                    await ws.send_bytes(get_tagged_cbor(m))

                if isinstance(lue, InsertNotification):
                    ds = lue.data_saved
                    if inline_data_send:
                        availability = []
                        chunks_arriving = 1
                    else:
                        the_url, available_until = self._store_data(lue.raw_data, available_for, presented_as)
                        self.logger.debug(
                            f"serve_events_forwarder_one: sending ref {the_url} {available_until}"
                        )
                        availability = [ResourceAvailability(the_url, available_until)]
                        chunks_arriving = 0
                    dr2 = DataReady(
                        index=ds.index,
                        time_inserted=ds.time_inserted,
                        digest=ds.digest,
                        content_type=ds.content_type,
                        content_length=ds.content_length,
                        availability=availability,
                        chunks_arriving=chunks_arriving,
                        clocks=ds.clocks,
                        origin_node=ds.origin_node,
                        unique_id=ds.unique_id,
                    )
                    # logger.debug(f"Forwarding {dr} -> {dr2}")
                    await send(dr2)
                    if inline_data_send:
                        # TODO: divide chunks
                        chunk = Chunk(digest=dr2.digest, i=0, n=1, index=0, data=lue.raw_data.content)
                        await send(chunk)
                    else:
                        pass
                elif isinstance(lue, ConnectionEstablished):
                    await send(SilenceMsg(0.0, f"Connection established to {url}"))

                elif isinstance(
                    lue,
                    (
                        WarningMsg,
                        ErrorMsg,
                        FinishedMsg,
                        SilenceMsg,
                    ),
                ):
                    await send(lue)
                else:
                    self.logger.warning(f"Unknown message type {lue}")
                    raise NotImplementedError(f"Cannot handle {lue!r}")

            ld = await client.listen_url_events3(
                url,
                inline_data=inline_data_receive,
                raise_on_error=False,
                add_silence=None,
                callback=callback,
            )

            await ld.wait_for_done_or_stop_on_event(self.shutdown_event)


def add_nocache_headers(h: CIMultiDict[str]) -> None:
    h.update(HEADER_NO_CACHE)
    h["Cookie"] = f"help-no-cache={time.monotonic_ns()}"


def get_unique_id(node_id: NodeID, topic_name: TopicNameV) -> SourceID:
    if topic_name.is_root():
        return cast(SourceID, node_id)
    return cast(SourceID, f"{node_id}:{topic_name.as_relative_url()}")


def put_meta_headers(h: CIMultiDict[str], tp: TopicProperties) -> None:
    if tp.streamable:
        put_link_header(h, f"{EVENTS_SUFFIX}/", REL_EVENTS_NODATA, "websocket")
        put_link_header(h, f"{EVENTS_SUFFIX}/?send_data=1", REL_EVENTS_DATA, "websocket")

    if tp.pushable:
        put_link_header(h, f"{REL_STREAM_PUSH_SUFFIX}/", REL_STREAM_PUSH, "websocket")
    put_link_header(h, f"{REL_URL_META}/", REL_META, CONTENT_TYPE_DTPS_INDEX_CBOR)

    if tp.has_history:
        put_link_header(h, f"{REL_URL_HISTORY}/", REL_HISTORY, CONTENT_TYPE_TOPIC_HISTORY_CBOR)


#


@async_error_catcher
async def update_clock(s: DTPSServer, topic_name: TopicNameV, interval: float, initial_delay: float) -> None:
    await asyncio.sleep(initial_delay)
    s.logger.info(f"Starting clock {topic_name.as_relative_url()} with interval {interval}")
    oq = s.get_oq(topic_name)
    while True:
        t = time.time_ns()
        data = str(t).encode()
        await oq.publish(RawData(content=data, content_type=MIME_JSON))
        try:
            await asyncio.sleep(interval)
        except CancelledError:
            s.logger.info(f"Clock {topic_name.as_relative_url()} cancelled")
            raise  #


def get_simple_cbor(ob: Any) -> bytes:
    return cbor2.dumps(asdict(ob))


def get_tagged_cbor(ob: Any) -> bytes:
    data = {ob.__class__.__name__: asdict(ob)}
    return cbor2.dumps(data)


def removeprefix(s: str, prefix: str) -> str:
    if s.startswith(prefix):
        return s[len(prefix) :]
    else:
        return s[:]


def topic_name_from_json_pointer(path: str) -> TopicNameV:
    path = unescape_json_pointer(path)

    components = []
    for p in path.split("/"):
        if not p:
            continue
        components.append(p)

    return TopicNameV.from_components(components)
