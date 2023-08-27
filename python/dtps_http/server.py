import asyncio
import json
import time
import traceback
import uuid
from dataclasses import asdict, field, replace
from typing import Any, Awaitable, Callable, cast, Optional, Sequence

import cbor2
import yaml
from aiohttp import web, WSMsgType
from aiopubsub import Hub, Key, Publisher, Subscriber
from multidict import CIMultiDict
from pydantic.dataclasses import dataclass

from . import __version__, logger
from .constants import (
    CONTENT_TYPE_DTPS_INDEX_CBOR,
    CONTENT_TYPE_TOPIC_DIRECTORY,
    CONTENT_TYPE_TOPIC_HISTORY_CBOR,
    EVENTS_SUFFIX,
    HEADER_CONTENT_LOCATION,
    HEADER_DATA_ORIGIN_NODE_ID,
    HEADER_DATA_UNIQUE_ID,
    HEADER_NO_AVAIL,
    HEADER_NO_CACHE,
    HEADER_NODE_ID,
    HEADER_NODE_PASSED_THROUGH,
    REL_EVENTS_DATA,
    REL_EVENTS_NODATA,
    REL_HISTORY,
    REL_META,
    REL_URL_HISTORY,
    REL_URL_META,
    TOPIC_AVAILABILITY,
    TOPIC_CLOCK,
    TOPIC_LIST,
    TOPIC_LOGS,
    TOPIC_STATE_NOTIFICATION,
    TOPIC_STATE_SUMMARY,
)
from .structures import (
    ChannelInfo,
    ChannelInfoDesc,
    Chunk,
    Clocks,
    ContentInfo,
    DataReady,
    History,
    is_structure,
    LinkBenchmark,
    MinMax,
    RawData,
    ResourceAvailability,
    TopicProperties,
    TopicReachability,
    TopicRef,
    TopicsIndex,
)
from .types import NodeID, SourceID, TopicNameV, URLString
from .types_of_source import (
    ForwardedQueue,
    Native,
    NotAvailableYet,
    NotFound,
    OurQueue,
    Source,
    SourceComposition,
)
from .urls import get_relative_url, join, parse_url_unescape, URL, url_to_string
from .utils import async_error_catcher, multidict_update

SEND_DATA_ARGNAME = "send_data"
ROOT = TopicNameV.root()
__all__ = [
    "DTPSServer",
    "ForwardedTopic",
    "get_link_headers",
]


@dataclass
class DataSaved:
    index: int
    time_inserted: int
    digest: str
    content_type: str
    content_length: int
    clocks: Clocks


SUB_ID = str
K_INDEX = "index"


class ObjectQueue:
    sequence: list[DataSaved]
    _data: dict[str, RawData]
    _seq: int
    _name: TopicNameV
    _hub: Hub
    _pub: Publisher
    _sub: Subscriber
    tr: TopicRef
    max_history: Optional[int]

    def __init__(self, hub: Hub, name: TopicNameV, tr: TopicRef, max_history: Optional[int]):
        self._hub = hub
        self._pub = Publisher(self._hub, Key())
        self._sub = Subscriber(self._hub, name.as_relative_url())
        self._seq = 0
        self.sequence = []
        self._data = {}
        self._name = name
        self.tr = tr
        self.max_history = max_history

    def get_channel_info(self) -> ChannelInfo:
        if not self.sequence:
            newest = None
            oldest = None
        else:
            newest = ChannelInfoDesc(self.sequence[-1].index, self.sequence[-1].time_inserted)
            oldest = ChannelInfoDesc(self.sequence[0].index, self.sequence[0].time_inserted)

        ci = ChannelInfo(queue_created=self.tr.created, num_total=self._seq, newest=newest, oldest=oldest)
        return ci

    def publish_text(self, text: str) -> None:
        data = text.encode("utf-8")
        content_type = "text/plain"
        self.publish(RawData(data, content_type))

    def publish_cbor(self, obj: object, content_type: str = "application/cbor") -> None:
        """Publish a python object as a cbor2 encoded object."""
        data = cbor2.dumps(obj)
        self.publish(RawData(data, content_type))

    def publish_json(self, obj: object) -> None:
        """Publish a python object as a cbor2 encoded object."""
        data = json.dumps(obj)
        content_type = "application/json"
        self.publish(RawData(data.encode(), content_type))

    def publish(self, obj: RawData) -> None:
        use_seq = self._seq
        self._seq += 1
        digest = obj.digest()
        clocks = self.current_clocks()
        ds = DataSaved(use_seq, time.time_ns(), digest, obj.content_type, len(obj.content), clocks=clocks)
        self._data[digest] = obj
        self.sequence.append(ds)
        if self.max_history:
            if len(self.sequence) > self.max_history:
                self.sequence.pop(0)
        self._pub.publish(
            Key(self._name.as_relative_url(), K_INDEX), use_seq
        )  # logger.debug(f"published #{self._seq} {self._name}: {obj!r}")

    def current_clocks(self) -> Clocks:
        clocks = Clocks.empty()
        if self._seq > 0:
            based_on = self._seq - 1
            clocks.logical[self.tr.unique_id] = MinMax(based_on, based_on)
        now = time.time_ns()
        clocks.wall[self.tr.unique_id] = MinMax(now, now)
        return clocks

    def last(self) -> DataSaved:
        if self.sequence:
            ds = self.sequence[-1]
            return ds
        else:
            raise KeyError("No data in queue")

    def last_data(self) -> RawData:
        return self.get(self.last().digest)

    def get(self, digest: str) -> RawData:
        return self._data[digest]

    def subscribe(self, callback: "Callable[[ObjectQueue, int], Awaitable[None]]") -> SUB_ID:
        wrap_callback = lambda key, msg: callback(self, msg)
        self._sub.add_async_listener(Key(self._name.as_relative_url(), K_INDEX), wrap_callback)
        # last_used = list(self._sub._listeners)[-1]
        return ""  # TODO

    def unsubscribe(self, sub_id: SUB_ID) -> None:
        pass  # TODO

    def get_data_ready(self, ds: DataSaved, presented_as: str, inline_data: bool) -> DataReady:
        actual_url = self._name.as_relative_url() + "data/" + ds.digest + "/"
        rel_url = get_relative_url(actual_url, presented_as)
        if inline_data:
            nchunks = 1
            availability_ = []
        else:
            nchunks = 0
            availability_ = [ResourceAvailability(url=rel_url, available_until=time.time() + 60)]

        data = DataReady(
            sequence=ds.index,
            time_inserted=ds.time_inserted,
            digest=ds.digest,
            content_type=ds.content_type,
            content_length=ds.content_length,
            availability=availability_,
            chunks_arriving=nchunks,
            clocks=ds.clocks,
            unique_id=self.tr.unique_id,
            origin_node=self.tr.origin_node,
        )
        return data


@dataclass
class ForwardedTopic:
    unique_id: SourceID  # unique id for the stream
    origin_node: NodeID  # unique id of the node that created the stream
    app_data: dict[str, bytes]
    forward_url_data: URL
    forward_url_events: Optional[URL]
    forward_url_events_inline_data: Optional[URL]
    reachability: list[TopicReachability]
    properties: TopicProperties
    content_info: ContentInfo


class DTPSServer:
    node_id: NodeID

    _oqs: dict[TopicNameV, ObjectQueue]
    _forwarded: dict[TopicNameV, ForwardedTopic]

    tasks: "list[asyncio.Task[Any]]"
    digest_to_urls: dict[str, list[URL]]
    node_app_data: dict[str, bytes]
    topic_prefix: TopicNameV

    def __init__(
        self,
        *,
        topics_prefix: TopicNameV,
        on_startup: "Sequence[Callable[[DTPSServer], Awaitable[None]]]" = (),
    ) -> None:
        self.app = web.Application()

        self.node_app_data = {}
        self.node_started = time.time_ns()

        self.topics_prefix = topics_prefix
        routes = web.RouteTableDef()
        self._more_on_startup = on_startup
        self.app.on_startup.append(self.on_startup)
        self.app.on_shutdown.append(self.on_shutdown)
        # routes.get("/")(self.serve_index)

        routes.get("/{topic:.*}" + EVENTS_SUFFIX + "/")(self.serve_events)
        routes.get("/{topic:.*}" + REL_URL_META + "/")(self.serve_meta)
        routes.get("/{topic:.*}" + REL_URL_HISTORY + "/")(self.serve_history)

        routes.get("/{topic:.*}/data/{digest}/")(self.serve_data_get)
        routes.post("/{topic:.*}")(self.serve_post)
        routes.get("/{topic:.*}")(self.serve_get)
        self.app.add_routes(routes)

        self.hub = Hub()
        self._oqs = {}
        self._forwarded = {}
        self.tasks = []
        self.logger = logger
        self.available_urls = []
        self.node_id = NodeID(str(uuid.uuid4()))

        self.digest_to_urls = {}

        content_info = ContentInfo(
            accept_content_type=[CONTENT_TYPE_TOPIC_DIRECTORY],
            storage_content_type=[CONTENT_TYPE_TOPIC_DIRECTORY],
            produces_content_type=[CONTENT_TYPE_TOPIC_DIRECTORY],
            jschema=None,
            examples=[],
        )

        tr = TopicRef(
            get_unique_id(self.node_id, ROOT),
            self.node_id,
            {},
            [],
            content_info=content_info,
            properties=TopicProperties.streamable_readonly(),
            created=time.time_ns(),
        )
        self._oqs[ROOT] = ObjectQueue(self.hub, ROOT, tr, max_history=5)
        index = self.create_root_index()
        wire = index.to_wire()
        as_cbor = cbor2.dumps(asdict(wire))
        self._oqs[ROOT].publish(RawData(as_cbor, CONTENT_TYPE_DTPS_INDEX_CBOR))

    def has_forwarded(self, topic_name: TopicNameV) -> bool:
        return topic_name in self._forwarded

    def get_headers_alternatives(self, request: web.Request) -> CIMultiDict[str]:
        original_url = request.url

        # noinspection PyProtectedMember
        sock = request.transport._sock  # type: ignore
        sockname = sock.getsockname()
        if isinstance(sockname, str):
            path = sockname.replace("/", "%2F")
            use_url = str(original_url).replace("http://", "http+unix://").replace("localhost", path)
        else:
            use_url = str(original_url)

        res: CIMultiDict[str] = CIMultiDict()
        if not self.available_urls:
            res[HEADER_NO_AVAIL] = "No alternative URLs available"
            return res

        alternatives: list[str] = []
        url = str(use_url)

        for a in self.available_urls + [
            f"http://127.0.0.1:{original_url.port}/",
            f"http://localhost:{original_url.port}/",
        ]:
            if url.startswith(a):
                for b in self.available_urls:
                    if a == b:
                        continue
                    alternative = b + url.removeprefix(a)
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
        # list_alternatives = "".join(f"\n  {_}" for _ in alternatives)
        # logger.debug(f"get_headers_alternatives: {request.url} -> \n effective: {use_url} \n {
        # list_alternatives}")
        return res

    def set_available_urls(self, urls: list[str]) -> None:
        self.available_urls = urls

    def add_available_url(self, url: str) -> None:
        self.available_urls.append(url)
        oq = self.get_oq(TOPIC_AVAILABILITY)
        oq.publish_json(self.available_urls)

    def remember_task(self, task: "asyncio.Task[Any]") -> None:
        """Add a task to the list of tasks to be cancelled on shutdown"""
        self.tasks.append(task)

    def _update_lists(self):
        topics = []
        topics.extend(self._oqs.keys())
        topics.extend(self._forwarded.keys())
        self._oqs[TOPIC_LIST].publish_json(sorted([_.as_relative_url() for _ in topics]))

        index = self.create_root_index()
        index_wire = index.to_wire()
        self._oqs[ROOT].publish_cbor(asdict(index_wire), CONTENT_TYPE_DTPS_INDEX_CBOR)

    async def remove_oq(self, name: TopicNameV) -> None:
        if name in self._oqs:
            self._oqs.pop(name)
            self._update_lists()

    async def remove_forward(self, name: TopicNameV) -> None:
        if name in self._forwarded:
            self._forwarded.pop(name)
            self._update_lists()

    async def add_forwarded(self, name: TopicNameV, forwarded: ForwardedTopic) -> None:
        if name in self._forwarded or name in self._oqs:
            raise ValueError(f"Topic {name} already exists")
        self._forwarded[name] = forwarded

    def get_oq(self, name: TopicNameV) -> ObjectQueue:
        if name in self._forwarded:
            raise ValueError(f"Topic {name} is a forwarded one")

        return self._oqs[name]

    async def create_oq(
        self, name: TopicNameV, tp: Optional[TopicProperties] = None, max_history: Optional[int] = None
    ) -> ObjectQueue:
        if name in self._forwarded:
            raise ValueError(f"Topic {name} is a forwarded one")
        if name in self._oqs:
            raise ValueError(f"Topic {name} is a forwarded one")

        unique_id = get_unique_id(self.node_id, name)

        treach = TopicReachability(
            URLString(name.as_relative_url()),
            self.node_id,
            forwarders=[],
            benchmark=LinkBenchmark.identity(),
        )
        reachability: list[TopicReachability] = [treach]
        if tp is None:
            tp = TopicProperties.streamable_readonly()

        tr = TopicRef(
            unique_id=unique_id,
            origin_node=self.node_id,
            app_data={},
            reachability=reachability,
            created=time.time_ns(),
            properties=tp,
            content_info=ContentInfo.simple(CONTENT_TYPE_TOPIC_DIRECTORY),
        )

        self._oqs[name] = ObjectQueue(self.hub, name, tr, max_history=max_history)
        self._update_lists()
        return self._oqs[name]

    @async_error_catcher
    async def on_startup(self, _: web.Application) -> None:
        # self.logger.info("on_startup")

        content_info = ContentInfo(
            accept_content_type=[CONTENT_TYPE_TOPIC_DIRECTORY],
            storage_content_type=[CONTENT_TYPE_TOPIC_DIRECTORY],
            produces_content_type=[CONTENT_TYPE_TOPIC_DIRECTORY],
            jschema=None,
            examples=[],
        )
        tr = TopicRef(
            get_unique_id(self.node_id, TOPIC_LIST),
            self.node_id,
            {},
            [],
            content_info=content_info,
            properties=TopicProperties.streamable_readonly(),
            created=time.time_ns(),
        )
        self._oqs[TOPIC_LIST] = ObjectQueue(self.hub, TOPIC_LIST, tr, max_history=100)

        await self.create_oq(TOPIC_LOGS, max_history=100)
        await self.create_oq(TOPIC_CLOCK, max_history=10)
        await self.create_oq(TOPIC_AVAILABILITY, max_history=10)
        await self.create_oq(TOPIC_STATE_SUMMARY, max_history=10)
        await self.create_oq(TOPIC_STATE_NOTIFICATION, max_history=10)

        self.remember_task(asyncio.create_task(update_clock(self, TOPIC_CLOCK, 1.0, 0.0)))

        for f in self._more_on_startup:
            await f(self)

    @async_error_catcher
    async def on_shutdown(self, _: web.Application) -> None:
        self.logger.info("on_shutdown")
        for t in self.tasks:
            t.cancel()

    def create_root_index(self) -> TopicsIndex:
        topics: dict[TopicNameV, TopicRef] = {}
        for topic_name, oqs in self._oqs.items():
            qual_topic_name = self.topics_prefix + topic_name
            reach = TopicReachability(
                URLString(qual_topic_name.as_relative_url()),
                self.node_id,
                forwarders=[],
                benchmark=LinkBenchmark.identity(),
            )
            topic_ref = replace(oqs.tr, reachability=[reach])
            topics[qual_topic_name] = topic_ref

        for topic_name, fd in self._forwarded.items():
            qual_topic_name = self.topics_prefix + topic_name
            #
            # reach = TopicReachability(URLString(qual_topic_name.as_relative_url()), self.node_id,
            #                           self.node_id, forwarders=[], benchmark=LinkBenchmark.identity(), )

            tr = TopicRef(
                fd.unique_id,
                fd.origin_node,
                {},
                fd.reachability,
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
                            URLString(x.as_relative_url()),
                            self.node_id,
                            forwarders=[],
                            benchmark=LinkBenchmark.identity(),
                        ),
                    ]
                    topics[x] = TopicRef(
                        get_unique_id(self.node_id, x),
                        self.node_id,
                        {},
                        reachability,
                        properties=TopicProperties.streamable_readonly(),
                        created=time.time_ns(),
                        content_info=ContentInfo.simple(CONTENT_TYPE_DTPS_INDEX_CBOR),
                    )

        index_internal = TopicsIndex(topics=topics)
        return index_internal

    @async_error_catcher
    async def serve_index(self, request: web.Request) -> web.Response:
        headers_s = "".join(f"{k}: {v}\n" for k, v in request.headers.items())
        self.logger.debug(f"serve_index: {request.url} \n {headers_s}")

        index_internal = self.create_root_index()
        index_wire = index_internal.to_wire()

        headers: CIMultiDict[str] = CIMultiDict()

        add_nocache_headers(headers)
        multidict_update(headers, self.get_headers_alternatives(request))
        self._add_own_headers(headers)
        json_data = asdict(index_wire)

        # get all the accept headers
        accept = []
        for _ in request.headers.getall("accept", []):
            accept.extend(_.split(","))

        if "application/cbor" not in accept and CONTENT_TYPE_DTPS_INDEX_CBOR not in accept:
            if "text/html" in accept:
                topics_html = "<ul>"
                for topic_name, topic_ref in index_internal.topics.items():
                    if topic_name.is_root():
                        continue
                    topics_html += f"<li><a href='{topic_name.as_relative_url()}'><code>{topic_name.as_relative_url()}</code></a></li>\n"
                topics_html += "</ul>"
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
        headers = CIMultiDict()
        # TODO: add nodeid
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
        for s in oq.sequence:
            a = oq.get_data_ready(s, presented_as, inline_data=False)
            history[a.sequence] = a

        hist = History(history)

        cbor = cbor2.dumps(asdict(hist))
        rd = RawData(cbor, CONTENT_TYPE_TOPIC_HISTORY_CBOR)
        title = f"History for {topic_name_s}"
        return self.visualize_data(request, title, rd, headers)

        # return web.Response(body=cbor, content_type=CONTENT_TYPE_TOPIC_HISTORY_CBOR, headers=headers)

        # logger.debug(f"serve_history: {request.url!r} -> {source!r}")  #  # raise NotImplementedError()

    #  index_internal = await source.get_meta_info(request.url.path, self)
    #
    #  index_wire = index_internal.to_wire()
    #  rd = RawData(cbor2.dumps(asdict(index_wire)), CONTENT_TYPE_DTPS_INDEX_CBOR)
    #  title = f"Meta for {topic_name_s}"
    #  return self.visualize_data(request, title, rd)

    @async_error_catcher
    async def serve_meta(self, request: web.Request) -> web.StreamResponse:
        headers = CIMultiDict()
        # TODO: add nodeid
        topic_name_s = request.match_info["topic"]
        try:
            source = self.resolve(topic_name_s)
        except KeyError as e:
            raise web.HTTPNotFound(text=f"{e}", headers=headers)

        logger.debug(f"serve_meta: {request.url!r} -> {source!r}")

        index_internal = await source.get_meta_info(request.url.path, self)

        index_wire = index_internal.to_wire()
        rd = RawData(cbor2.dumps(asdict(index_wire)), CONTENT_TYPE_DTPS_INDEX_CBOR)
        title = f"Meta for {topic_name_s}"
        return self.visualize_data(request, title, rd, headers)

    #  topics: dict[TopicNameV, TopicRef] = {}
    #  headers_s = "".join(f"{k}: {v}\n" for k, v in request.headers.items())
    #  self.logger.debug(f"serve_index: {request.url} \n {headers_s}")
    #  for topic_name, oqs in self._oqs.items():
    #      qual_topic_name = self.topics_prefix + topic_name
    #      reach = TopicReachability(URLString(qual_topic_name.as_relative_url()), self.node_id,
    #                                forwarders=[], benchmark=LinkBenchmark.identity(), )
    #      topic_ref = replace(oqs.tr, reachability=[reach])
    #      topics[qual_topic_name] = topic_ref
    #
    #  for topic_name, fd in self._forwarded.items():
    #      qual_topic_name = self.topics_prefix + topic_name
    #
    #      tr = TopicRef(fd.unique_id, fd.origin_node, {}, fd.reachability, properties=fd.properties,
    #                    created=time.time_ns(), content_info=fd.content_info, )
    #      topics[qual_topic_name] = tr
    #
    #  index_internal = TopicsIndex(topics=topics)
    #  index_wire = index_internal.to_wire()
    #
    #  headers: CIMultiDict[str] = CIMultiDict()
    #
    #  add_nocache_headers(headers)
    #  multidict_update(headers, self.get_headers_alternatives(request))
    #  self._add_own_headers(headers)
    #  json_data = asdict(index_wire)
    #
    #  json_data["debug-available"] = self.available_urls
    #
    #
    #  get all the accept headers
    #  accept = []
    #  for _ in request.headers.getall("accept", []):
    #      accept.extend(_.split(","))
    #
    #  if "application/cbor" not in accept:
    #      if "text/html" in accept:
    #          topics_html = "<ul>"
    #          for topic_name, topic_ref in topics.items():
    #              topics_html += f"<li><a href='{topic_name.as_relative_url()}'><code>{topic_name.as_dash_sep()}</code></a></li>\n"
    #          topics_html += "</ul>"
    #
    #  language=html
    #          html_index = f"""
    #          <html lang="en">
    #          <head>
    #          <style>
    #          pre {{
    #              background-color:
    # eee;
    #              padding: 10px;
    #              border: 1px solid
    # 999;
    #              border-radius: 5px;
    #          }}
    #          </style>
    #          <title>DTPS server</title>
    #          </head>
    #          <body>
    #          <h1>DTPS server</h1>
    #
    #          <p> This response coming to you in HTML format because you requested it in HTML format.</p>
    #
    #          <p>Node ID: <code>{self.node_id}</code></p>
    #          <p>Node App Data:</p>
    #          <pre><code>{yaml.dump(self.node_app_data, indent=3)}</code></pre>
    #
    #          <h2>Topics</h2>
    #          {topics_html}
    #          <h2>Index answer presented in YAML</h2>
    #          <pre><code>{yaml.dump(json_data, indent=3)}</code></pre>
    #
    #
    #          <h2>Your request headers</h2>
    #          <pre><code>{headers_s}</code></pre>
    #          </body>
    #          </html>
    #          """
    #          return web.Response(body=html_index, content_type="text/html", headers=headers)
    #
    #  as_cbor = cbor2.dumps(json_data)
    #
    #  return web.Response(body=as_cbor, content_type=CONTENT_TYPE_DTPS_INDEX_CBOR, headers=headers)

    def resolve(self, url: str) -> Source:
        after: Optional[str]

        if url and not url.endswith("/"):
            url, _, after = url.rpartition("/")
        else:
            after = None

        tn = TopicNameV.from_relative_url(url)
        sources = self.iterate_sources()

        subtopics: list[tuple[TopicNameV, list[str], list[str], Source]] = []

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
            raise KeyError(f"Cannot find a matching topic for {url}")

        origin_node = self.node_id
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

    def iterate_sources(self) -> dict[TopicNameV, Source]:
        res: dict[TopicNameV, Source] = {}
        for topic_name, x in self._forwarded.items():
            sb = ForwardedQueue(topic_name)
            res[topic_name] = sb
        for topic_name, x in self._oqs.items():
            sb = OurQueue(topic_name)
            res[topic_name] = sb

        ordered = sorted(res.items(), key=lambda y: len(y[0].components), reverse=True)
        return {k: v for k, v in ordered}

    @async_error_catcher
    async def serve_get(self, request: web.Request) -> web.StreamResponse:
        headers: CIMultiDict[str] = CIMultiDict()
        self._add_own_headers(headers)
        add_nocache_headers(headers)

        topic_name_s = request.match_info["topic"]

        try:
            source = self.resolve(topic_name_s)
        except KeyError as e:
            logger.error(f"serve_get: {request.url!r} -> {topic_name_s!r} -> {e}")
            raise web.HTTPNotFound(text=f"{e}", headers=headers) from e

        put_meta_headers(headers, source.get_properties(self))

        logger.debug(f"serve_get: {request.url!r} -> {source!r}")

        if isinstance(source, ForwardedQueue):
            # Optimization: streaming
            return await self.serve_get_proxied(request, self._forwarded[source.topic_name])

        # if topic_name not in self._oqs:
        #     msg = f'Cannot find topic "{topic_name.as_dash_sep()}"'
        #     raise web.HTTPNotFound(text=msg, headers=headers)

        title = topic_name_s
        url = topic_name_s
        logger.info(f"url: {topic_name_s!r} source: {source!r}")
        rs = await source.get_resolved_data(request, url, self)
        rd: RawData
        match rs:
            case RawData() as r:
                rd = r
            case Native(ob):
                logger.info(f"Native: {ob}")
                cbor_data = cbor2.dumps(ob)
                rd = RawData(
                    cbor_data, "application/cbor"
                )  # return web.Response(body=cbor_data, headers=headers, content_type="application/cbor")
            case NotAvailableYet() as r:
                rd = r  # return web.Response(body=None, headers=headers, status=209)
            case NotFound():
                raise NotImplementedError(f"Cannot handle {rs!r}")
            case _:
                raise AssertionError

        return self.visualize_data(request, title, rd, headers)

    def make_friendly_visualization(self, title: str, rd: RawData) -> web.StreamResponse:
        headers: CIMultiDict[str] = CIMultiDict()

        yaml_str = rd.get_as_yaml()
        html_index = f"""
        <html>
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
    
        <p>This response coming to you in HTML format because you requested it in HTML format.</p>
        
        <p>Original content type: <code>{rd.content_type}</code></p>
        
        <pre><code>{yaml_str}</code></pre>
     
        """
        return web.Response(body=html_index, content_type="text/html", headers=headers)

    def visualize_data(
        self, request: web.Request, title: str, rd: RawData | NotAvailableYet, headers: CIMultiDict[str]
    ) -> web.StreamResponse:
        accept_headers = request.headers.get("accept", "")
        # headers: CIMultiDict[str] = CIMultiDict()
        # self._add_own_headers(headers)
        # put_meta_headers(headers)
        # add_nocache_headers(headers)
        # multidict_update(headers, self.get_headers_alternatives(request))
        # logger.info(f'request headers: {dict(request.headers)}')
        accepts_html = "text/html" in accept_headers
        if isinstance(rd, RawData):
            if rd.content_type != "text/html" and accepts_html and is_structure(rd.content_type):
                return self.make_friendly_visualization(title, rd)
            else:
                return web.Response(body=rd.content, content_type=rd.content_type, headers=headers)
        elif isinstance(rd, NotAvailableYet):
            if accepts_html:
                html_index = f"""
<html>
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
                return web.Response(body=html_index, content_type="text/html", status=209, headers=headers)
            else:
                return web.Response(body=None, content_type=None, status=209, headers=headers)

        else:
            raise AssertionError(f"Cannot handle {rd!r}")

    #  headers[HEADER_DATA_UNIQUE_ID] = oq.tr.unique_id
    #  headers[HEADER_DATA_ORIGIN_NODE_ID] = oq.tr.origin_node
    #  headers[HEADER_NODE_ID] = self.node_id

    #  try:
    #      data = oq.last_data()
    #  except KeyError:
    #      return web.Response(body=None, headers=headers, status=209)
    #  209 = no content

    #  lb = LinkBenchmark.identity()
    #  lb.fill_headers(headers)
    #
    #  return web.Response(body=data.content, headers=headers, content_type=data.content_type)

    #
    # @async_error_catcher

    @async_error_catcher
    async def serve_get_proxied(self, request: web.Request, fd: ForwardedTopic) -> web.StreamResponse:
        from .client import DTPSClient

        async with DTPSClient.create() as client:
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

                    for r in fd.reachability:
                        if r.answering == self.node_id:
                            r.benchmark.fill_headers(headers)

                    # headers.add('X-DTPS-Forwarded-node', resp.headers.get(HEADER_NODE_ID, '???'))
                    multidict_update(headers, self.get_headers_alternatives(request))
                    self._add_own_headers(headers)
                    # headers.update(HEADER_NO_CACHE)

                    response = web.StreamResponse(status=resp.status, headers=headers)

                    await response.prepare(request)
                    async for chunk in resp.content.iter_any():
                        await response.write(chunk)

                    return response

    def _add_own_headers(self, headers: CIMultiDict) -> None:
        # passed_already = headers.get(HEADER_NODE_PASSED_THROUGH, [])

        prevnodeids = headers.getall(HEADER_NODE_ID, [])
        if len(prevnodeids) > 1:
            raise ValueError(f"More than one {HEADER_NODE_ID} header found: {prevnodeids}")

        if prevnodeids:
            headers.add(HEADER_NODE_PASSED_THROUGH, prevnodeids[0])

        server_string = f"lib-dtps-http/Python/{__version__}"
        HEADER_SERVER = "Server"

        current_server_strings = headers.getall(HEADER_SERVER, [])
        # logger.info(f'current_server_strings: {current_server_strings} cur = {headers}')
        if HEADER_SERVER not in current_server_strings:
            headers.add(HEADER_SERVER, server_string)
        # leave our own
        headers.popall(HEADER_NODE_ID, None)
        headers[HEADER_NODE_ID] = self.node_id

    @async_error_catcher
    async def serve_post(self, request: web.Request) -> web.Response:
        topic_name_s: str = request.match_info["topic"]
        topic_name = TopicNameV.from_relative_url(topic_name_s)

        content_type = request.headers.get("Content-Type", "application/octet-stream")
        data = await request.read()
        oq = self.get_oq(topic_name)
        rd = RawData(data, content_type)

        oq.publish(rd)

        headers: CIMultiDict[str] = CIMultiDict()
        add_nocache_headers(headers)
        self._add_own_headers(headers)
        multidict_update(headers, self.get_headers_alternatives(request))
        return web.Response(status=200, headers=headers)

    @async_error_catcher
    async def serve_data_get(self, request: web.Request) -> web.Response:
        headers: CIMultiDict[str] = CIMultiDict()
        multidict_update(headers, self.get_headers_alternatives(request))
        self._add_own_headers(headers)
        topic_name_s = request.match_info["topic"] + "/"
        topic_name = TopicNameV.from_relative_url(topic_name_s)
        digest = request.match_info["digest"]
        if topic_name not in self._oqs:
            raise web.HTTPNotFound(headers=headers)
        oq = self._oqs[topic_name]
        data = oq.get(digest)
        # headers[HEADER_SEE_EVENTS] = f"../events/"
        headers[HEADER_DATA_UNIQUE_ID] = oq.tr.unique_id
        headers[HEADER_DATA_ORIGIN_NODE_ID] = oq.tr.origin_node

        return web.Response(body=data.content, headers=headers, content_type=data.content_type)

    @async_error_catcher
    async def serve_events(self, request: web.Request) -> web.WebSocketResponse:
        if SEND_DATA_ARGNAME in request.query:
            send_data = True
        else:
            send_data = False

        topic_name_s = request.match_info["topic"]

        logger.info(f"serve_events: {request} topic_name={topic_name_s} send_data={send_data}")
        topic_name = TopicNameV.from_relative_url(topic_name_s)
        if topic_name not in self._oqs and topic_name not in self._forwarded:
            headers: CIMultiDict[str] = CIMultiDict()

            self._add_own_headers(headers)
            raise web.HTTPNotFound(headers=headers)

        logger.info(
            f"serve_events: {topic_name.as_dash_sep()} send_data={send_data} headers={request.headers}"
        )
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

            await self.serve_events_forwarder(ws, fd, send_data)
            return ws

        oq_ = self._oqs[topic_name]
        ws.headers[HEADER_DATA_UNIQUE_ID] = oq_.tr.unique_id
        ws.headers[HEADER_DATA_ORIGIN_NODE_ID] = oq_.tr.origin_node
        await ws.prepare(request)

        exit_event = asyncio.Event()
        ci = oq_.get_channel_info()

        await ws.send_bytes(get_tagged_cbor(ci))

        async def send_message(_: ObjectQueue, i: int) -> None:
            mdata = oq_.sequence[i]
            digest = mdata.digest

            # if send_data:
            #     nchunks = 1
            #     availability_ = []
            # else:
            #     nchunks = 0
            #     availability_ = [ResourceAvailability(url=URLString(f"../data/{digest}/"),
            #                                           available_until=time.time() + 60)]

            # data = DataReady(sequence=i, time_inserted=mdata.time_inserted, digest=mdata.digest,
            #                  content_type=mdata.content_type, content_length=mdata.content_length,
            #                  availability=availability_, chunks_arriving=nchunks, clocks=mdata.clocks,
            #                  unique_id=oq_.tr.unique_id, origin_node=oq_.tr.origin_node, )

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
                chunk = Chunk(digest, 0, 1, 0, the_bytes)
                as_cbor = get_tagged_cbor(chunk)

                try:
                    await ws.send_bytes(as_cbor)
                except ConnectionResetError:
                    exit_event.set()
                    pass

        if oq_.sequence:
            last = oq_.last()

            await send_message(oq_, last.index)

        s = oq_.subscribe(send_message)
        try:
            await exit_event.wait()
            await ws.close()
        finally:
            oq_.unsubscribe(s)

        return ws

    @async_error_catcher
    async def serve_events_forwarder(
        self,
        ws: web.WebSocketResponse,
        fd: ForwardedTopic,
        inline_data: bool,
    ) -> None:
        assert fd.forward_url_events is not None
        while True:
            if ws.closed:
                break

            try:
                # FIXME: logic not correct
                if inline_data:
                    if fd.forward_url_events_inline_data is not None:
                        await self.serve_events_forward_simple(ws, fd.forward_url_events_inline_data)
                    else:
                        await self.serve_events_forwarder_one(ws, True)
                else:
                    url = fd.forward_url_events

                    await self.serve_events_forwarder_one(ws, url, inline_data)
            except Exception as e:
                logger.error(f"Exception in serve_events_forwarder_one: {traceback.format_exc()}")
                await asyncio.sleep(1)

    async def serve_events_forward_simple(self, ws_to_write: web.WebSocketResponse, url: URL) -> None:
        """Iterates using direct data in websocket."""
        logger.debug(f"serve_events_forward_simple: {url} [no overhead forwarding]")
        from dtps_http import DTPSClient

        async with DTPSClient.create() as client:
            async with client.my_session(url) as (session, use_url):
                async with session.ws_connect(use_url) as ws:
                    logger.debug(f"websocket to {use_url} ready")
                    async for msg in ws:
                        if msg.type == WSMsgType.CLOSE:
                            break
                        if msg.type == WSMsgType.TEXT:
                            await ws_to_write.send_str(msg.data)
                        elif msg.type == WSMsgType.BINARY:
                            await ws_to_write.send_bytes(msg.data)
                        else:
                            logger.warning(f"Unknown message type {msg.type}")

    @async_error_catcher
    async def serve_events_forwarder_one(
        self, ws: web.WebSocketResponse, url: URL, inline_data: bool
    ) -> None:
        assert isinstance(url, URL)
        use_remote_inline_data = True  # TODO: configurable
        logger.debug(f"serve_events_forwarder_one: {url} {inline_data=} {use_remote_inline_data=}")
        from .client import DTPSClient

        async with DTPSClient.create() as client:
            async for dr, rd in client.listen_url_events(url, inline_data=use_remote_inline_data):
                urls = [join(url, _.url) for _ in dr.availability]
                self.digest_to_urls[dr.digest] = urls
                digesturl = URLString(f"../data/{dr.digest}/")
                urls_strings: list[URLString]
                urls_strings = [url_to_string(_) for _ in urls] + [digesturl]
                if inline_data:
                    availability = []
                    chunks_arriving = 1
                else:
                    availability = [
                        ResourceAvailability(url=url_string, available_until=time.time() + 60)
                        for url_string in urls_strings
                    ]
                    chunks_arriving = 0
                dr2 = DataReady(
                    sequence=dr.sequence,
                    time_inserted=dr.time_inserted,
                    digest=dr.digest,
                    content_type=dr.content_type,
                    content_length=dr.content_length,
                    availability=availability,
                    chunks_arriving=chunks_arriving,
                    clocks=dr.clocks,
                    origin_node=dr.origin_node,
                    unique_id=dr.unique_id,
                )
                # logger.debug(f"Forwarding {dr} -> {dr2}")
                await ws.send_json(asdict(dr2))

                if inline_data:
                    await ws.send_bytes(rd.content)


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
    put_link_header(h, f"{REL_URL_META}/", REL_META, CONTENT_TYPE_DTPS_INDEX_CBOR)

    if tp.has_history:
        put_link_header(h, f"{REL_URL_HISTORY}/", REL_HISTORY, CONTENT_TYPE_TOPIC_HISTORY_CBOR)


#


def put_link_header(h: CIMultiDict[str], url: str, rel: str, content_type: Optional[str]):
    l = LinkHeader(url, rel, attributes={"rel": rel})
    if content_type is not None:
        l.attributes["type"] = content_type

    h.add("Link", l.to_header())


@dataclass
class LinkHeader:
    url: str
    rel: str
    attributes: dict[str, str] = field(default_factory=dict)

    def to_header(self) -> str:
        s = f"<{self.url}>; rel={self.rel}"
        for k, v in self.attributes.items():
            s += f"; {k}={v}"
        return s

    @classmethod
    def parse(cls, header: str) -> "LinkHeader":
        pairs = header.split(";")
        if not pairs:
            raise ValueError
        first = pairs[0]
        if not first.startswith("<") or not first.endswith(">"):
            raise ValueError
        url = first[1:-1]

        attributes: dict[str, str] = {}
        for p in pairs[1:]:
            p = p.strip()
            k, _, v = p.partition("=")
            k = k.strip()
            v = v.strip()
            attributes[k] = v

        rel = attributes.pop("rel", "")
        return cls(url, rel, attributes)


def get_link_headers(h: CIMultiDict[str]) -> dict[str, LinkHeader]:
    res: dict[str, LinkHeader] = {}
    for l in h.getall("Link", []):
        lh = LinkHeader.parse(l)
        res[lh.rel] = lh
    return res


@async_error_catcher
async def update_clock(s: DTPSServer, topic_name: TopicNameV, interval: float, initial_delay: float) -> None:
    await asyncio.sleep(initial_delay)
    logger.info(f"Starting clock {topic_name.as_relative_url()} with interval {interval}")
    oq = s.get_oq(topic_name)
    while True:
        t = time.time_ns()
        data = str(t).encode()
        oq.publish(RawData(data, "application/json"))
        await asyncio.sleep(interval)


def get_tagged_cbor(ob: dataclass) -> bytes:
    data = {ob.__class__.__name__: asdict(ob)}
    return cbor2.dumps(data)
