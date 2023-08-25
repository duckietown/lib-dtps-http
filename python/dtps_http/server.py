import asyncio
import json
import time
import traceback
import uuid
from dataclasses import asdict, field
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
    REL_META,
    REL_URL_META,
    TOPIC_AVAILABILITY,
    TOPIC_CLOCK,
    TOPIC_LIST,
    TOPIC_LOGS,
    TOPIC_STATE_NOTIFICATION,
    TOPIC_STATE_SUMMARY,
)
from .structures import (
    ContentInfo,
    DataReady,
    LinkBenchmark,
    RawData,
    ResourceAvailability,
    TopicProperties,
    TopicReachability,
    TopicRef,
    TopicsIndex,
)
from .types import NodeID, SourceID, TopicNameV, URLString
from .urls import join, parse_url_unescape, URL, url_to_string
from .utils import async_error_catcher, multidict_update

SEND_DATA_ARGNAME = "send_data"

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

    def __init__(self, hub: Hub, name: TopicNameV, tr: TopicRef):
        self._hub = hub
        self._pub = Publisher(self._hub, Key())
        self._sub = Subscriber(self._hub, name.as_relative_url())
        self._seq = 0
        self.sequence = []
        self._data = {}
        self._name = name
        self.tr = tr

    def publish_text(self, text: str) -> None:
        data = text.encode("utf-8")
        content_type = "text/plain"
        self.publish(RawData(data, content_type))

    def publish_cbor(self, obj: object) -> None:
        """Publish a python object as a cbor2 encoded object."""
        data = cbor2.dumps(obj)
        content_type = "application/cbor"
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
        ds = DataSaved(use_seq, time.time_ns(), digest, obj.content_type, len(obj.content))
        self._data[digest] = obj
        self.sequence.append(ds)
        self._pub.publish(
            Key(self._name.as_relative_url(), K_INDEX), use_seq
        )  # logger.debug(f"published #{self._seq} {self._name}: {obj!r}")

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
        routes.get("/")(self.serve_index)
        routes.get("/{topic:.*}/events/")(self.serve_events)
        routes.get("/{topic:.*}/data/{digest}/")(self.serve_data_get)
        routes.post("/{topic:.*}/")(self.serve_post)
        routes.get("/{topic:.*}/")(self.serve_get)
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
            get_unique_id(self.node_id, TOPIC_LIST),
            self.node_id,
            {},
            [],
            content_info=content_info,
            properties=TopicProperties.streamable_readonly(),
            created=time.time_ns(),
        )
        self._oqs[TOPIC_LIST] = ObjectQueue(self.hub, TOPIC_LIST, tr)

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

    def remember_task(self, task: "asyncio.Task[Any]") -> None:
        """Add a task to the list of tasks to be cancelled on shutdown"""
        self.tasks.append(task)

    def _update_lists(self):
        self._oqs[TOPIC_LIST].publish_json(sorted([_.as_relative_url() for _ in self._oqs]))

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

    async def get_oq(self, name: TopicNameV) -> ObjectQueue:
        if name in self._forwarded:
            raise ValueError(f"Topic {name} is a forwarded one")

        # if name not in self._oqs:
        #     raise KeyError(f"Topic {name.as_relative_url()} not found")
        #     if tr is None:
        #         unique_id = get_unique_id(self.node_id, name)
        #
        #         tr = TopicReachability(URLString(f"topics/{name}/"), self.node_id, forwarders=[],
        #                                benchmark=LinkBenchmark.identity(), )
        #         reachability: list[TopicReachability] = [tr]
        #         tr = TopicRef(unique_id=unique_id, origin_node=self.node_id, app_data={},
        #                       reachability=reachability, )
        #     self._oqs[name] = ObjectQueue(self.hub, name, tr)
        #
        #     self._update_lists()
        return self._oqs[name]

    async def create_oq(self, name: TopicNameV, tp: Optional[TopicProperties] = None) -> ObjectQueue:
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

        self._oqs[name] = ObjectQueue(self.hub, name, tr)
        self._update_lists()
        return self._oqs[name]

    @async_error_catcher
    async def on_startup(self, _: web.Application) -> None:
        self.logger.info("on_startup")
        for f in self._more_on_startup:
            await f(self)

        await self.create_oq(TOPIC_LOGS)
        await self.create_oq(TOPIC_CLOCK)
        await self.create_oq(TOPIC_AVAILABILITY)
        await self.create_oq(TOPIC_STATE_SUMMARY)
        await self.create_oq(TOPIC_STATE_NOTIFICATION)

    @async_error_catcher
    async def on_shutdown(self, _: web.Application) -> None:
        self.logger.info("on_shutdown")
        for t in self.tasks:
            t.cancel()

    @async_error_catcher
    async def serve_index(self, request: web.Request) -> web.Response:
        topics: dict[TopicNameV, TopicRef] = {}
        headers_s = "".join(f"{k}: {v}\n" for k, v in request.headers.items())
        self.logger.debug(f"serve_index: {request.url} \n {headers_s}")
        for topic_name, oqs in self._oqs.items():
            qual_topic_name = self.topics_prefix + topic_name

            topic_ref: TopicRef = oqs.tr
            topics[qual_topic_name] = topic_ref

        for topic_name, fd in self._forwarded.items():
            qual_topic_name = self.topics_prefix + topic_name
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

        index_internal = TopicsIndex(topics=topics)
        index_wire = index_internal.to_wire()

        # print(yaml.dump(asdict(index), indent=3))
        headers: CIMultiDict[str] = CIMultiDict()

        add_nocache_headers(headers)
        multidict_update(headers, self.get_headers_alternatives(request))
        self._add_own_headers(headers)
        json_data = asdict(index_wire)
        # json_data["debug-available"] = self.available_urls

        # get all the accept headers
        accept = []
        for _ in request.headers.getall("accept", []):
            accept.extend(_.split(","))

        if "application/cbor" not in accept:
            if "text/html" in accept:
                topics_html = "<ul>"
                for topic_name, topic_ref in topics.items():
                    topics_html += f"<li><a href='topics/{topic_name.as_relative_url()}/'><code>{topic_name.as_relative_url()}</code></a></li>\n"
                topics_html += "</ul>"

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

        return web.Response(
            body=as_cbor, content_type=CONTENT_TYPE_DTPS_INDEX_CBOR, headers=headers
        )  # return web.json_response(json_data, content_type=CONTENT_TYPE_TOPIC_DIRECTORY, headers=headers)

    @async_error_catcher
    async def serve_get(self, request: web.Request) -> web.StreamResponse:
        topic_name_s = request.match_info["topic"]
        headers: CIMultiDict[str] = CIMultiDict()
        self._add_own_headers(headers)
        add_nocache_headers(headers)

        topic_name = TopicNameV.from_relative_url(topic_name_s)
        if topic_name in self._forwarded:
            return await self.serve_get_proxied(request, self._forwarded[topic_name])

        if topic_name not in self._oqs:
            raise web.HTTPNotFound(headers=headers)

        oq = self._oqs[topic_name]
        put_meta_headers(headers)
        # headers[HEADER_SEE_EVENTS] = f"events/"
        # headers[HEADER_SEE_EVENTS_INLINE_DATA] = f"events/?{SEND_DATA_ARGNAME}"
        multidict_update(headers, self.get_headers_alternatives(request))
        headers[HEADER_DATA_UNIQUE_ID] = oq.tr.unique_id
        headers[HEADER_DATA_ORIGIN_NODE_ID] = oq.tr.origin_node
        headers[HEADER_NODE_ID] = self.node_id

        try:
            data = oq.last_data()
        except KeyError:
            return web.Response(body=None, headers=headers, status=209)  # 209 = no content

        lb = LinkBenchmark.identity()
        lb.fill_headers(headers)

        return web.Response(body=data.content, headers=headers, content_type=data.content_type)

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
        oq = await self.get_oq(topic_name)
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

        topic_name = request.match_info["topic"]
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
        logger.info(f"serve_events: {request}")
        if SEND_DATA_ARGNAME in request.query:
            send_data = True
        else:
            send_data = False

        topic_name = request.match_info["topic"]
        if topic_name not in self._oqs and topic_name not in self._forwarded:
            headers: CIMultiDict[str] = CIMultiDict()

            self._add_own_headers(headers)
            raise web.HTTPNotFound(headers=headers)

        logger.info(f"serve_events: {topic_name} send_data={send_data} headers={request.headers}")
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

        async def send_message(_: ObjectQueue, i: int) -> None:
            mdata = oq_.sequence[i]
            digest = mdata.digest

            if send_data:
                nchunks = 1
                availability_ = []
            else:
                nchunks = 0
                availability_ = [
                    ResourceAvailability(
                        url=URLString(f"../data/{digest}/"), available_until=time.time() + 60
                    )
                ]

            data = DataReady(
                sequence=i,
                time_inserted=mdata.time_inserted,
                digest=mdata.digest,
                content_type=mdata.content_type,
                content_length=mdata.content_length,
                availability=availability_,
                chunks_arriving=nchunks,
            )
            if ws.closed:
                exit_event.set()
                return
            try:
                as_struct = asdict(data)
                as_cbor = cbor2.dumps(as_struct)
                await ws.send_bytes(as_cbor)
            except ConnectionResetError:
                exit_event.set()
                pass

            if send_data:
                the_bytes = oq_.get(digest).content
                try:
                    await ws.send_bytes(the_bytes)
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
                )
                # logger.debug(f"Forwarding {dr} -> {dr2}")
                await ws.send_json(asdict(dr2))

                if inline_data:
                    await ws.send_bytes(rd.content)


def add_nocache_headers(h: CIMultiDict[str]) -> None:
    h.update(HEADER_NO_CACHE)
    h["Cookie"] = f"help-no-cache={time.monotonic_ns()}"


def get_unique_id(node_id: NodeID, topic_name: TopicNameV) -> SourceID:
    return cast(SourceID, f"{node_id}:{topic_name.as_relative_url()}")


def put_meta_headers(h: CIMultiDict[str]):
    put_link_header(h, f"{EVENTS_SUFFIX}/", REL_EVENTS_NODATA, "websocket")
    put_link_header(h, f"{EVENTS_SUFFIX}/?send_data=1", REL_EVENTS_DATA, "websocket")
    put_link_header(
        h, f"{REL_URL_META}/", REL_META, CONTENT_TYPE_DTPS_INDEX_CBOR
    )  # put_link_header(  #     h,  #     f"{URL_HISTORY}/",  #     REL_HISTORY,  #     CONTENT_TYPE_DTPS_INDEX_CBOR  # )


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
