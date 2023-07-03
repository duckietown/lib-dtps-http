import asyncio
import json
import time
import traceback
import uuid
from dataclasses import asdict, replace
from typing import Awaitable, Callable, Optional, Sequence

import cbor2
from aiohttp import web
from aiopubsub import Hub, Key, Publisher, Subscriber
from multidict import CIMultiDict
from pydantic.dataclasses import dataclass

from . import logger
from .components import join_topic_names
from .constants import (
    CONTENT_TYPE_TOPIC_DIRECTORY,
    HEADER_NO_CACHE,
    HEADER_NODE_ID,
    HEADER_NODE_PASSED_THROUGH,
    HEADER_SEE_EVENTS,
    TOPIC_LIST,
)
from .structures import (
    DataReady,
    NodeID,
    RawData,
    SourceID,
    TopicName,
    TopicReachability,
    TopicRef,
    TopicsIndex,
)
from .types import URLString
from .urls import join, URL, url_to_string
from .utils import async_error_catcher

__all__ = [
    "DTPSServer",
]


@dataclass
class DataSaved:
    index: int
    time_inserted: int
    digest: str
    # cid: str


SUB_ID = str
K_INDEX = "index"


class ObjectQueue:
    _sequence: list[DataSaved]
    _data: dict[int, RawData]
    _seq: int
    _name: TopicName

    def __init__(self, hub: Hub, name: TopicName, tr: TopicRef):
        self._hub = hub
        self._pub = Publisher(self._hub, Key())
        self._sub = Subscriber(self._hub, name)
        self._seq = 0
        self._sequence = []
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
        ds = DataSaved(use_seq, time.time_ns(), digest)
        self._data[use_seq] = obj
        self._sequence.append(ds)
        self._pub.publish(Key(self._name, K_INDEX), use_seq)
        # logger.debug(f"published #{self._seq} {self._name}: {obj!r}")

    def last(self) -> DataSaved:
        if self._sequence:
            ds = self._sequence[-1]
            return ds
        else:
            raise KeyError("No data in queue")

    def last_data(self) -> RawData:
        return self.get(self.last().index)

    def get(self, index: int) -> RawData:
        return self._data[index]

    def subscribe(self, callback: "Callable[[ObjectQueue, int], Awaitable[None]]") -> SUB_ID:
        wrap_callback = lambda key, msg: callback(self, msg)
        self._sub.add_async_listener(Key(self._name, K_INDEX), wrap_callback)
        # last_used = list(self._sub._listeners)[-1]
        return ""  # TODO

    def unsubscribe(self, sub_id: SUB_ID) -> None:
        pass  # TODO


class DTPSServer:
    _oqs: dict[TopicName, ObjectQueue]

    def __init__(
        self,
        *,
        topics_prefix: str | tuple[str, ...] = (),
        on_startup: "Sequence[Callable[[DTPSServer], Awaitable[None]]]" = (),
    ) -> None:
        self.app = web.Application()

        self.topics_prefix = topics_prefix
        routes = web.RouteTableDef()
        self._more_on_startup = on_startup
        self.app.on_startup.append(self.on_startup)
        self.app.on_shutdown.append(self.on_shutdown)
        routes.get("/")(self.serve_index)
        routes.post("/topics/{topic}/")(self.serve_post)
        routes.get("/topics/{topic}/")(self.serve_get)
        routes.get("/topics/{topic}/events/")(self.serve_events)
        routes.get("/topics/{topic}/data/{index}")(self.serve_data_get)
        routes.get("/data/{digest}/")(self.serve_data_digest)
        self.app.add_routes(routes)

        self.hub = Hub()
        self._oqs = {}  # 'clock': ObjectQueue(self.hub, 'clock')}
        self.tasks = []
        self.logger = logger
        self.available_urls = []
        self.node_id = NodeID(str(uuid.uuid4()))

        # self.own_headers = CIMultiDict()
        # self.own_headers[HEADER_NODE_ID] = self.node_id
        self.forward_events = {}
        self.forward_data = {}
        self.digest_to_urls = {}

    digest_to_urls: dict[str, list[URL]]
    forward_events: dict[TopicName, URL]
    forward_data: dict[TopicName, URL]
    node_id: NodeID

    def get_headers_alternatives(self, request: web.Request) -> CIMultiDict:
        original_url = request.url

        sock = request.transport._sock  # type: ignore
        sockname = sock.getsockname()
        if isinstance(sockname, str):
            path = sockname.replace("/", "%2F")
            use_url = str(original_url).replace("http://", "http+unix://").replace("localhost", path)
        else:
            use_url = str(original_url)

        HEADER_NO_AVAIL = "X-Content-Location-Not-Available"

        res = CIMultiDict()
        if not self.available_urls:
            res[HEADER_NO_AVAIL] = "No alternative URLs available"
            return res

        alternatives = []
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

        for a in sorted(alternatives):
            res.add("Content-Location", a)
        if not alternatives:
            res[HEADER_NO_AVAIL] = f"Nothing matched {url} of {self.available_urls}"
        else:
            res.popall(HEADER_NO_AVAIL, [])
        # list_alternatives = "".join(f"\n  {_}" for _ in alternatives)
        # logger.debug(f"get_headers_alternatives: {request.url} -> \n effective: {use_url} \n {
        # list_alternatives}")
        return res

    def set_available_urls(self, urls: list[str]) -> None:
        self.available_urls = urls

    def remember_task(self, task: asyncio.Task) -> None:
        """Add a task to the list of tasks to be cancelled on shutdown"""
        self.tasks.append(task)

    async def remove_oq(self, name: TopicName) -> None:
        if name in self._oqs:
            del self._oqs[name]
            if TOPIC_LIST in self._oqs:
                self._oqs[TOPIC_LIST].publish_json(sorted(list(self._oqs)))

    async def get_oq(self, name: TopicName, tr: Optional[TopicRef] = None) -> ObjectQueue:
        if name not in self._oqs:
            if tr is None:
                unique_id = SourceID(f"{name}@{self.node_id}")

                tr = TopicReachability(URLString(f"topics/{name}/"), self.node_id, forwarders=[])
                reachability: list[TopicReachability] = [tr]
                tr = TopicRef(
                    unique_id=unique_id,
                    origin_node=self.node_id,
                    app_static_data=None,
                    reachability=reachability,
                )
            self._oqs[name] = ObjectQueue(self.hub, name, tr)

            if TOPIC_LIST in self._oqs:
                self._oqs[TOPIC_LIST].publish_json(sorted(list(self._oqs)))
        return self._oqs[name]

    @async_error_catcher
    async def on_startup(self, _) -> None:
        self.logger.info("on_startup")
        for f in self._more_on_startup:
            await f(self)

        await self.get_oq(TOPIC_LIST)

    @async_error_catcher
    async def on_shutdown(self, _) -> None:
        self.logger.info("on_shutdown")
        for t in self.tasks:
            t.cancel()

    @async_error_catcher
    async def serve_index(self, request: web.Request) -> web.Response:
        topics = {}
        for topic_name, oqs in self._oqs.items():
            qual_topic_name = join_topic_names(self.topics_prefix, topic_name)

            topic_ref: TopicRef = oqs.tr
            print(json.dumps(asdict(topic_ref), indent=3))
            current_reachability = topic_ref.reachability
            all_reachability = []
            all_reachability.extend(current_reachability)

            # for reach in current_reachability:
            #     if '://' not in reach.url and reach.answering == self.node_id:  # relative
            #
            #         for a in self.available_urls:
            #             new_url = a + reach.url
            #
            #             reach_new = TopicReachability(
            #                 url=URLString(new_url),
            #                 answering=self.node_id,
            #                 forwarders=reach.forwarders
            #             )
            #             all_reachability.append(reach_new)
            #
            # reach2 = TopicReachability(
            #     url=URLString(f"topics/{topic_name}/"),
            #     forwarders=[]
            # )
            # reachability2 = oqs.tr.reachability + [reach2]
            # urls = [f"topics/{topic_name}/"] + oqs.tr.urls

            topic_ref2 = replace(topic_ref, reachability=all_reachability)

            topics[qual_topic_name] = topic_ref2

        index = TopicsIndex(node_id=self.node_id, topics=topics)

        print(json.dumps(asdict(index), indent=3))
        headers = CIMultiDict()
        headers.update(HEADER_NO_CACHE)
        headers.update(self.get_headers_alternatives(request))
        self._add_own_headers(headers)
        return web.json_response(asdict(index), content_type=CONTENT_TYPE_TOPIC_DIRECTORY, headers=headers)

    @async_error_catcher
    async def serve_get(self, request: web.Request) -> web.Response:
        topic_name = request.match_info["topic"]
        headers = CIMultiDict()
        self._add_own_headers(headers)
        headers.update(HEADER_NO_CACHE)

        if topic_name in self.forward_data:
            return await self.serve_get_proxied(request, self.forward_data[topic_name])

        if topic_name not in self._oqs:
            raise web.HTTPNotFound(headers=headers)

        oq = self._oqs[topic_name]
        data = oq.last_data()
        headers[HEADER_SEE_EVENTS] = f"events/"
        headers.update(self.get_headers_alternatives(request))
        return web.Response(body=data.content, headers=headers, content_type=data.content_type)

    @async_error_catcher
    async def serve_get_proxied(self, request: web.Request, url: URL) -> web.StreamResponse:
        from .client import DTPSClient

        async with DTPSClient.create() as client:
            async with client._session(url) as (session, use_url):
                # Create the proxied request using the original request's headers
                async with session.get(use_url, headers=request.headers) as resp:
                    # Read the response's body

                    # Create a response with the proxied request's status and body,
                    # forwarding all the headers
                    headers = CIMultiDict()
                    headers.update(resp.headers)
                    headers.popall("X-Content-Location-Not-Available", [])
                    headers.popall("Content-Location", [])
                    # headers.add('X-DTPS-Forwarded-node', resp.headers.get(HEADER_NODE_ID, '???'))
                    headers.update(self.get_headers_alternatives(request))
                    self._add_own_headers(headers)
                    headers.update(HEADER_NO_CACHE)

                    response = web.StreamResponse(status=resp.status, headers=headers)

                    await response.prepare(request)
                    async for chunk in resp.content.iter_any():
                        await response.write(chunk)

                    return response

                    #
                    # response = web.Response(
                    #     body=body,
                    #     status=resp.status,
                    #     headers=headers,
                    # )
                    # logger.info(f'Proxied request to {use_url}, {response}')
                    # return response

    def _add_own_headers(self, headers: CIMultiDict) -> None:
        # passed_already = headers.get(HEADER_NODE_PASSED_THROUGH, [])

        prevnodeids = headers.getall(HEADER_NODE_ID, [])
        if len(prevnodeids) > 1:
            raise ValueError(f"More than one {HEADER_NODE_ID} header found: {prevnodeids}")

        if prevnodeids:
            headers.add(HEADER_NODE_PASSED_THROUGH, prevnodeids[0])

        # headers.add(HEADER_NODE_PASSED_THROUGH, self.node_id)
        headers.popall(HEADER_NODE_ID, None)
        headers[HEADER_NODE_ID] = self.node_id

    @async_error_catcher
    async def serve_post(self, request: web.Request) -> web.Response:
        topic_name = request.match_info["topic"]

        content_type = request.headers.get("Content-Type", "application/octet-stream")
        data = await request.read()
        oq = await self.get_oq(topic_name)
        rd = RawData(data, content_type)

        # logger.info(f"Received data for topic {topic_name!r}: {rd}")
        oq.publish(rd)
        # logger.info(f"published data for topic {topic_name!r}: {rd}")

        headers = CIMultiDict()
        headers.update(HEADER_NO_CACHE)
        self._add_own_headers(headers)
        headers.update(self.get_headers_alternatives(request))
        return web.Response(status=200, headers=headers)

    @async_error_catcher
    async def serve_data_get(self, request: web.Request) -> web.Response:
        headers = CIMultiDict()
        headers.update(self.get_headers_alternatives(request))
        self._add_own_headers(headers)

        topic_name = request.match_info["topic"]
        index = int(request.match_info["index"])
        if topic_name not in self._oqs:
            raise web.HTTPNotFound(headers=headers)
        oq = self._oqs[topic_name]
        data = oq.get(index)
        headers[HEADER_SEE_EVENTS] = f"../events/"

        return web.Response(body=data.content, headers=headers, content_type=data.content_type)

    @async_error_catcher
    async def serve_data_digest(self, request: web.Request) -> web.Response:
        headers = CIMultiDict()

        digest = request.match_info["digest"]
        if digest not in self.digest_to_urls:
            self._add_own_headers(headers)
            raise web.HTTPNotFound(headers=headers)

        urls = self.digest_to_urls[digest]

        raise NotImplementedError()
        # index = int(request.match_info["index"])
        # if topic_name not in self._oqs:
        #     raise web.HTTPNotFound()
        # oq = self._oqs[topic_name]
        # data = oq.get(index)
        # headers[HEADER_SEE_EVENTS] = f"../events/"
        # headers.update(self.get_headers_alternatives(request))
        # self._add_own_headers(headers)
        # return web.Response(body=data.content, headers=headers, content_type=data.content_type)

    @async_error_catcher
    async def serve_events(self, request: web.Request) -> web.WebSocketResponse:
        topic_name = request.match_info["topic"]
        if topic_name not in self._oqs:
            headers = CIMultiDict()

            self._add_own_headers(headers)
            raise web.HTTPNotFound(headers=headers)

        tr = self._oqs[topic_name].tr

        ws = web.WebSocketResponse()
        ws.headers.update(self.get_headers_alternatives(request))
        self._add_own_headers(ws.headers)
        await ws.prepare(request)

        if topic_name in self.forward_events:
            use_forward_url = self.forward_events[topic_name]
            await self.serve_events_forwarder(ws, use_forward_url)
            return ws

        exit_event = asyncio.Event()

        async def send_message(_: ObjectQueue, i: int) -> None:
            mdata = oq_._sequence[i]
            data = DataReady(sequence=i, digest=mdata.digest, urls=[URLString(f"../data/{i}")])
            if ws.closed:
                exit_event.set()
                return
            try:
                await ws.send_json(asdict(data))
            except ConnectionResetError:
                exit_event.set()
                pass

        oq_ = self._oqs[topic_name]
        s = oq_.subscribe(send_message)
        last = oq_.last()
        data_last = DataReady(last.index, digest=last.digest, urls=[URLString(f"../data/{last.index}")])
        await ws.send_json(asdict(data_last))

        try:
            await exit_event.wait()
            await ws.close()
        finally:
            oq_.unsubscribe(s)

        return ws

    @async_error_catcher
    async def serve_events_forwarder(self, ws: web.WebSocketResponse, url: URL) -> None:
        assert isinstance(url, URL)
        while True:
            if ws.closed:
                break

            try:
                await self.serve_events_forwarder_one(ws, url)
            except Exception as e:
                logger.error(f"Exception in serve_events_forwarder_one: {traceback.format_exc()}")
                await asyncio.sleep(1)

    @async_error_catcher
    async def serve_events_forwarder_one(self, ws: web.WebSocketResponse, url: URL) -> None:
        assert isinstance(url, URL)
        from .client import DTPSClient

        dr: DataReady
        async with DTPSClient.create() as client:
            async for dr in client.listen_url_events(url):
                urls = [join(url, _) for _ in dr.urls]

                self.digest_to_urls[dr.digest] = urls
                # first , join the data
                digesturl = URLString(f"/data/{dr.digest}/")

                dr2 = DataReady(
                    sequence=dr.sequence,
                    digest=dr.digest,
                    urls=[url_to_string(_) for _ in urls] + [digesturl],
                )
                logger.info(f"Forwarding {dr} -> {dr2}")
                await ws.send_json(asdict(dr2))
