import asyncio
import time
from asyncio import Event
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Sequence,
    cast,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
)

import cbor2
from aiohttp import ClientResponseError
from typing_extensions import ParamSpec

from dtps_http import (
    async_error_catcher,
    Bounds,
    ConnectionJob,
    CONTENT_TYPE_PATCH_CBOR,
    ContentInfo,
    DTPSClient,
    FinishedMsg,
    FoundMetadata,
    join,
    ListenDataInterface,
    MIME_OCTET,
    NodeID,
    NoSuchTopic,
    parse_url_unescape,
    RawData,
    TopicNameV,
    TopicOriginUnavailable,
    TopicProperties,
    TopicRefAdd,
    URL,
    url_to_string,
    URLIndexer,
    URLString,
)
from . import logger
from .config import ContextInfo, ContextManager
from .ergo_ui import (
    ConnectionInterface,
    ContextConfig,
    DTPSContext,
    HistoryInterface,
    ListenerInfo,
    PatchType,
    PublisherInterface,
    RPCFunction,
    ServeFunction,
    SubscriptionInterface,
)

PS = ParamSpec("PS")

X = TypeVar("X")

__all__ = [
    "ContextManagerUse",
]


class CannotConnectToAnyURL(Exception):
    pass


@dataclass
class CurrentConnection:
    url: URLIndexer
    metadata: FoundMetadata


class ContextManagerUse(ContextManager):
    # best_url: URLIndexer
    # all_urls: List[URL]
    last_connection: Optional[CurrentConnection]
    client: DTPSClient
    contexts: "Dict[Tuple[Tuple[str, ...], ContextConfig], ContextManagerUseContext]"
    tasks: List["asyncio.Task[Any]"]

    def __init__(self, base_name: str, context_info: "ContextInfo"):
        self.client = DTPSClient(nickname=base_name, shutdown_event=None)
        self.context_info = context_info
        self.contexts = {}
        self.base_name = base_name
        self.base_config = ContextConfig.default()
        assert not self.context_info.is_create()
        self.last_connection = None
        self.tasks = []

    def remember_task(self, task: "asyncio.Task[Any]") -> None:
        self.tasks.append(task)

    async def init(self) -> None:
        await self.client.init()
        # alternatives = [(cast(URLIndexer, parse_url_unescape(_.url)), None) for _ in self.context_info.urls]
        # best_url = await self.client.find_best_alternative(alternatives)
        #
        # self.all_urls = [u for (u, _) in alternatives]
        # if best_url is None:
        #     msg = f"Could not connect to any of {alternatives}"
        #     raise ValueError(msg)
        #
        # self.best_url = best_url

    async def get_current_connection(self) -> CurrentConnection:
        if self.last_connection is not None:
            try:
                md = await self.client.get_metadata(self.last_connection.url)

            except ClientResponseError:
                pass
            else:
                self.last_connection.metadata = md

                return self.last_connection

        alternatives = [(cast(URLIndexer, parse_url_unescape(_.url)), None) for _ in self.context_info.urls]
        best_url = await self.client.find_best_alternative(alternatives)
        if best_url is None:
            msg = f"Could not connect to any of {alternatives}"
            raise CannotConnectToAnyURL(msg)
        metadata = await self.client.get_metadata(best_url)
        self.last_connection = CurrentConnection(url=best_url, metadata=metadata)
        return self.last_connection

    async def get_all_urls(self) -> List[URLIndexer]:
        urls: List[URLIndexer] = []
        if self.last_connection is not None:
            urls.append(self.last_connection.url)
            urls.extend(cast(List[URLIndexer], self.last_connection.metadata.alternative_urls))
        urls.extend([cast(URLIndexer, parse_url_unescape(_.url)) for _ in self.context_info.urls])
        return sorted(set(urls))

    async def get_best_url(self) -> URLIndexer:
        """
        Get the best url to which to reach this,
        or raises CannotConnectToAnyURL.

        It first tries to use the best_url if it is already set.
        Otherwise, it tries to find the best url among the alternatives.

        """
        connection = await self.get_current_connection()
        return connection.url
        #
        # if self.best_url is not None:
        #     # check if it still works
        #     try:
        #         await self.client.get_metadata(self.best_url)
        #         return self.best_url
        #     except ClientResponseError:
        #         pass
        #
        # alternatives = [(cast(URLIndexer, parse_url_unescape(_.url)), None) for _ in self.context_info.urls]
        # best_url = await self.client.find_best_alternative(alternatives)
        # if best_url is None:
        #     msg = f"Could not connect to any of {alternatives}"
        #     raise CannotConnectToAnyURL(msg)
        # self.best_url = best_url
        # metadata = await self.client.get_metadata(best_url)
        # return best_url

    async def aclose(self) -> None:
        await self.client.aclose()
        for t in self.tasks:
            t.cancel()

    def get_context_by_components(self, components: Tuple[str, ...], config: ContextConfig) -> "DTPSContext":
        key = (components, config)
        if key not in self.contexts:
            merged = self.base_config.specialize(config)
            self.contexts[key] = ContextManagerUseContext(self, components, merged)

        return self.contexts[key]

    def get_context(self) -> "DTPSContext":
        return self.get_context_by_components((), self.base_config)


class ContextManagerUseContextPublisher(PublisherInterface):
    queue_in: "asyncio.Queue[RawData]"
    queue_out: "asyncio.Queue[bool]"
    task_push: "asyncio.Task[Any]"

    def __init__(self, master: "ContextManagerUseContext"):
        self.master = master

        self.queue_in = asyncio.Queue()
        self.queue_out = asyncio.Queue()

    async def init(self) -> None:
        url_topic = await self.master._get_best_url()
        self.task_push = await self.master.master.client.push_continuous(
            url_topic, queue_in=self.queue_in, queue_out=self.queue_out
        )

    async def publish(self, rd: RawData, /) -> None:
        await self.queue_in.put(rd)
        success = await self.queue_out.get()
        if not success:
            raise Exception(f"Could not push {rd.short_description()}")

    async def terminate(self) -> None:
        self.task_push.cancel()

    async def get_listener_info(self) -> Optional[ListenerInfo]:
        # Not available for remote contexts
        return None


class ContextManagerUseSubscription(SubscriptionInterface):
    def __init__(self, ldi: ListenDataInterface):
        self.ldi = ldi

    async def unsubscribe(self) -> None:
        await self.ldi.stop()


# min frequency to warn for
WARN_USE_PUBLISH_CONTEXT_N_per_S = 0.5
WARN_USE_PUBLISH_CONTEXT_HORIZON_S = 10.0
WARN_USE_PUBLISH_CONTEXT_N_MIN = 4


class FakeSubscriptionInterface(SubscriptionInterface):
    real: Optional[SubscriptionInterface]

    def __init__(self, event: Event):
        self.real = None
        self.unsubscribe_event = event

    async def unsubscribe(self) -> None:
        self.unsubscribe_event.set()
        if self.real is not None:
            await self.real.unsubscribe()


class ContextManagerUseContext(DTPSContext):
    master: ContextManagerUse
    config: ContextConfig
    components: Tuple[str, ...]
    last_published: List[float]

    def __init__(self, master: ContextManagerUse, components: Tuple[str, ...], config: ContextConfig):
        self.master = master
        self.components = components

        self.last_published = []
        self.config = config

    def __repr__(self) -> str:
        return f"DTPSContext({self.components!r}, {self.config!r})"

    def get_config(self) -> ContextConfig:
        return self.config

    def configure(self, cc: ContextConfig, /) -> "DTPSContext":
        merged = self.config.specialize(cc)
        return self.master.get_context_by_components(self.components, merged)

    def _get_frequency_publishing(self) -> float:
        now = time.time()
        while self.last_published[0] < now - WARN_USE_PUBLISH_CONTEXT_HORIZON_S:
            self.last_published.pop(0)
        if not self.last_published:
            return 0.0
        t0 = self.last_published[0]
        t1 = self.last_published[-1]
        n = len(self.last_published)
        freq = (t1 - t0) / n
        return freq

    async def aclose(self) -> None:
        await self.master.aclose()

    async def get_urls(self) -> List[URLString]:
        all_urls = await self.master.get_all_urls()
        rurl = self._get_components_as_topic().as_relative_url()
        return [url_to_string(join(u, rurl)) for u in all_urls]

    async def get_node_id(self) -> Optional[NodeID]:
        return await self.patient(self.get_node_id_)

    async def get_node_id_(self) -> Optional[NodeID]:
        url = await self._get_best_url()
        md = await self.master.client.get_metadata(url)
        return md.origin_node

    async def exists(self) -> bool:
        return await self.patient(self.exists_)

    async def exists_(self) -> bool:
        url = await self._get_best_url()
        client = self.master.client
        try:
            await client.get_metadata(url)
            return True
        except ClientResponseError as e:
            if e.status == 404:
                # logger.debug(f"exists: {url} -> 404 -> {e}")
                return False
            else:
                raise

    async def patch(self, patch_data: List[Dict[str, Any]], /) -> None:
        return await self.patient(self.patch_, patch_data)

    async def patch_(self, patch_data: List[Dict[str, Any]], /) -> None:
        url = await self._get_best_url()
        data = cbor2.dumps(patch_data)
        res = await self.master.client.patch(url, CONTENT_TYPE_PATCH_CBOR, data)

    def _get_components_as_topic(self) -> TopicNameV:
        return TopicNameV.from_components(self.components)

    def navigate(self, *components: str) -> "DTPSContext":
        c: list[str] = []
        for comp in components:
            c.extend([_ for _ in comp.split("/") if _])
        return self.master.get_context_by_components(self.components + tuple(c), self.config)

    def meta(self) -> "DTPSContext":
        return self / ":meta"  # TODO: actually we can do some error checks here

    async def list(self) -> List[str]:
        # TODO: DTSW-4801: implement list()
        raise NotImplementedError()

    async def remove(self) -> None:
        return await self.patient(self.remove_)

    async def remove_(self) -> None:
        url = await self._get_best_url()
        return await self.master.client.delete(url)

    async def data_get(self) -> RawData:
        return await self.patient(self.data_get_)

    async def data_get_(self) -> RawData:
        url = await self._get_best_url()
        return await self.master.client.get(url, None)

    async def subscribe(
        self,
        on_data: Callable[[RawData], Awaitable[None]],
        /,
        max_frequency: Optional[float] = None,
        inline: bool = True,
    ) -> "SubscriptionInterface":
        if not self.config.patient:
            return await self.subscribe_once(on_data, max_frequency, inline)

        # first let's get the data once

        stop_event = Event()
        fldi = FakeSubscriptionInterface(stop_event)

        task = asyncio.create_task(self._subscribe_patient_task(fldi, on_data, max_frequency, inline))
        self.master.remember_task(task)
        return fldi

    @async_error_catcher
    async def _subscribe_patient_task(
        self,
        fldi: FakeSubscriptionInterface,
        on_data: Callable[[RawData], Awaitable[None]],
        /,
        max_frequency: Optional[float] = None,
        inline: bool = True,
    ) -> None:

        logger.debug(f"subscribe _subscribe_patient_task: starting")
        ntries = 0
        nsuccess = 0
        while True:
            logger.debug(f"_subscribe_patient_task patient: loop {ntries=} {nsuccess=}")
            try:
                finished_event = Event()

                async def on_finished(finished: FinishedMsg) -> None:
                    logger.debug(f"_subscribe_patient_task: {finished}")
                    finished_event.set()

                ntries += 1
                si = await self.subscribe_once(on_data, max_frequency, inline, on_finished=on_finished)
                nsuccess += 1
                fldi.real = si
                logger.debug(f"_subscribe_patient_task: wait for finished_event")
                await finished_event.wait()
                await asyncio.sleep(1)
                if fldi.unsubscribe_event.is_set():
                    break
                # await fldi.unsubscribe_event.wait()
            except asyncio.CancelledError:
                raise
            except CannotConnectToAnyURL:
                logger.debug(f"_subscribe_patient_task: cannot connect yet, retrying")
                await asyncio.sleep(1)
            except Exception as e:  # ok but which error?
                logger.error(f"_subscribe_patient_task: Error in subscribe: {e}")
                await asyncio.sleep(1)

    async def subscribe_once(
        self,
        on_data: Callable[[RawData], Awaitable[None]],
        /,
        max_frequency: Optional[float] = None,
        inline: bool = True,
        on_finished: Optional[Callable[[FinishedMsg], Awaitable[None]]] = None,
    ) -> "SubscriptionInterface":
        url = await self._get_best_url()
        ldi = await self.master.client.listen_url(
            url,
            on_data,
            inline_data=inline,
            raise_on_error=True,
            max_frequency=max_frequency,
            on_finished=on_finished,
        )
        # logger.debug(f"subscribed to {url} -> {t}")
        return ContextManagerUseSubscription(ldi)

    async def history(self) -> "Optional[HistoryInterface]":
        # TODO: DTSW-4803: [use] implement history
        raise NotImplementedError()

    async def _get_best_url(self) -> URL:
        """Raises CannotConnectToAnyURL"""
        topic = self._get_components_as_topic()
        best_url = await self.master.get_best_url()
        url = join(best_url, topic.as_relative_url())
        return url

    async def publish(self, data: RawData) -> None:
        self.last_published.append(time.time())
        freq = self._get_frequency_publishing()
        url = await self._get_best_url()
        enough = len(self.last_published) >= WARN_USE_PUBLISH_CONTEXT_N_MIN

        if enough and (freq > WARN_USE_PUBLISH_CONTEXT_N_per_S):
            msg = (
                f"The publishing frequency for\n    {url}\nis {freq:.1f} messages per second:"
                "consider using publisher() to publish using websockets"
            )
            logger.warn(msg)
        await self.master.client.publish(url, data)

    async def publisher(self) -> "ContextManagerUseContextPublisher":
        publisher = ContextManagerUseContextPublisher(self)
        await publisher.init()
        return publisher

    @asynccontextmanager
    async def publisher_context(self) -> AsyncIterator["PublisherInterface"]:
        publisher = await self.publisher()
        try:
            yield publisher
        finally:
            await publisher.terminate()

    async def patient(self, f: Callable[PS, Awaitable[X]], *args: PS.args, **kwargs: PS.kwargs) -> X:
        if self.get_config().patient:
            return await self.patient_(f, *args, **kwargs)
        else:
            return await f(*args, **kwargs)

    async def patient_(self, f: Callable[PS, Awaitable[X]], *args: PS.args, **kwargs: PS.kwargs) -> X:
        while True:
            try:
                return await f(*args, **kwargs)
            except CannotConnectToAnyURL as e:
                await asyncio.sleep(1)
                continue

    async def call(self, data: RawData) -> RawData:
        return await self.patient(self.call_, data)

    async def call_(self, data: RawData) -> RawData:
        client = self.master.client
        url = await self._get_best_url()
        return await client.call(url, data)

    async def expose(
        self, urls: "Sequence[str] | DTPSContext", /, *, mask_origin: bool = False
    ) -> "DTPSContext":
        return await self.patient(self.expose_, urls, mask_origin=mask_origin)

    async def expose_(
        self, c: "DTPSContext | Sequence[str]", /, *, mask_origin: bool = False
    ) -> "DTPSContext":
        topic = self._get_components_as_topic()
        url0 = await self.master.get_best_url()
        if isinstance(c, DTPSContext):
            urls = await c.get_urls()
            node_id = await c.get_node_id()
        else:
            urls = cast(List[URLString], list(c))
            node_id = None
        await self.master.client.add_proxy(
            cast(URLIndexer, url0), topic, node_id, urls, mask_origin=mask_origin
        )
        return self

    async def queue_create(
        self,
        *,
        transform: Optional[RPCFunction] = None,
        serve: Optional[ServeFunction] = None,
        bounds: Optional[Bounds] = None,
        content_info: Optional[ContentInfo] = None,
        topic_properties: Optional[TopicProperties] = None,
        app_data: Optional[Dict[str, Any]] = None,
    ) -> "DTPSContext":
        return await self.patient(
            self.queue_create_,
            transform=transform,
            serve=serve,
            bounds=bounds,
            content_info=content_info,
            topic_properties=topic_properties,
            app_data=app_data,
        )

    async def queue_create_(
        self,
        *,
        transform: Optional[RPCFunction] = None,
        serve: Optional[ServeFunction] = None,
        bounds: Optional[Bounds] = None,
        content_info: Optional[ContentInfo] = None,
        topic_properties: Optional[TopicProperties] = None,
        app_data: Optional[Dict[str, Any]] = None,
    ) -> "DTPSContext":
        topic = self._get_components_as_topic()

        url = await self._get_best_url()

        if transform is not None:
            msg = "transform is not supported for remote queues"
            raise ValueError(msg)

        if serve is not None:
            msg = "serve is not supported for remote queues"
            raise ValueError(msg)

        try:
            md = await self.master.client.get_metadata(url)
        except ClientResponseError:
            logger.debug("OK: queue_create: does not exist: %s", url)
            # TODO: check 404
            pass
        else:
            logger.debug(f"queue_create: already exists: {url}")
            return self

        if bounds is None:
            bounds = Bounds.default()

        if content_info is None:
            content_info = ContentInfo.simple(MIME_OCTET)

        if topic_properties is None:
            topic_properties = TopicProperties.rw_pushable()

        if app_data is None:
            app_data = {}

        parameters = TopicRefAdd(
            content_info=content_info,
            properties=topic_properties,
            app_data=app_data,
            bounds=bounds,
        )
        best_url = await self.master.get_best_url()
        await self.master.client.add_topic(best_url, topic, parameters)
        return self

    async def until_ready(
        self,
        retry_every: float = 2.0,
        retry_max: Optional[int] = None,
        timeout: Optional[float] = None,
        print_every: float = 10.0,
        quiet: bool = False,
    ) -> "DTPSContext":
        stime: float = time.time()
        num_tries: int = 0
        printed_last: float = time.time()
        while True:
            # check timeout
            if timeout is not None and time.time() - stime > timeout:
                msg = f"Timeout waiting for {self._get_components_as_topic()}"
                raise TimeoutError(msg)
            # check max tries
            if retry_max is not None and num_tries >= retry_max:
                msg = f"Max tries reached waiting for {self._get_components_as_topic()}"
                raise TimeoutError(msg)
            # perform GET
            try:
                await self.data_get()
                return self
            except (asyncio.TimeoutError, NoSuchTopic, TopicOriginUnavailable):
                if not quiet and time.time() - printed_last > print_every:
                    waited: float = time.time() - stime
                    logger.warning(
                        f"I have been waiting for {self._get_components_as_topic()} for {waited:.0f}s"
                    )
                    printed_last = time.time()
                # wait and retry
                await asyncio.sleep(retry_every)
                num_tries += 1
                continue
        return self

    async def connect_to(self, context: "DTPSContext", /) -> "ConnectionInterface":
        return await self.patient(self.connect_to_, context)

    async def connect_to_(self, c: "DTPSContext", /) -> "ConnectionInterface":
        # TODO: DTSW-4805: [use] implement connect_to

        if not isinstance(c, ContextManagerUseContext):
            raise TypeError(f"Expected ContextManagerUseContext, got {type(c)}")

        topic1 = self._get_components_as_topic()
        topic2 = c._get_components_as_topic()

        url = await self.master.get_best_url()

        connection_job = ConnectionJob(source=topic1, target=topic2, service_mode="AllMessages")
        name = topic1 + topic2
        await self.master.client.connect(url, name, connection_job)

        return ConnectionInterfaceImpl(self.master, url, name)

    async def subscribe_diff(
        self, on_data: Callable[[PatchType], Awaitable[None]], /
    ) -> "SubscriptionInterface":
        msg = "subscribe_diff is not supported for remote contexts yet"
        raise NotImplementedError(msg)
        a: SubscriptionInterface
        return a


class ConnectionInterfaceImpl(ConnectionInterface):
    def __init__(self, master: ContextManagerUse, url: URLIndexer, connection_name: TopicNameV):
        self.master = master
        self.url = url

        self.connection_name = connection_name

    async def disconnect(self) -> None:
        await self.master.client.disconnect(self.url, self.connection_name)

        raise NotImplementedError()
        pass
