"""Ergo use."""

__all__ = ["ConnectionInterface", "ContextManagerUse"]

import asyncio
import time
import traceback
import typing
from asyncio import CancelledError, Event, Queue, QueueEmpty, QueueFull, Task
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from typing import Any, TypeVar

import cbor2
from aiohttp import ClientResponseError, ServerDisconnectedError
from typing_extensions import ParamSpec

import dtps_http
from dtps import logger
from dtps.config import ContextInfo, ContextManager
from dtps.ergo_abstract import (
    AbstractDTPSContext,
    AbstractHistoryInterface,
    AbstractPublisherInterface,
    AbstractSubscriptionInterface,
    ContextConfig,
    ListenerInfo,
    PatchType,
    RPCFunction,
    ServeFunction,
)
from dtps_http import (
    CONTENT_TYPE_PATCH_CBOR,
    DEFAULT_CALLBACK_QUEUE_SIZE,
    MIME_OCTET,
    URL,
    AbstractListenDataInterface,
    Bounds,
    ConnectionJob,
    ContentInfo,
    DTPSClient,
    FinishedMessage,
    FoundMetadata,
    NodeID,
    NoSuchTopicError,
    RawData,
    TopicNameV,
    TopicOriginUnavailableError,
    TopicProperties,
    TopicRefAdd,
    URLIndexer,
    URLString,
)
from dtps_http.exceptions import DTPSError

STATUS_ERROR = 404
WARN_USE_PUBLISH_CONTEXT_FREQUENCY = 0.5
WARN_USE_PUBLISH_CONTEXT_HORIZON = 10
WARN_USE_PUBLISH_CONTEXT_MINIMUM = 4

PS = ParamSpec("PS")
X = TypeVar("X")


class CannotConnectToAnyURLError(Exception):
    pass


class ConnectionInterface:
    """Connection interface."""

    connection_name: TopicNameV
    master: "ContextManagerUse"
    url: URLIndexer

    def __init__(
        self,
        master: "ContextManagerUse",
        url: URLIndexer,
        connection_name: TopicNameV,
    ) -> None:
        """Initialize connection interface."""
        self.master = master
        self.url = url
        self.connection_name = connection_name

    async def disconnect(self) -> None:
        """Disconnect."""
        await self.master.client.disconnect(self.url, self.connection_name)
        raise NotImplementedError


class ContextManagerUse(ContextManager):
    """Context manager user."""

    last_connection: "CurrentConnection | None"
    client: DTPSClient
    contexts: (
        "dict[tuple[tuple[str, ...], ContextConfig], ContextManagerUseContext]"
    )
    tasks: list[Task[Any]]

    def __init__(self, base_name: str, context_info: ContextInfo) -> None:
        """Initialize context manager user."""
        self.client = DTPSClient(nickname=base_name, shutdown_event=None)
        self.context_info = context_info
        self.contexts = {}
        self.base_name = base_name
        self.base_config = ContextConfig.default()
        if self.context_info.is_create():
            raise DTPSError
        self.last_connection = None
        self.tasks = []

    def remember_task(self, task: Task[Any]) -> None:
        """Remember task."""
        self.tasks.append(task)

    async def get_current_connection(self) -> "CurrentConnection":
        """Return current connection."""
        if self.last_connection is not None:
            try:
                md = await self.client.get_metadata(self.last_connection.url)
            except ClientResponseError:
                pass
            else:
                self.last_connection.metadata = md
                return self.last_connection
        alternatives = []
        for url in self.context_info.urls:
            parsed_url = dtps_http.parse_url_unescape(url.url)
            url_indexer = typing.cast(URLIndexer, parsed_url)
            alternatives.append((url_indexer, None))
        best_url = await self.client.find_best_alternative(alternatives)
        if best_url is None:
            message = f"Could not connect to any of {alternatives}."
            raise CannotConnectToAnyURLError(message)
        metadata = await self.client.get_metadata(best_url)
        self.last_connection = CurrentConnection(
            url=best_url,
            metadata=metadata,
        )
        return self.last_connection

    async def get_all_urls(self) -> list[URLIndexer]:
        """Return URLs."""
        urls: list[URLIndexer] = []
        if self.last_connection is not None:
            urls.append(self.last_connection.url)
            extension = typing.cast(
                list[URLIndexer],
                self.last_connection.metadata.alternative_urls,
            )
            urls.extend(extension)
        extension = []
        for url in self.context_info.urls:
            parsed_url = dtps_http.parse_url_unescape(url.url)
            url_indexer = typing.cast(URLIndexer, parsed_url)
            extension.append(url_indexer)
        urls.extend(extension)
        url_set = set(urls)
        return sorted(url_set)

    async def get_best_url(self) -> URLIndexer:
        """Return the best url.

        Returns `best_url` if it is set, otherwise the best URL among
        the alternatives.
        """
        connection = await self.get_current_connection()
        return connection.url

    async def aclose(self) -> None:
        """Close asynchronously."""
        await self.client.aclose()
        for task in self.tasks:
            task.cancel()

    def get_context_by_components(
        self,
        components: tuple[str, ...],
        config: ContextConfig,
    ) -> AbstractDTPSContext:
        """Return context from components."""
        key = (components, config)
        if key not in self.contexts:
            merged = self.base_config.specialize(config)
            self.contexts[key] = ContextManagerUseContext(
                self,
                components,
                merged,
            )
        return self.contexts[key]

    def get_context(self) -> AbstractDTPSContext:
        """Return context."""
        return self.get_context_by_components((), self.base_config)


class ContextManagerUseContext(AbstractDTPSContext):
    components: tuple[str, ...]
    config: ContextConfig
    last_published: list[float]
    master: ContextManagerUse

    def __init__(
        self,
        master: ContextManagerUse,
        components: tuple[str, ...],
        config: ContextConfig,
    ) -> None:
        self.master = master
        self.components = components
        self.last_published = []
        self.config = config

    def __repr__(self) -> str:
        return f"AbstractDTPSContext({self.components!r}, {self.config!r})"

    def get_config(self) -> ContextConfig:
        return self.config

    def configure(
        self,
        context_configuration: ContextConfig,
        /,
    ) -> AbstractDTPSContext:
        merged = self.config.specialize(context_configuration)
        return self.master.get_context_by_components(self.components, merged)

    def _get_frequency_publishing(self) -> float:
        current_time = time.time()
        while (
            self.last_published[0]
            < current_time - WARN_USE_PUBLISH_CONTEXT_HORIZON
        ):
            self.last_published.pop(0)
        if not self.last_published:
            return 0
        last_published_length = len(self.last_published)
        return (
            self.last_published[-1] - self.last_published[0]
        ) / last_published_length

    @staticmethod
    def _get_on_finished(finished_event: Event) -> Any:
        async def on_finished(finished: FinishedMessage) -> None:
            logger.debug("_subscribe_patient_task: %s", finished)
            finished_event.set()

        return on_finished

    async def aclose(self) -> None:
        await self.master.aclose()

    async def get_urls(self) -> list[URLString]:
        topic = self.get_components_as_topic()
        relative_url = topic.as_relative_url()
        url_strings = []
        for url in await self.master.get_all_urls():
            new_url = dtps_http.join(url, relative_url)
            url_string = dtps_http.url_to_string(new_url)
            url_strings.append(url_string)
        return url_strings

    async def get_node_id(self) -> NodeID | None:
        return await self.patient(self.get_node_id_)

    def get_path_components(self) -> tuple[str, ...]:
        return self.components

    async def get_node_id_(self) -> NodeID | None:
        url = await self.get_best_url()
        md = await self.master.client.get_metadata(url)
        return md.origin_node

    async def exists(self) -> bool:
        return await self.patient(self.exists_)

    async def exists_(self) -> bool:
        url = await self.get_best_url()
        try:
            await self.master.client.get_metadata(url)
        except ClientResponseError as error:
            if error.status == STATUS_ERROR:
                return False
            raise
        return True

    async def patch(self, patch_data: list[dict[str, Any]], /) -> None:
        return await self.patient(self.patch_, patch_data)

    async def patch_(self, patch_data: list[dict[str, Any]], /) -> None:
        url = await self.get_best_url()
        data = cbor2.dumps(patch_data)
        await self.master.client.patch(url, CONTENT_TYPE_PATCH_CBOR, data)

    def get_components_as_topic(self) -> TopicNameV:
        return TopicNameV.from_components(self.components)

    def navigate(self, *components: str) -> AbstractDTPSContext:
        component_list: list[str] = []
        for component in components:
            split_component = component.split("/")
            component_list_extension = [
                component_ for component_ in split_component if component_
            ]
            component_list.extend(component_list_extension)
        new_components = self.components + tuple(component_list)
        return self.master.get_context_by_components(
            new_components,
            self.config,
        )

    def meta(self) -> AbstractDTPSContext:
        # TODO: actually we can do some error checks here
        return self / ":meta"

    async def list(self) -> list[str]:
        # TODO: DTSW-4801: implement list()
        raise NotImplementedError

    async def remove(self) -> None:
        return await self.patient(self.remove_)

    async def remove_(self) -> None:
        url = await self.get_best_url()
        return await self.master.client.delete(url)

    async def data_get(self) -> RawData:
        return await self.patient(self.data_get_)

    async def data_get_(self) -> RawData:
        url = await self.get_best_url()
        return await self.master.client.get(url, None)

    async def subscribe(
        self,
        on_data: Callable[[RawData], Awaitable[None]],
        /,
        max_frequency: float | None = None,
        queue_size: int = DEFAULT_CALLBACK_QUEUE_SIZE,
        *,
        inline: bool = True,
    ) -> AbstractSubscriptionInterface:
        if not self.config.patient:
            return await self.subscribe_once(
                on_data,
                max_frequency,
                inline=inline,
            )
        stop_event = Event()
        fake_subscription_interface = FakeSubscriptionInterface(stop_event)
        task = asyncio.create_task(
            self._subscribe_patient_task(
                fake_subscription_interface,
                on_data,
                max_frequency,
                queue_size,
                inline=inline,
            ),
        )
        self.master.remember_task(task)
        return fake_subscription_interface

    @staticmethod
    def _get_on_data(queue: Queue) -> Any:
        def on_data(data: RawData) -> None:
            try:
                queue.put_nowait(data)
            except QueueFull:
                with suppress(QueueEmpty):
                    queue.get_nowait()
                queue.put_nowait(data)

        return on_data

    def _get_processor(
        self,
        queue: Queue,
        on_data: Callable[[RawData], Awaitable[None]],
    ) -> Any:
        async def _processor() -> None:
            while True:
                data: RawData = await queue.get()
                # ==> this block runs user code, we need to catch
                # exceptions
                try:
                    await on_data(data)
                except Exception:
                    logger.exception(
                        "Exception in user callback for queue %s:",
                        self,
                    )
                    traceback.print_exc()
                # <== this block runs user code, we need to catch
                # exceptions

        return _processor

    @dtps_http.async_error_catcher
    async def _subscribe_patient_task(
        self,
        fake_subscription_interface: "FakeSubscriptionInterface",
        on_data: Callable[[RawData], Awaitable[None]],
        /,
        max_frequency: float | None = None,
        queue_size: int = DEFAULT_CALLBACK_QUEUE_SIZE,
        *,
        inline: bool = True,
    ) -> None:
        logger.debug("subscribe _subscribe_patient_task: starting")
        number_of_tries = 0
        number_of_successes = 0
        while True:
            logger.debug(
                "_subscribe_patient_task patient: loop %s %s",
                number_of_tries,
                number_of_successes,
            )
            try:
                finished_event = Event()
                on_finished = self._get_on_finished(finished_event)
                number_of_tries += 1
                subscription_interface = await self.subscribe_once(
                    on_data,
                    max_frequency,
                    on_finished,
                    queue_size,
                    inline=inline,
                )
                number_of_successes += 1
                fake_subscription_interface.real = subscription_interface
                logger.debug(
                    "_subscribe_patient_task: wait for finished_event",
                )
                await finished_event.wait()
                await asyncio.sleep(1)
                if fake_subscription_interface.unsubscribe_event.is_set():
                    break
            except CancelledError:
                raise
            except CannotConnectToAnyURLError:
                logger.debug(
                    "_subscribe_patient_task: cannot connect yet, retrying",
                )
                await asyncio.sleep(1)
            except Exception as exception:
                logger.exception(
                    "_subscribe_patient_task: Error in subscribe: %s",
                    exception,
                )
                await asyncio.sleep(1)

    async def subscribe_once(
        self,
        on_data: Callable[[RawData], Awaitable[None]],
        /,
        max_frequency: float | None = None,
        on_finished: Callable[[FinishedMessage], Awaitable[None]]
        | None = None,
        queue_size: int = DEFAULT_CALLBACK_QUEUE_SIZE,
        *,
        inline: bool = True,
    ) -> AbstractSubscriptionInterface:
        url = await self.get_best_url()
        queue: Queue = Queue(maxsize=queue_size)
        processor = self._get_processor(queue, on_data)
        coroutine = processor()
        loop = asyncio.get_event_loop()
        # create processor task
        asyncio.run_coroutine_threadsafe(coroutine, loop)
        on_data = self._get_on_data(queue)
        listen_data_interface = await self.master.client.listen_url(
            url,
            on_data,
            inline_data=inline,
            raise_on_error=True,
            max_frequency=max_frequency,
            on_finished=on_finished,
        )
        return ContextManagerUseSubscription(listen_data_interface)

    async def history(self) -> AbstractHistoryInterface | None:
        # TODO: DTSW-4803: [use] implement history
        raise NotImplementedError

    async def get_best_url(self) -> URL:
        """Return best url."""
        topic = self.get_components_as_topic()
        best_url = await self.master.get_best_url()
        relative_url = topic.as_relative_url()
        return dtps_http.join(best_url, relative_url)

    async def publish(self, data: RawData) -> None:
        current_time = time.time()
        self.last_published.append(current_time)
        freq = self._get_frequency_publishing()
        url = await self.get_best_url()
        enough = len(self.last_published) >= WARN_USE_PUBLISH_CONTEXT_MINIMUM
        if enough and (freq > WARN_USE_PUBLISH_CONTEXT_FREQUENCY):
            message = (
                "The publishing frequency for\n"
                "    %s\n"
                "is %.1f messages per second: consider using `publisher` to "
                "publish using websockets.",
                url,
                freq,
            )
            logger.warning(message)
        await self.master.client.publish(url, data)

    async def publisher(self) -> "ContextManagerUseContextPublisher":
        publisher = ContextManagerUseContextPublisher(self)
        await publisher.initialize()
        return publisher

    @asynccontextmanager
    async def publisher_context(
        self,
    ) -> AsyncIterator[AbstractPublisherInterface]:
        publisher = await self.publisher()
        try:
            yield publisher
        finally:
            await publisher.terminate()

    async def patient(
        self,
        f: Callable[PS, Awaitable[X]],
        *args: PS.args,
        **kwargs: PS.kwargs,
    ) -> X:
        if self.get_config().patient:
            return await self.patient_(f, *args, **kwargs)
        return await f(*args, **kwargs)

    async def patient_(
        self,
        f: Callable[PS, Awaitable[X]],
        *args: PS.args,
        **kwargs: PS.kwargs,
    ) -> X:
        while True:
            try:
                return await f(*args, **kwargs)
            except (CannotConnectToAnyURLError, ServerDisconnectedError):
                await asyncio.sleep(1)
                continue
            except Exception:
                logger.exception(
                    "Unexpected error in patient; will retry anyway",
                    exc_info=True,
                )
                await asyncio.sleep(1)
                continue

    async def call(self, data: RawData) -> RawData:
        return await self.patient(self.call_, data)

    async def call_(self, data: RawData) -> RawData:
        client = self.master.client
        url = await self.get_best_url()
        return await client.call(url, data)

    async def expose(
        self,
        urls: Sequence[str] | AbstractDTPSContext,
        /,
        *,
        mask_origin: bool = False,
    ) -> None:
        await self.patient(self.expose_, urls, mask_origin=mask_origin)

    async def expose_(
        self,
        context: AbstractDTPSContext | Sequence[str],
        /,
        *,
        mask_origin: bool = False,
    ) -> None:
        topic = self.get_components_as_topic()
        url0 = await self.master.get_best_url()
        if isinstance(context, AbstractDTPSContext):
            urls = await context.get_urls()
            node_id = await context.get_node_id()
        else:
            context_list = list(context)
            urls = typing.cast(list[URLString], context_list)
            node_id = None
        url_indexer = typing.cast(URLIndexer, url0)
        await self.master.client.add_proxy(
            url_indexer,
            topic,
            node_id,
            urls,
            mask_origin=mask_origin,
        )

    async def queue_create(
        self,
        *,
        transform: RPCFunction | None = None,
        serve: ServeFunction | None = None,
        bounds: Bounds | None = None,
        content_info: ContentInfo | None = None,
        topic_properties: TopicProperties | None = None,
        app_data: dict[str, bytes] | None = None,
    ) -> "ContextManagerUseContext":
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
        transform: RPCFunction | None = None,
        serve: ServeFunction | None = None,
        bounds: Bounds | None = None,
        content_info: ContentInfo | None = None,
        topic_properties: TopicProperties | None = None,
        app_data: dict[str, bytes] | None = None,
    ) -> "DTPSContext":
        topic = self.get_components_as_topic()
        url = await self.get_best_url()
        if transform is not None:
            message = "`transform` is not supported for remote queues."
            raise ValueError(message)
        if serve is not None:
            message = "`serve` is not supported for remote queues."
            raise ValueError(message)
        try:
            await self.master.client.get_metadata(url)
        except ClientResponseError:
            logger.debug("OK: queue_create: does not exist: %s", url)
            # TODO: check 404
        else:
            logger.debug(f"queue_create: already exists: {url}")
            return
        if bounds is None:
            bounds = Bounds.default()
        if content_info is None:
            content_info = ContentInfo.simple(MIME_OCTET)
        if topic_properties is None:
            topic_properties = TopicProperties.default()
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
        retry_every: float = 2,
        retry_max: int | None = None,
        timeout: float | None = None,
        print_every: float = 10,
        *,
        quiet: bool = False,
    ) -> None:
        current_time = time.time()
        number_of_tries = 0
        printed_last = current_time
        while True:
            # check timeout
            if timeout is not None and time.time() - current_time > timeout:
                topic = self.get_components_as_topic()
                message = f"Timeout waiting for {topic}"
                raise TimeoutError(message)
            # check max tries
            if retry_max is not None and number_of_tries >= retry_max:
                topic = self.get_components_as_topic()
                message = f"Max tries reached waiting for {topic}."
                raise TimeoutError(message)
            # perform GET
            try:
                await self.data_get()
            except CancelledError:
                raise
            except (
                TimeoutError,
                NoSuchTopicError,
                TopicOriginUnavailableError,
                CannotConnectToAnyURLError,
            ):
                if not quiet and time.time() - printed_last > print_every:
                    waited = time.time() - current_time
                    topic = self.get_components_as_topic()
                    logger.warning(
                        "I have been waiting for %s for %.0fs",
                        topic,
                        waited,
                    )
                    printed_last = time.time()
                # wait and retry
                await asyncio.sleep(retry_every)
                number_of_tries += 1
                continue
            except Exception as error:
                logger.exception(
                    "Unexpected error %s in `until_ready`. Continuing anyway.",
                    error.__class__.__name__,
                    exc_info=True,
                )
                raise

    async def connect_to(
        self,
        context: AbstractDTPSContext,
        /,
    ) -> ConnectionInterface:
        return await self.patient(self.connect_to_, context)

    async def connect_to_(
        self,
        context: AbstractDTPSContext,
        /,
    ) -> ConnectionInterface:
        # TODO: DTSW-4805: [use] implement connect_to
        if not isinstance(context, ContextManagerUseContext):
            context_class = type(context)
            message = (
                f"Expected `ContextManagerUseContext`, got `{context_class}`."
            )
            raise TypeError(message)
        topic1 = self.get_components_as_topic()
        topic2 = context.get_components_as_topic()
        url = await self.master.get_best_url()
        connection_job = ConnectionJob(
            source=topic1,
            target=topic2,
            service_mode="AllMessages",
        )
        name = topic1 + topic2
        await self.master.client.connect(url, name, connection_job)
        return ConnectionInterface(self.master, url, name)

    async def subscribe_diff(
        self,
        on_data: Callable[[PatchType], Awaitable[None]],
        /,
    ) -> AbstractSubscriptionInterface:
        message = "`subscribe_diff` is not supported for remote contexts yet."
        raise NotImplementedError(message)


class ContextManagerUseContextPublisher(AbstractPublisherInterface):
    master: ContextManagerUseContext
    queue_in: Queue[RawData]
    queue_out: Queue[bool]
    task_push: Task[Any]

    def __init__(self, master: ContextManagerUseContext) -> None:
        self.master = master
        self.queue_in = Queue()
        self.queue_out = Queue()

    async def initialize(self) -> None:
        url_topic = await self.master.get_best_url()
        self.task_push = await self.master.master.client.push_continuous(
            url_topic,
            queue_in=self.queue_in,
            queue_out=self.queue_out,
        )

    async def publish(self, raw_data: RawData, /) -> None:
        await self.queue_in.put(raw_data)
        success = await self.queue_out.get()
        if not success:
            short_description = raw_data.short_description()
            message = f"Could not push {short_description}."
            raise DTPSError(message)

    async def terminate(self) -> None:
        self.task_push.cancel()

    async def get_listener_info(self) -> ListenerInfo | None:
        # Not available for remote contexts
        return None


class ContextManagerUseSubscription(AbstractSubscriptionInterface):
    def __init__(
        self,
        listen_data_interface: AbstractListenDataInterface,
    ) -> None:
        self.listen_data_interface = listen_data_interface

    async def unsubscribe(self) -> None:
        await self.listen_data_interface.stop()


@dataclass
class CurrentConnection:
    url: URLIndexer
    metadata: FoundMetadata


class FakeSubscriptionInterface(AbstractSubscriptionInterface):
    real: AbstractSubscriptionInterface | None

    def __init__(self, event: Event) -> None:
        self.real = None
        self.unsubscribe_event = event

    async def unsubscribe(self) -> None:
        self.unsubscribe_event.set()
        if self.real is not None:
            await self.real.unsubscribe()
