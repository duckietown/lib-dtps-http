# ruff: noqa: INP001
"""Compare local DTPS direct HTTP, WebSocket-publisher, and SHM delivery.

The direct HTTP path calls ``DTPSContext.publish()``, which sends each message
as an HTTP POST over a Unix-domain socket. The WebSocket path creates a
``DTPSContext.publisher()`` and publishes through its persistent WebSocket.
Both normal paths receive inline events through a WebSocket subscription. The
SHM path uses the same remote publish and subscribe APIs with ``shm_only=True``.
Each publication waits for its matching callback, so the reported rate
is confirmed delivery rate rather than producer-only throughput.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import struct
import tempfile
import time
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, cast

from dtps import context_cleanup
from dtps_http import (
    MIME_OCTET,
    Bounds,
    RawData,
    make_http_unix_url,
    parse_url_unescape,
    url_to_string,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from dtps.ergo_ui import DTPSContext, PublisherInterface


LOGGER = logging.getLogger(__name__)
_MESSAGE_HEADER = struct.Struct("!QQ")
_DEFAULT_PAYLOAD_SIZES = (1024, 65536, 1_000_000)
_DEFAULT_SAMPLES = 100
_DEFAULT_WARMUP = 20
_DEFAULT_TIMEOUT_SECONDS = 5.0
_HTTP_REQUEST_BODY_LIMIT_BYTES = 1024 * 1024
_HTTP_DIRECT_TRANSPORT = "http-direct"
_HTTP_DIRECT_TRANSPORT_LABEL = "HTTP direct publish"
_WEBSOCKET_PUBLISHER_TRANSPORT = "websocket-publisher"
_WEBSOCKET_PUBLISHER_TRANSPORT_LABEL = "WebSocket publisher"
_SHM_TRANSPORT = "shm"
_EMPTY_PERCENTILE_VALUES_ERROR = (
    "Cannot calculate a percentile from no values."
)
_EMPTY_TIMING_VALUES_ERROR = "Cannot summarize no timing samples."
_MEASUREMENT_INTERVAL_ERROR = (
    "Benchmark measurement interval was not positive."
)
_SAMPLES_ERROR = "--samples must be positive."
_WARMUP_ERROR = "--warmup cannot be negative."
_TIMEOUT_ERROR = "--timeout must be positive."


class BenchmarkError(ValueError):
    """Raised when benchmark configuration or delivery is invalid."""


@dataclass(frozen=True)
class Delivery:
    """One subscriber callback annotated with its publisher metadata."""

    sequence: int
    published_at_ns: int
    received_at_ns: int


@dataclass(frozen=True)
class MetricSummary:
    """A latency summary expressed in microseconds."""

    count: int
    mean_us: float
    p50_us: float
    p95_us: float
    min_us: float
    max_us: float


@dataclass(frozen=True)
class TransportResult:
    """Measured delivery metrics for one payload size and transport."""

    transport: str
    payload_size_bytes: int
    samples: int
    confirmed_messages_per_second: float
    publish_to_callback_latency_us: MetricSummary
    publish_duration_us: MetricSummary


@dataclass(frozen=True)
class BenchmarkReport:
    """The portable JSON representation of one benchmark invocation."""

    transport_paths: dict[str, str]
    configuration: BenchmarkConfiguration
    notes: list[str]
    results: list[TransportResult]


@dataclass(frozen=True)
class BenchmarkConfiguration:
    """Shared options for a benchmark measurement run."""

    samples: int
    warmup: int
    timeout_seconds: float


@dataclass(frozen=True)
class BenchmarkContexts:
    """The DTPS contexts and channel location used by one run."""

    create: DTPSContext
    use: DTPSContext
    channel_directory: Path
    configuration: BenchmarkConfiguration


@dataclass(frozen=True)
class TransportEndpoints:
    """One transport's publication endpoint and optional SHM channel."""

    use_topic: DTPSContext
    channel_path: str
    transport: str
    publisher: Optional[PublisherInterface]


def _percentile(values: Sequence[float], percentile: float) -> float:
    """Return a linearly interpolated percentile."""
    if not values:
        raise BenchmarkError(_EMPTY_PERCENTILE_VALUES_ERROR)
    ordered_values = sorted(values)
    last_index = len(ordered_values) - 1
    position = last_index * percentile
    lower_index = int(position)
    upper_index = min(lower_index + 1, last_index)
    fraction = position - lower_index
    lower_value = ordered_values[lower_index]
    upper_value = ordered_values[upper_index]
    return lower_value + ((upper_value - lower_value) * fraction)


def _summarize_ns(values: Sequence[int]) -> MetricSummary:
    """Convert nanosecond samples into a microsecond summary."""
    if not values:
        raise BenchmarkError(_EMPTY_TIMING_VALUES_ERROR)
    values_us = [value / 1000.0 for value in values]
    count = len(values_us)
    return MetricSummary(
        count=count,
        mean_us=sum(values_us) / count,
        p50_us=_percentile(values_us, 0.50),
        p95_us=_percentile(values_us, 0.95),
        min_us=min(values_us),
        max_us=max(values_us),
    )


def _make_payload_buffer(payload_size: int) -> bytearray:
    """Return a payload buffer with space for metadata."""
    if payload_size < _MESSAGE_HEADER.size:
        message = (
            f"Payload size must be at least {_MESSAGE_HEADER.size} bytes, "
            f"not {payload_size}."
        )
        raise BenchmarkError(message)
    return bytearray(payload_size)


def _set_owner_only_permissions(path: Path, mode: int) -> None:
    """Set a benchmark-owned path to its intended owner-only mode."""
    path.chmod(mode)


def _validate_http_payload_size(payload_size: int) -> None:
    """Reject payloads that the direct HTTP path cannot publish."""
    if payload_size < _HTTP_REQUEST_BODY_LIMIT_BYTES:
        return
    message = (
        "Payload size must be below the direct HTTP request-body limit of "
        f"{_HTTP_REQUEST_BODY_LIMIT_BYTES} bytes."
    )
    raise BenchmarkError(message)


def _decode_delivery(raw_data: RawData) -> Delivery:
    """Decode publication metadata at the point the subscriber runs."""
    content = raw_data.content
    if len(content) < _MESSAGE_HEADER.size:
        message = "Received benchmark payload without its metadata header."
        raise ValueError(message)
    sequence, published_at_ns = _MESSAGE_HEADER.unpack_from(content)
    received_at_ns = time.perf_counter_ns()
    return Delivery(sequence, published_at_ns, received_at_ns)


def _validate_delivery_timestamp(
    delivery: Delivery,
    sequence: int,
    published_at_ns: int,
) -> None:
    """Check that the callback carried the published timestamp."""
    if delivery.published_at_ns == published_at_ns:
        return
    message = (
        "Subscriber received a payload whose timestamp does not match "
        f"sequence {sequence}."
    )
    raise BenchmarkError(message)


async def _wait_for_delivery(
    deliveries: asyncio.Queue,
    expected_sequence: int,
    timeout_seconds: float,
) -> Delivery:
    """Wait for a callback while ignoring stale retained payloads."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    while True:
        remaining_seconds = deadline - loop.time()
        if remaining_seconds <= 0:
            message = (
                "Timed out waiting for delivery of benchmark sequence "
                f"{expected_sequence}."
            )
            raise TimeoutError(message)
        delivery = await asyncio.wait_for(
            deliveries.get(),
            timeout=remaining_seconds,
        )
        if delivery.sequence == expected_sequence:
            return delivery


@asynccontextmanager
async def _create_context_pair(
    socket_path: Path,
) -> AsyncIterator[tuple[DTPSContext, DTPSContext]]:
    """Start local DTPS contexts for all benchmark transport paths."""
    unix_url = make_http_unix_url(str(socket_path))
    unix_url_string = url_to_string(unix_url)
    environment = {
        "DTPS_BASE_benchmarkserver": f"create:{unix_url_string}",
        "DTPS_BASE_benchmarkclient": unix_url_string,
    }
    async with context_cleanup(
        "benchmarkserver",
        environment,
    ) as context_create:
        await _set_owner_only_unix_socket_permissions(context_create)
        async with context_cleanup(
            "benchmarkclient",
            environment,
        ) as context_use:
            yield context_create, context_use


async def _set_owner_only_unix_socket_permissions(
    context: DTPSContext,
) -> None:
    """Restore private Unix socket access after a restrictive umask."""
    for url_string in await context.get_urls():
        url = parse_url_unescape(url_string)
        if url.scheme != "http+unix" or url.host is None:
            continue
        _set_owner_only_permissions(Path(url.host), 0o600)


async def _benchmark_transport(
    contexts: BenchmarkContexts,
    payload_size: int,
    transport: str,
) -> TransportResult:
    """Benchmark confirmed delivery for one transport and payload."""
    context_create = contexts.create
    context_use = contexts.use
    configuration = contexts.configuration
    topic_name = f"benchmark-{transport}-{payload_size}"
    create_topic_context = context_create / topic_name
    await create_topic_context.queue_create(
        bounds=Bounds.max_length(1),
    )
    use_topic = context_use / topic_name
    deliveries: asyncio.Queue = asyncio.Queue()

    async def on_data(raw_data: RawData) -> None:
        delivery = _decode_delivery(raw_data)
        deliveries.put_nowait(delivery)

    channel_path = contexts.channel_directory / topic_name
    channel_path_string = str(channel_path)
    if transport == _SHM_TRANSPORT:
        shm_subscription_topic = cast("Any", use_topic)
        subscription = await shm_subscription_topic.subscribe(
            on_data,
            shm_path=channel_path_string,
            shm_only=True,
        )
    elif transport in (
        _HTTP_DIRECT_TRANSPORT,
        _WEBSOCKET_PUBLISHER_TRANSPORT,
    ):
        subscription = await use_topic.subscribe(on_data)
    else:
        message = f"Unsupported transport {transport!r}."
        raise BenchmarkError(message)

    publisher: Optional[PublisherInterface] = None
    try:
        if transport == _WEBSOCKET_PUBLISHER_TRANSPORT:
            publisher = await use_topic.publisher()
        endpoints = TransportEndpoints(
            use_topic,
            channel_path_string,
            transport,
            publisher,
        )
        (
            publish_to_callback_latencies_ns,
            publish_durations_ns,
            measurement_started_at_ns,
            measurement_finished_at_ns,
        ) = await _measure_transport_samples(
            contexts,
            endpoints,
            deliveries,
            payload_size,
        )
    finally:
        if publisher is not None:
            await publisher.terminate()
        await subscription.unsubscribe()

    elapsed_seconds = (
        measurement_finished_at_ns - measurement_started_at_ns
    ) / 1_000_000_000.0
    if elapsed_seconds <= 0:
        raise BenchmarkError(_MEASUREMENT_INTERVAL_ERROR)
    return TransportResult(
        transport=transport,
        payload_size_bytes=payload_size,
        samples=configuration.samples,
        confirmed_messages_per_second=(
            configuration.samples / elapsed_seconds
        ),
        publish_to_callback_latency_us=_summarize_ns(
            publish_to_callback_latencies_ns,
        ),
        publish_duration_us=_summarize_ns(publish_durations_ns),
    )


async def _measure_transport_samples(
    contexts: BenchmarkContexts,
    endpoints: TransportEndpoints,
    deliveries: asyncio.Queue,
    payload_size: int,
) -> tuple[list[int], list[int], int, int]:
    """Publish each sample and await its subscriber callback."""
    configuration = contexts.configuration
    publish_to_callback_latencies_ns: list[int] = []
    publish_durations_ns: list[int] = []
    payload_buffer = _make_payload_buffer(payload_size)
    total_samples = configuration.warmup + configuration.samples
    measurement_started_at_ns = 0
    measurement_finished_at_ns = 0
    for sample_index in range(total_samples):
        sequence = sample_index + 1
        published_at_ns = time.perf_counter_ns()
        _MESSAGE_HEADER.pack_into(
            payload_buffer,
            0,
            sequence,
            published_at_ns,
        )
        raw_data = RawData(
            content=bytes(payload_buffer),
            content_type=MIME_OCTET,
        )
        if sample_index == configuration.warmup:
            measurement_started_at_ns = published_at_ns

        if endpoints.transport == _SHM_TRANSPORT:
            shm_publisher = cast("Any", endpoints.use_topic)
            await shm_publisher.publish(
                raw_data,
                shm_path=endpoints.channel_path,
                shm_only=True,
            )
        elif endpoints.transport == _HTTP_DIRECT_TRANSPORT:
            await endpoints.use_topic.publish(raw_data)
        elif endpoints.transport == _WEBSOCKET_PUBLISHER_TRANSPORT:
            publisher = endpoints.publisher
            if publisher is None:
                raise BenchmarkError(
                    "WebSocket publisher transport has no publisher object."
                )
            await publisher.publish(raw_data)
        else:
            raise BenchmarkError(
                f"Unsupported transport {endpoints.transport!r}."
            )
        publish_finished_at_ns = time.perf_counter_ns()
        delivery = await _wait_for_delivery(
            deliveries,
            sequence,
            configuration.timeout_seconds,
        )
        _validate_delivery_timestamp(
            delivery,
            sequence,
            published_at_ns,
        )
        if sample_index < configuration.warmup:
            continue
        publish_to_callback_latencies_ns.append(
            delivery.received_at_ns - published_at_ns,
        )
        publish_durations_ns.append(
            publish_finished_at_ns - published_at_ns,
        )
    measurement_finished_at_ns = time.perf_counter_ns()
    return (
        publish_to_callback_latencies_ns,
        publish_durations_ns,
        measurement_started_at_ns,
        measurement_finished_at_ns,
    )


async def _run_benchmark(
    payload_sizes: Sequence[int],
    configuration: BenchmarkConfiguration,
) -> BenchmarkReport:
    """Run every configured direct HTTP, WebSocket, and SHM benchmark path."""
    results: list[TransportResult] = []
    with tempfile.TemporaryDirectory(
        prefix="dtps-shm-benchmark-",
    ) as directory:
        temporary_directory = Path(directory)
        _set_owner_only_permissions(temporary_directory, 0o700)
        socket_path = temporary_directory / "node.sock"
        channel_directory = temporary_directory / "channels"
        channel_directory.mkdir(mode=0o700)
        _set_owner_only_permissions(channel_directory, 0o700)
        async with _create_context_pair(socket_path) as (
            context_create,
            context_use,
        ):
            contexts = BenchmarkContexts(
                context_create,
                context_use,
                channel_directory,
                configuration,
            )
            for payload_size in payload_sizes:
                for transport in (
                    _HTTP_DIRECT_TRANSPORT,
                    _WEBSOCKET_PUBLISHER_TRANSPORT,
                    _SHM_TRANSPORT,
                ):
                    result = await _benchmark_transport(
                        contexts,
                        payload_size,
                        transport,
                    )
                    results.append(result)
    notes = [
        (
            "HTTP direct publish calls DTPSContext.publish(), which uses an "
            "HTTP POST over a Unix-domain socket. Its confirmation subscriber "
            "receives inline events through a WebSocket."
        ),
        (
            "WebSocket publisher creates DTPSContext.publisher() before "
            "warmup and publishes through its persistent WebSocket. Its "
            "confirmation subscriber also receives inline WebSocket events."
        ),
        "SHM uses a temporary, owner-only channel and latest-value semantics.",
        (
            "Each sample waits for its matching callback, so SHM wake-up "
            "coalescing does not hide drops."
        ),
        (
            "Publish-to-callback latency includes local payload serialization "
            "and the selected high-level publish API path."
        ),
        (
            "Both DTPS contexts run in this process; this is not a "
            "cross-process scheduling benchmark."
        ),
        (
            "Run multiple times on target hardware before treating small "
            "deltas as meaningful."
        ),
    ]
    return BenchmarkReport(
        transport_paths={
            _HTTP_DIRECT_TRANSPORT: (
                "Direct DTPS HTTP POST publisher to inline WebSocket "
                "subscriber over a Unix-domain socket"
            ),
            _WEBSOCKET_PUBLISHER_TRANSPORT: (
                "Persistent DTPS WebSocket publisher to inline WebSocket "
                "subscriber over a Unix-domain socket"
            ),
            _SHM_TRANSPORT: "SHM-only publisher to SHM-only subscriber",
        },
        configuration=configuration,
        notes=notes,
        results=results,
    )


def _configure_logging() -> None:
    """Configure concise command-line benchmark output."""
    logging.basicConfig(format="%(message)s", level=logging.INFO)


def _log_result(result: TransportResult) -> None:
    """Write one readable transport result to the command line."""
    publish_to_callback = result.publish_to_callback_latency_us
    publish_duration = result.publish_duration_us
    transport_label = _HTTP_DIRECT_TRANSPORT_LABEL
    if result.transport == _WEBSOCKET_PUBLISHER_TRANSPORT:
        transport_label = _WEBSOCKET_PUBLISHER_TRANSPORT_LABEL
    if result.transport == _SHM_TRANSPORT:
        transport_label = "SHM"
    LOGGER.info(
        "%s %d B: %.1f confirmed msg/s | publish-to-callback mean %.2f us, "
        "p95 %.2f us, max %.2f us | publish mean %.2f us",
        transport_label,
        result.payload_size_bytes,
        result.confirmed_messages_per_second,
        publish_to_callback.mean_us,
        publish_to_callback.p95_us,
        publish_to_callback.max_us,
        publish_duration.mean_us,
    )


def _parse_arguments() -> tuple[argparse.ArgumentParser, argparse.Namespace]:
    """Parse command-line configuration for the manual benchmark."""
    parser = argparse.ArgumentParser(
        description=(
            "Compare direct DTPS HTTP publication, persistent WebSocket "
            "publication, and shared-memory delivery over a Unix-domain "
            "socket."
        ),
    )
    parser.add_argument(
        "--payload-size",
        action="append",
        type=int,
        dest="payload_sizes",
        help=(
            "Payload size in bytes; repeat for multiple sizes. Defaults to "
            "1 KiB, 64 KiB, and 1,000,000 bytes."
        ),
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=_DEFAULT_SAMPLES,
        help=f"Measured samples per transport; default {_DEFAULT_SAMPLES}.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=_DEFAULT_WARMUP,
        help=f"Discarded samples per transport; default {_DEFAULT_WARMUP}.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=_DEFAULT_TIMEOUT_SECONDS,
        help=(
            "Maximum seconds to wait for each subscriber callback; default "
            f"{_DEFAULT_TIMEOUT_SECONDS}."
        ),
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional path for the complete machine-readable report.",
    )
    return parser, parser.parse_args()


def _validate_arguments(args: argparse.Namespace) -> list[int]:
    """Validate options and return the selected payload sizes."""
    payload_sizes = args.payload_sizes
    if payload_sizes is None:
        payload_sizes = list(_DEFAULT_PAYLOAD_SIZES)
    if args.samples <= 0:
        raise BenchmarkError(_SAMPLES_ERROR)
    if args.warmup < 0:
        raise BenchmarkError(_WARMUP_ERROR)
    if args.timeout <= 0:
        raise BenchmarkError(_TIMEOUT_ERROR)
    for payload_size in payload_sizes:
        _make_payload_buffer(payload_size)
        _validate_http_payload_size(payload_size)
    return payload_sizes


def _write_report(path: Path, report: BenchmarkReport) -> None:
    """Write the report and create any missing parent directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(asdict(report), indent=2, sort_keys=True)
    path.write_text(text + "\n", encoding="utf-8")


def main() -> int:
    """Run the selected benchmark pairs and optionally save results."""
    _configure_logging()
    parser, args = _parse_arguments()
    try:
        payload_sizes = _validate_arguments(args)
    except BenchmarkError as error:
        parser.error(str(error))
    configuration = BenchmarkConfiguration(
        samples=args.samples,
        warmup=args.warmup,
        timeout_seconds=args.timeout,
    )
    report = asyncio.run(
        _run_benchmark(
            payload_sizes,
            configuration,
        )
    )
    for result in report.results:
        _log_result(result)
    output_path = args.output_json
    if output_path is not None:
        _write_report(output_path, report)
        LOGGER.info("Saved report to %s", output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
