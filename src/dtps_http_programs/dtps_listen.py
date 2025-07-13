"""DTPS listen."""

__all__ = ["dtps_listen_main"]


import argparse
import asyncio
import time
from typing import Any

from dtps_http import (
    URL,
    DTPSClient,
    ErrorMessage,
    FinishedMessage,
    NodeID,
    StopContinuousLoopError,
    async_error_catcher,
    parse_url_unescape,
    pretty,
)
from dtps_http.structures import ListenURLEvents
from dtps_http_programs import logger


def callback(
    max_time: int,
    max_messages: int,
    max_errors: int,
    start_time: float,
    number_of_messages: int,
    error_messages: list[ErrorMessage],
    *,
    raise_on_error: bool,
) -> Any:
    def callback(listen_url_events: ListenURLEvents) -> None:
        pretty_listen_url_events = pretty(listen_url_events)
        logger.info(pretty_listen_url_events)
        if isinstance(listen_url_events, FinishedMessage):
            logger.info("Finished.")
        if isinstance(listen_url_events, ErrorMessage):
            error_messages.append(listen_url_events)
        if time.time() - start_time > max_time:
            logger.info("Timeout.")
        if number_of_messages > max_messages:
            logger.info("Maximum number of messages.")
        if len(error_messages) > max_errors and raise_on_error:
            details = "".join(
                f"=== {i} ===\n" + x.comment + "\n===========\n\n"
                for i, x in enumerate(error_messages)
            )
            message = f"Found error messages:\n{details}"
            raise StopContinuousLoopError(message)

    return callback


@async_error_catcher
async def dtps_listen_main_future(
    url: URL,
    *,
    expect_node: NodeID | None,
    switch_identity_ok: bool,
    raise_on_error: bool,
    max_time: int,
    max_messages: int,
    max_errors: int,
    inline_data: bool,
) -> None:
    """Run DTPS listen future."""
    logger.info("Listening to %s", url)
    start_time = time.time()
    number_of_messages = 0
    error_messages: list[ErrorMessage] = []
    async with DTPSClient.create(shutdown_event=None) as client:
        await client.listen_continuous(
            url,
            expect_node,
            switch_identity_ok=switch_identity_ok,
            raise_on_error=raise_on_error,
            add_silence=1,
            inline_data=inline_data,
            callback=callback(
                max_time,
                max_messages,
                max_errors,
                start_time,
                number_of_messages,
                error_messages,
                raise_on_error=raise_on_error,
            ),
            max_frequency=None,
        )
    if error_messages and raise_on_error:
        iterable = (
            f"=== {i} ===\n" + x.comment + "\n===========\n\n"
            for i, x in enumerate(error_messages)
        )
        details = "".join(iterable)
        message = f"Found error messages:\n{details}"
        raise Exception(message)


def dtps_listen_main(args: list[str] | None = None) -> None:
    """Run DTPS listen."""
    parser = argparse.ArgumentParser(
        description="Listens to a DTPS source using websockets.",
    )
    parser.add_argument("--url", required=True, help="Topic URL inline data")
    parser.add_argument("--expect", help="Expected node id")
    parser.add_argument(
        "--max-time",
        type=int,
        default=1_000_000,
        help="Maximum time to listen",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=1_000_000,
        help="Maximum messages to receive",
    )
    parser.add_argument(
        "--max-errors",
        type=int,
        default=1_000_000,
        help="Maximum errors to tolerate",
    )
    parser.add_argument(
        "--inline-data",
        default=False,
        action="store_true",
        help="Use inline data",
    )
    parser.add_argument(
        "--raise-on-error",
        default=False,
        action="store_true",
        help="Raise if any error from the other side",
    )
    parsed = parser.parse_args(args=args)
    url = parse_url_unescape(parsed.url)
    raise_on_error = parsed.raise_on_error
    future = dtps_listen_main_future(
        url,
        expect_node=parsed.expect,
        switch_identity_ok=True,
        raise_on_error=raise_on_error,
        max_time=parsed.max_time,
        max_messages=parsed.max_messages,
        max_errors=parsed.max_errors,
        inline_data=parsed.inline_data,
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(future)
