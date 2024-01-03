import argparse
import asyncio
import time
from io import StringIO
from pprint import pprint
from typing import List, Optional

from dtps_http import (
    async_error_catcher,
    DTPSClient,
    ErrorMsg,
    FinishedMsg,
    NodeID,
    parse_url_unescape,
    pretty,
    StopContinuousLoop,
    URL,
)
from dtps_http.client import ListenDataInterface
from dtps_http.structures import ListenURLEvents
from . import logger

__all__ = [
    "dtps_listen_main",
]


@async_error_catcher
async def dtps_listen_main_f(
    url: URL,
    expect_node: Optional[NodeID],
    switch_identity_ok: bool,
    raise_on_error: bool,
    max_time: int,
    max_messages: int,
    max_errors: int,
    inline_data: bool,
) -> None:
    logger.info("Listening to %s", url)
    t0 = time.time()
    nmessages = 0
    error_msgs = []
    async with DTPSClient.create(shutdown_event=None) as client:
        ld: ListenDataInterface

        async def callback(d: ListenURLEvents):
            logger.info(pretty(d))
            if isinstance(d, FinishedMsg):
                logger.info("Finished")

            if isinstance(d, ErrorMsg):
                error_msgs.append(d)
                # if raise_on_error:
                #     raise Exception(d.comment)
            dt = time.time() - t0
            if dt > max_time:
                logger.info("Timeout")
            if nmessages > max_messages:
                logger.info("Max messages")
            if len(error_msgs) > max_errors:
                if raise_on_error:
                    details = "".join(
                        f"=== {i} ===\n" + x.comment + "\n===========\n\n" for i, x in enumerate(error_msgs)
                    )
                    msg = f"Found error messages:\n{details}"
                    raise StopContinuousLoop(msg)

        ld = await client.listen_continuous(
            url,
            expect_node,
            switch_identity_ok=switch_identity_ok,
            raise_on_error=raise_on_error,
            add_silence=1,
            inline_data=inline_data,
            callback=callback,
        )

    if error_msgs and raise_on_error:
        details = "".join(
            f"=== {i} ===\n" + x.comment + "\n===========\n\n" for i, x in enumerate(error_msgs)
        )
        msg = f"Found error messages:\n{details}"
        raise Exception(msg)


def dtps_listen_main(args: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Listens to a DTPS source using websockets")
    parser.add_argument("--url", required=True, help="Topic URL inline data")
    parser.add_argument("--expect", help="Expected node id")
    parser.add_argument("--max-time", type=int, default=1_000_000, help="Maximum time to listen")
    parser.add_argument("--max-messages", type=int, default=1_000_000, help="Maximum messages to receive")
    parser.add_argument("--max-errors", type=int, default=1_000_000, help="Maximum errors to tolerate")
    parser.add_argument("--inline-data", default=False, action="store_true", help="Use inline data")

    parser.add_argument(
        "--raise-on-error", default=False, action="store_true", help="Raise if any error from the other side"
    )
    parsed = parser.parse_args(args=args)
    url = parse_url_unescape(parsed.url)

    raise_on_error = parsed.raise_on_error
    f = dtps_listen_main_f(
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
    loop.run_until_complete(f)
