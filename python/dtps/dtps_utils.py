import asyncio
from asyncio import CancelledError, Queue
from typing import Any, Awaitable, Callable, List, Optional, TypeVar

from dtps_http import RawData
from . import logger
from .ergo_ui import DTPSContext, SubscriptionInterface

__all__ = [
    "process_lowdatasize_last_recent",
]


async def process_lowdatasize_last_recent(
    context: DTPSContext,
    expensive_callback: Callable[[RawData], Awaitable[None]],
) -> "ExpensiveCallbackSubscription":
    """
    Make sure we are not a slow reader even for an expensive callback.

    Suitable for this case:

    0) only the last message is important (e.g. joystick)
    1) there are a lot of small messages
    2) low-latency is important
    2) the callback is expensive (e.g. write to hardware)


    Note! If the callback uses *blocking IO* we need to do it
    in a different process! (or thread)

    """
    s = ExpensiveCallbackSubscription(expensive_callback)
    await s.init(context)
    return s


X = TypeVar("X")


async def queue_get_multiple(q: "Queue[X]") -> List[X]:
    """Gets a packet of ready messages from the queue (at least one)."""
    first = await q.get()
    msgs = [first]
    while True:
        try:
            msgs.append(q.get_nowait())
        except asyncio.QueueEmpty:
            break
    return msgs


class ExpensiveCallbackSubscription(SubscriptionInterface):
    q: "Queue[RawData]"
    sub: Optional[SubscriptionInterface]
    task: "Optional[asyncio.Task[Any]]"

    def __init__(self, expensive_callback: Callable[[RawData], Awaitable[None]]):
        self.expensive_callback = expensive_callback
        self.q = Queue()
        self.sub = None
        self.nreceived = 0
        self.nprocessed = 0
        self.nskipped = 0

    async def init(self, context: DTPSContext) -> None:
        self.sub = await context.subscribe(self.raw_callback)
        self.task = asyncio.create_task(self.process_task())

    async def process_task(self) -> None:
        while True:
            rd = await queue_get_multiple(self.q)
            # only process the last one
            last = rd[-1]
            nskipped_now = len(rd) - 1
            self.nprocessed += 1
            self.nskipped += nskipped_now

            if nskipped_now > 0:
                percentage_skipped = self.nskipped / self.nreceived
                logger.debug(
                    f"Skipped {nskipped_now} messages now in this iteration.\n"
                    f"Total received: {self.nreceived}. "
                    f"Total skipped: {self.nskipped} ({percentage_skipped:.2%})\n"
                )
            try:
                await self.expensive_callback(last)
            except CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in expensive callback", exc_info=e)
                continue

    async def raw_callback(self, r: RawData) -> None:
        self.nreceived += 1
        self.q.put_nowait(r)

    async def aclose(self) -> None:
        if self.nreceived == 0:
            logger.debug("No messages received")
        else:

            percentage_skipped = self.nskipped / self.nreceived
            percentage_processed = self.nprocessed / self.nreceived

            logger.debug(
                f"Final statistics:\n"
                f" Total received:  {self.nreceived:5} \n"
                f" Total processed: {self.nprocessed:5} ({percentage_processed:.2%})\n"
                f" Total skipped:   {self.nskipped:5} ({percentage_skipped:.2%})\n"
            )

        if self.sub is not None:
            await self.sub.unsubscribe()
            self.sub = None
        if self.task is not None:
            self.task.cancel()
            self.task = None

    async def unsubscribe(self) -> None:
        await self.aclose()
