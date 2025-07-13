"""DTPS utils."""

__all__ = ["process_low_data_size_last_recent"]

import asyncio
from asyncio import CancelledError, Queue, QueueEmpty, Task
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from dtps import logger
from dtps.ergo_abstract import (
    AbstractDTPSContext,
    AbstractSubscriptionInterface,
)
from dtps_http import RawData

X = TypeVar("X")


class ExpensiveCallbackSubscription(AbstractSubscriptionInterface):
    expensive_callback: Callable[[RawData], Awaitable[None]]
    number_processed: int
    number_received: int
    number_skipped: int
    queue: Queue[RawData]
    sub: AbstractSubscriptionInterface | None
    task: Task[Any] | None

    def __init__(
        self,
        expensive_callback: Callable[[RawData], Awaitable[None]],
    ) -> None:
        self.expensive_callback = expensive_callback
        self.queue = Queue()
        self.sub = None
        self.number_received = 0
        self.number_processed = 0
        self.number_skipped = 0

    async def aclose(self) -> None:
        if self.number_received == 0:
            logger.debug("No messages received")
        else:
            percentage_skipped = self.number_skipped / self.number_received
            percentage_processed = self.number_processed / self.number_received
            logger.debug(
                "Final statistics:\n"
                " Total received:  %s\n"
                " Total processed: %s (%.2f)\n"
                " Total skipped:   %s (%.2f)\n",
                self.number_received,
                self.number_processed,
                percentage_processed,
                self.number_skipped,
                percentage_skipped,
            )
        if self.sub is not None:
            await self.sub.unsubscribe()
            self.sub = None
        if self.task is not None:
            self.task.cancel()
            self.task = None

    async def initialize(self, context: AbstractDTPSContext) -> None:
        self.sub = await context.subscribe(self.raw_callback)
        coroutine = self.process_task()
        self.task = asyncio.create_task(coroutine)

    async def process_task(self) -> None:
        while True:
            raw_data_list = await queue_get_multiple(self.queue)
            last = raw_data_list[-1]
            number_skipped_now = len(raw_data_list) - 1
            self.number_processed += 1
            self.number_skipped += number_skipped_now
            if number_skipped_now > 0:
                percentage_skipped = self.number_skipped / self.number_received
                logger.debug(
                    "Skipped %s messages now in this iteration.\n"
                    "Total received: %s. Total skipped: %s (%.2f)\n",
                    number_skipped_now,
                    self.number_received,
                    self.number_skipped,
                    percentage_skipped,
                )
            try:
                await self.expensive_callback(last)
            except CancelledError:
                break
            except Exception as exception:
                logger.exception(
                    "Error in expensive callback",
                    exc_info=exception,
                )
                continue

    async def raw_callback(self, raw_data: RawData) -> None:
        self.number_received += 1
        self.queue.put_nowait(raw_data)

    async def unsubscribe(self) -> None:
        await self.aclose()


async def process_low_data_size_last_recent(
    context: AbstractDTPSContext,
    expensive_callback: Callable[[RawData], Awaitable[None]],
) -> ExpensiveCallbackSubscription:
    """Ensure we are not a slow reader, even for an expensive callback.

    Suitable for the following cases:

    1) only the last message is important;
    2) there are many small messages;
    3) low-latency is important;
    4) the callback is expensive (e.g. write to hardware)

    Note: if the callback uses *blocking IO*, we need to do it
    in a different process (or thread).

    """
    subscription = ExpensiveCallbackSubscription(expensive_callback)
    await subscription.initialize(context)
    return subscription


async def queue_get_multiple(queue: Queue[X]) -> list[X]:
    """Get at least one packet of ready messages from the queue."""
    first = await queue.get()
    messages = [first]
    while True:
        try:
            message = queue.get_nowait()
            messages.append(message)
        except QueueEmpty:
            break
    return messages
