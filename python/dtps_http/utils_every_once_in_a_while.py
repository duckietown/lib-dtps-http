import time

__all__ = [
    "EveryOnceInAWhile",
]

from typing import Optional


class EveryOnceInAWhile:
    """Simple class to do a task every once in a while."""

    interval: Optional[float]
    last: float
    ever_called: bool

    def __init__(self, interval: float, do_first_now: bool = True):
        self.interval = interval
        if do_first_now:
            self.last = 0.0
        else:
            self.last = time.time()
        self.ever_called = False

    def now(self) -> bool:
        if self.interval is None:
            return True
        n = time.time()
        dt = n - self.last
        if dt >= self.interval:
            self.last = n
            self.ever_called = True
            return True
        else:
            return False

    def was_ever_time(self) -> bool:
        return self.ever_called
