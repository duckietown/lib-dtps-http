"""Utilities every once in a while."""

__all__ = ["EveryOnceInAWhile"]

import time


class EveryOnceInAWhile:
    """Every once in a while.

    Simple class to do a task every once in a while.
    """

    ever_called: bool
    interval: float | None
    last: float

    def __init__(self, interval: float, *, do_first_now: bool = True) -> None:
        """Initialize every once in a while."""
        self.interval = interval
        if do_first_now:
            self.last = 0
        else:
            self.last = time.time()
        self.ever_called = False

    def now(self) -> bool:
        """Return `True` if now, `False` otherwise."""
        if self.interval is None:
            return True
        current_time = time.time()
        if current_time - self.last >= self.interval:
            self.last = current_time
            self.ever_called = True
            return True
        return False

    def was_ever_time(self) -> bool:
        """Return `True` if ever called, `False` otherwise."""
        return self.ever_called
