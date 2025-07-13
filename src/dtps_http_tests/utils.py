"""Utilities."""

__all__ = ["test_timeout"]

import asyncio
from functools import wraps
from typing import Any


def test_timeout(seconds: float) -> Any:
    """Run timeout test."""

    def decorator(func) -> Any:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                # Use asyncio.wait_for to apply the timeout
                return await asyncio.wait_for(func(*args, **kwargs), seconds)
            except TimeoutError:
                # Raise a custom exception or handle the timeout as
                # needed
                message = (
                    f"Function {func.__name__} timed out after {seconds} "
                    "seconds."
                )
                raise TimeoutError(message)

        return wrapper

    return decorator
