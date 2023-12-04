import asyncio
from functools import wraps

__all__ = ["test_timeout"]


def test_timeout(seconds):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                # Use asyncio.wait_for to apply the timeout
                return await asyncio.wait_for(func(*args, **kwargs), timeout=seconds)
            except asyncio.TimeoutError:
                # Raise a custom exception or handle the timeout as needed
                raise TimeoutError(f"Function {func.__name__} timed out after {seconds} seconds")

        return wrapper

    return decorator
