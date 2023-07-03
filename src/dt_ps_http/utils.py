import functools
import traceback

from . import logger

__all__ = [
    "async_error_catcher",
]


def async_error_catcher(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Exception in async in {func.__name__}:\n{traceback.format_exc()}")
            raise

    return wrapper
