import functools
import traceback
from typing import Any, Awaitable, Callable, ParamSpec, TYPE_CHECKING, TypeVar
from multidict import CIMultiDict
from aiohttp import web

from . import logger

__all__ = [
    "async_error_catcher",
    "method_lru_cache",
    "multidict_update",
]

PS = ParamSpec("PS")
X = TypeVar("X")

F = TypeVar("F", bound=Callable[..., Any])

if TYPE_CHECKING:

    def async_error_catcher(_: F, /) -> F:
        ...

else:

    def async_error_catcher(func: Callable[PS, Awaitable[X]]) -> Callable[PS, Awaitable[X]]:
        @functools.wraps(func)
        async def wrapper(*args: PS.args, **kwargs: PS.kwargs) -> X:
            try:
                return await func(*args, **kwargs)
            # except web.HTTPError:
            #     raise
            except BaseException:
                logger.error(f"Exception in async in {func.__name__}:\n{traceback.format_exc()}")
                raise

        return wrapper


if TYPE_CHECKING:

    def method_lru_cache(_: F, /) -> F:
        ...

else:
    from methodtools import lru_cache as method_lru_cache


def multidict_update(dest: CIMultiDict, src: CIMultiDict) -> None:
    for k, v in src.items():
        dest.add(k, v)
