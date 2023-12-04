import functools
import os
import stat
import traceback
from asyncio import CancelledError
from typing import Any, AsyncIterator, Awaitable, Callable, Optional, TYPE_CHECKING, TypeVar, Union

from multidict import CIMultiDict, CIMultiDictProxy
from typing_extensions import ParamSpec

from . import logger
from .constants import ENV_MASK_ORIGIN

__all__ = [
    "async_error_catcher",
    "async_error_catcher_iterator",
    "check_is_unix_socket",
    "method_lru_cache",
    "multidict_update",
    "should_mask_origin",
]

PS = ParamSpec("PS")
X = TypeVar("X")

F = TypeVar("F", bound=Callable[..., Any])
FA = TypeVar("FA", bound=Callable[..., Awaitable[Any]])

FAsync = TypeVar("FAsync", bound=Callable[..., AsyncIterator[Any]])

if TYPE_CHECKING:

    def async_error_catcher(_: FA, /) -> FA:
        ...

    def async_error_catcher_iterator(_: FAsync, /) -> FAsync:
        ...

else:

    def async_error_catcher(func: Callable[PS, Awaitable[X]]) -> Callable[PS, Awaitable[X]]:
        @functools.wraps(func)
        async def wrapper(*args: PS.args, **kwargs: PS.kwargs) -> X:
            try:
                return await func(*args, **kwargs)
            # except web.HTTPError:
            #     raise
            except CancelledError:
                raise
            except BaseException:
                logger.error(f"Exception in async in {func.__name__}:\n{traceback.format_exc()}")
                raise

        return wrapper

    def async_error_catcher_iterator(func: Callable[PS, AsyncIterator[X]]) -> Callable[PS, AsyncIterator[X]]:
        @functools.wraps(func)
        async def wrapper(*args: PS.args, **kwargs: PS.kwargs) -> X:
            try:
                async for _ in func(*args, **kwargs):
                    yield _
            except CancelledError:
                raise
            # except web.HTTPError:
            #     raise
            except BaseException:
                logger.error(f"Exception in async in {func.__name__}:\n{traceback.format_exc()}")
                raise

        return wrapper


if TYPE_CHECKING:

    def method_lru_cache() -> Callable[[F], F]:
        ...

else:
    from methodtools import lru_cache as method_lru_cache


def multidict_update(dest: CIMultiDict[X], src: Union[CIMultiDict[X], CIMultiDictProxy[X]]) -> None:
    for k, v in src.items():
        dest.add(k, v)


@functools.lru_cache(maxsize=None)
def should_mask_origin() -> bool:
    default = False
    v = os.environ.get(ENV_MASK_ORIGIN, str(default))
    t = is_truthy(v)
    if t is None:
        logger.warning(f"Cannot parse {ENV_MASK_ORIGIN}={v!r} as truthy or falsy; using default {default}")
        return default
    return t


def is_truthy(s: str) -> Optional[bool]:
    """
    Determines if the given string represents a truthy or falsy value.

    Parameters:
    input_str (str): A string that holds the value to be evaluated.

    Returns:
    bool|None: True if the value is truthy (e.g., "true", "True", "1", "yes").
               False if the value is falsy (e.g., "false", "False", "0", "no").
               None if the value does not match any truthy or falsy representation.

    Example:
    >>> is_truthy("True")
    True
    >>> is_truthy("false")
    False
    >>> is_truthy("not sure")
    None
    """

    # Convert the string to lowercase to ensure case-insensitive comparison.
    input_str_lower = s.lower()

    # Define sets of strings that are considered "truthy" and "falsy".
    truthy_set = {"true", "1", "yes", "t", "y"}
    falsy_set = {"false", "0", "no", "f", "n"}

    if input_str_lower in truthy_set:
        return True
    elif input_str_lower in falsy_set:
        return False
    else:
        return None  # The value is neither truthy nor falsy.


def check_is_unix_socket(u: str) -> None:
    exists = os.path.exists(u)
    if not exists:
        msg = f"Unix socket {u} does not exist.\n"

        d = os.path.dirname(u)
        if not os.path.exists(d):
            msg += f" Directory {d} does not exist.\n"
        else:
            msg += f" Directory {d} exists.\n"
            ls = os.listdir(d)
            msg += f" Contents of {d} are {ls!r}\n"
        raise ValueError(msg)

    st = os.stat(u)
    is_socket = stat.S_ISSOCK(st.st_mode)
    if not is_socket:
        msg = f"Path socket {u} exists but it is not a socket."
        raise ValueError(msg)
