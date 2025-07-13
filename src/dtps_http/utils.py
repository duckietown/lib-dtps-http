"""Utilities."""

__all__ = [
    "async_error_catcher",
    "async_error_catcher_iterator",
    "check_is_unix_socket",
    "method_lru_cache",
    "multidict_update",
    "parse_cbor_tagged",
    "parse_tagged",
    "pretty",
    "pydantic_parse",
    "should_mask_origin",
    "wait_for_unix_socket",
]

import asyncio
import functools
import os
import stat
import traceback
from asyncio import CancelledError
from collections.abc import AsyncIterator, Awaitable, Callable
from io import StringIO
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    TypeVar,
    cast,
)

import cbor2
import prettyprinter
from aiohttp.web_exceptions import HTTPNotFound
from multidict import CIMultiDict, CIMultiDictProxy
from pydantic import parse_obj_as
from typing_extensions import ParamSpec

from dtps_http import logger
from dtps_http.constants import ENV_MASK_ORIGIN

PRETTYPRINT_EXTRAS_EXCLUDE = os.environ.get("PRETTYPRINT_EXTRAS_EXCLUDE", "")
split_prettyprint_extras_exclude = PRETTYPRINT_EXTRAS_EXCLUDE.split(",")
filtered_prettyprint_extras_exclude = filter(len, PRETTYPRINT_EXTRAS_EXCLUDE)
iterable = [
    "ipython_repr_pretty",
    "ipython",
    "django",
    *list(filtered_prettyprint_extras_exclude),
]
exclude = frozenset(iterable)
prettyprinter.install_extras(exclude=exclude)

F = TypeVar("F", bound=Callable[..., Any])
FA = TypeVar("FA", bound=Callable[..., Awaitable[Any]])
PS = ParamSpec("PS")
X = TypeVar("X")

FAsync = TypeVar("FAsync", bound=Callable[..., AsyncIterator[Any]])

if TYPE_CHECKING:

    def async_error_catcher(_: FA, /) -> FA:
        """Asynchronous error catcher."""

    def async_error_catcher_iterator(_: FAsync, /) -> FAsync:
        """Asynchronous error catcher iterator."""

else:

    def async_error_catcher(
        func: Callable[PS, Awaitable[X]],
    ) -> Callable[PS, Awaitable[X]]:
        """Asynchronous error catcher."""

        @functools.wraps(func)
        async def wrapper(*args: PS.args, **kwargs: PS.kwargs) -> X:
            try:
                return await func(*args, **kwargs)
            except CancelledError:
                raise
            except HTTPNotFound:
                raise
            except BaseException:
                logger.exception(
                    "async_error_catcher: Exception in async in %s:\n%s",
                    func.__name__,
                    traceback.format_exc(),
                )
                raise

        return wrapper

    def async_error_catcher_iterator(
        func: Callable[PS, AsyncIterator[X]],
    ) -> Callable[PS, AsyncIterator[X]]:
        """Asynchronous error catcher iterator."""

        @functools.wraps(func)
        async def wrapper(
            *args: PS.args,
            **kwargs: PS.kwargs,
        ) -> AsyncIterator[X]:
            try:
                async for _ in func(*args, **kwargs):
                    yield _
            except CancelledError:
                raise
            except HTTPNotFound:
                raise
            except BaseException:
                logger.exception(
                    "Exception in async in %s:\n%s",
                    func.__name__,
                    traceback.format_exc(),
                )
                raise

        return wrapper


if TYPE_CHECKING:

    def method_lru_cache() -> Callable[[F], F]:
        """Method LRU cache."""

else:
    from methodtools import lru_cache as method_lru_cache


def multidict_update(
    dest: CIMultiDict[X],
    src: CIMultiDict[X] | CIMultiDictProxy[X],
) -> None:
    """Multidict update."""
    for key, value in src.items():
        dest.add(key, value)


@functools.lru_cache(maxsize=128)
def should_mask_origin() -> bool:
    """Return `True` if origin should be masked, `False` otherwise."""
    default = False
    default_string = str(default)
    env_mask_origin = os.environ.get(ENV_MASK_ORIGIN, default_string)
    mask_origin = is_truthy(env_mask_origin)
    if mask_origin is None:
        logger.warning(
            "Cannot parse %s=%r as truthy or falsy; using default %s",
            ENV_MASK_ORIGIN,
            mask_origin,
            default,
        )
        return default
    return mask_origin


def is_truthy(s: str) -> bool | None:
    """Return `True` if truthy, `False` otherwise.

    Determines if the given string represents a truthy or falsy value.

    Parameters
    ----------
    s (str): A string that holds the value to be evaluated.

    Returns
    -------
    bool|None: True if the value is truthy (e.g., "true", "True", "1",
    "yes").
               False if the value is falsy (e.g., "false", "False", "0",
               "no").
               None if the value does not match any truthy or falsy
               representation.

    Example:
    >>> is_truthy("True")
    True
    >>> is_truthy("false")
    False
    >>> is_truthy("not sure")
    None

    """
    # Convert the string to lowercase to ensure case-insensitive
    # comparison.
    input_str_lower = s.lower()
    # Define sets of strings that are considered "truthy" and "falsy".
    truthy_set = {"true", "1", "yes", "t", "y"}
    falsy_set = {"false", "0", "no", "f", "n"}
    if input_str_lower in truthy_set:
        return True
    if input_str_lower in falsy_set:
        return False
    return None  # The value is neither truthy nor falsy.


async def wait_for_unix_socket(u: str) -> None:
    """Wait for unix socket."""
    while True:
        path = Path(u)
        exists = path.exists()
        if exists:
            check_is_unix_socket(u)
            return
        await asyncio.sleep(0.1)
        continue


def check_is_unix_socket(u: str) -> None:
    """Return `True` if unix socket, `False` otherwise."""
    u_path = Path(u)
    exists = u_path.exists()
    if not exists:
        message = f"Unix socket {u} does not exist.\n"
        d = u_path.parent
        d_path = Path(d)
        if not d_path.exists():
            message += f" Directory {d} does not exist.\n"
        else:
            message += f" Directory {d} exists.\n"
            ls = os.listdir(d)
            message += f" Contents of {d} are {ls!r}.\n"
        raise ValueError(message)
    st = u_path.stat()
    is_socket = stat.S_ISSOCK(st.st_mode)
    if not is_socket:
        message = f"Path socket {u} exists but it is not a socket."
        raise ValueError(message)


def parse_cbor_tagged(b: bytes, *ts: type[X]) -> X:
    """Parse CBOR tagged."""
    as_struct = cbor2.loads(b)
    if not isinstance(as_struct, dict):
        message = f"parse_cbor_tagged: {as_struct!r} is not a dictionary."
        raise TypeError(message)
    as_struct = cast(dict[str, Any], as_struct)
    return parse_tagged(as_struct, *ts)


def parse_tagged(d: dict[str, Any], *ts: type[X]) -> X:
    """Parse tagged."""
    if not ts:
        message = "parse_tagged: no types given."
        raise ValueError(message)
    for t in ts:
        kn = t.__name__
        if kn in d:
            vals = d[kn]
            if not isinstance(vals, dict):
                message = (
                    f"parse_tagged: {d!r} has {kn!r} but it is not a "
                    "dictionary."
                )
                raise TypeError(message)
            return pydantic_parse(t, vals)
    message = f"parse_tagged: {d!r} does not have any of {ts!r}."
    raise ValueError(message)


def pydantic_parse(t: type[X], d: Any) -> X:
    """Pydantic parse."""
    return parse_obj_as(t, d)


def pretty(d: object, /) -> str:
    """Pretty."""
    io = StringIO()
    prettyprinter.pprint(d, stream=io)
    value = io.getvalue()
    return value.strip()
