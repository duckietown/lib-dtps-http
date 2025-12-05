"""
Compatibility module for Python 3.6 support.

This module provides compatibility shims for features that were 
introduced in Python 3.7+.
"""
import asyncio
import sys
import time

# asynccontextmanager was introduced in Python 3.7
# For Python 3.6, we use async_generator package
if sys.version_info >= (3, 7):
    from contextlib import asynccontextmanager
    from contextlib import AsyncExitStack
    time_ns = time.time_ns
    monotonic_ns = time.monotonic_ns
else:
    try:
        from async_generator import asynccontextmanager
    except ImportError:
        raise ImportError(
            "For Python 3.6, you need to install async_generator: "
            "pip install async_generator"
        )
    try:
        from async_exit_stack import AsyncExitStack
    except ImportError:
        raise ImportError(
            "For Python 3.6, you need to install async_exit_stack: "
            "pip install async_exit_stack"
        )
    # time.time_ns() was introduced in Python 3.7
    def time_ns():
        return int(time.time() * 1_000_000_000)
    
    # time.monotonic_ns() was introduced in Python 3.7
    def monotonic_ns():
        return int(time.monotonic() * 1_000_000_000)
    
    # asyncio.create_task() was introduced in Python 3.7
    # Monkey-patch it for compatibility with libraries that use it
    def _create_task(coro, *, name=None):
        loop = asyncio.get_event_loop()
        return loop.create_task(coro)
    
    asyncio.create_task = _create_task

__all__ = ["asynccontextmanager", "AsyncExitStack", "time_ns", "monotonic_ns"]
