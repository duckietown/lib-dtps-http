import sys
from typing import List, Optional

from . import logger

__all__ = [
    "dtps_main",
]


def dtps_main(args: Optional[List[str]] = None) -> None:
    if args is None:
        args = sys.argv[1:]
    from .dtps_listen import dtps_listen_main
    from .dtps_send_continuous import dtps_send_continuous_main
    from .dtps_stats import dtps_stats_main
    from .server_clock import clock_main, server_main

    commands = {
        "listen": dtps_listen_main,
        "stats": dtps_stats_main,
        "send": dtps_send_continuous_main,
        "server": server_main,
        "clock": clock_main,
    }
    if len(args) == 0:
        logger.error(f"Expected at least one argument: {list(commands.keys())}")

    first = args[0]
    if first not in commands:
        logger.error(f"Unknown command: {first}. Expected one of {list(commands.keys())}")
        sys.exit(2)

    commands[first](args[1:])
