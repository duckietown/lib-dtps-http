"""Together."""

__all__ = ["dtps_main"]

import sys

from dtps_http_programs import logger
from dtps_http_programs.dtps_listen import dtps_listen_main
from dtps_http_programs.dtps_send_continuous import dtps_send_continuous_main
from dtps_http_programs.dtps_stats import dtps_stats_main
from dtps_http_programs.server_clock import clock_main, server_main


def dtps_main(args: list[str] | None = None) -> None:
    """Run DTPS."""
    if args is None:
        args = sys.argv[1:]
    commands = {
        "listen": dtps_listen_main,
        "stats": dtps_stats_main,
        "send": dtps_send_continuous_main,
        "server": server_main,
        "clock": clock_main,
    }
    commands_keys = commands.keys()
    commands_keys_list = list(commands_keys)
    if len(args) == 0:
        logger.exception(
            "Expected at least one argument: %s",
            commands_keys_list,
        )
    first = args[0]
    if first not in commands:
        logger.exception(
            "Unknown command: %s. Expected one of %s",
            first,
            commands_keys_list,
        )
        sys.exit(2)
    commands[first](args[1:])
