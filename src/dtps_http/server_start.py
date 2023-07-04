import argparse
import asyncio
import socket
import sys
from contextlib import asynccontextmanager
from typing import AsyncIterator, Iterator, Optional
from socket import AddressFamily
import psutil
from aiohttp import web

from . import logger
from .server import DTPSServer

__all__ = [
    "app_start",
    "interpret_command_line_and_start",
]


def get_ip_addresses() -> Iterator[tuple[str, AddressFamily, str]]:
    for interface, snics in psutil.net_if_addrs().items():
        # print(f"interface={interface!r} snics={snics!r}")
        for snic in snics:
            # if snic.family == family:
            yield (
                interface,
                snic.family,
                snic.address,
            )


async def interpret_command_line_and_start(dtps: DTPSServer, args: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser()

    # default_hostname = socket.gethostname()

    #
    # print(f"addresses}")
    # print(f"ipv6s={ipv6s!r}")
    parser.add_argument("--tcp-port", type=int, default=None, required=False)
    parser.add_argument("--tcp-host", required=False, default="0.0.0.0")
    parser.add_argument("--unix-path", required=False, default=None)

    parsed = parser.parse_args(args)

    if parsed.tcp_port is None and parsed.unix_path is None:
        msg = "Please specify at least one of --tcp-port or --unix-path"
        logger.error(msg)
        sys.exit(msg)

    if parsed.tcp_port is not None:
        tcp = (parsed.tcp_host, parsed.tcp_port)

    else:
        tcp = None

    if parsed.unix_path is not None:
        unix_path = parsed.unix_path
    else:
        unix_path = None

    never = asyncio.Event()
    async with app_start(dtps, tcp=tcp, unix_path=unix_path):
        await never.wait()


@asynccontextmanager
async def app_start(
    s: DTPSServer,
    tcp: Optional[tuple[str, int]] = None,
    unix_path: Optional[str] = None,
) -> AsyncIterator[None]:
    runner = web.AppRunner(s.app)
    await runner.setup()

    available_urls: list[str] = []
    if tcp is not None:
        tcp_host, port = tcp
        the_url = f"http://{tcp_host}:{port}"
        logger.info(f"Starting TCP server - the URL is {the_url!r}")

        tcp_site = web.TCPSite(runner, tcp_host, port)
        await tcp_site.start()

        if tcp_host == "0.0.0.0":
            # addresses = list(get_ip_addresses())
            # macs = {}
            # for interface, family, address in addresses:
            #     if family == socket.AF_LINK:
            #         macs[interface] = address

            for interface, family, address in get_ip_addresses():
                if family != socket.AF_INET:
                    continue

                if address.startswith("127."):
                    continue

                the_url = f"http://{address}:{port}/"
                available_urls.append(the_url)

            the_url = f"http://{socket.gethostname()}:{port}/"
            available_urls.append(the_url)

            # add a weird address
            the_url = f"http://8.8.12.2:{port}/"
            available_urls.append(the_url)
            # add a non-existente hostname
            the_url = f"http://dewde.invalid.com:{port}/"
            available_urls.append(the_url)
            # add a wrong port
            the_url = f"http://localhost:12345/"
            available_urls.append(the_url)
            # add a wrong host
            the_url = f"http://google.com/"
            available_urls.append(the_url)

            for interface, family, address in get_ip_addresses():
                if family != socket.AF_INET6:
                    continue

                if address.startswith("::1") or address.startswith("fe80:"):
                    continue

                the_url = f"http://[{address}]:{port}/"

                available_urls.append(the_url)

            if False:
                for interface, family, address in get_ip_addresses():
                    if family != socket.AF_LINK:
                        continue

                    address = address.replace(":", "%3A")
                    the_url = f"http+ether://{address}:{port}"

                    available_urls.append(the_url)
        else:
            available_urls.append(the_url)

    else:
        logger.info("not starting TCP server. Use --tcp-port to start one.")

    if unix_path is not None:
        path = unix_path.replace("/", "%2F")
        the_url = f"http+unix://{path}/"

        logger.info(f"starting Unix server on path {unix_path!r} - the URL is {the_url!r}")
        unix_site = web.UnixSite(runner, unix_path)
        await unix_site.start()

        available_urls.append(the_url)
    else:
        logger.info("not starting Unix server. Use --unix-path to start one.")

    if not available_urls:
        msg = "Please specify at least one of --tcp-port or --unix-path"
        logger.error(msg)
        sys.exit(msg)

    s.set_available_urls(available_urls)
    logger.info("available URLs\n" + "".join("* " + _ + "\n" for _ in available_urls))
    # wait for finish signal
    yield
    await runner.cleanup()
