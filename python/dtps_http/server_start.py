import argparse
import asyncio
import json
import os
import socket
import sys
import tempfile
from contextlib import asynccontextmanager
from socket import AddressFamily
from typing import AsyncIterator, Iterator, List, Optional, Tuple

import psutil
from aiohttp import web

from . import logger
from .urls import URLIndexer, parse_url_unescape
from .server import DTPSServer
from .structures import Registration
from .types import TopicNameV

__all__ = [
    "app_start",
    "interpret_command_line_and_start",
]


def get_ip_addresses() -> Iterator[Tuple[str, AddressFamily, str]]:
    for interface, snics in psutil.net_if_addrs().items():
        # print(f"interface={interface!r} snics={snics!r}")
        for snic in snics:
            # if snic.family == family:
            yield (
                interface,
                snic.family,
                snic.address,
            )


async def interpret_command_line_and_start(dtps: DTPSServer, args: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument("--tcp-port", type=int, default=None, required=False)
    parser.add_argument("--tcp-host", required=False, default="0.0.0.0")
    parser.add_argument("--unix-path", required=False, default=None)
    parser.add_argument("--no-alternatives", default=False, action="store_true")
    parser.add_argument("--tunnel", required=False, default=None, help="cloudflare credentials")
    parser.add_argument("--advertise", action="append", help="extra advertisement URLS")
    parser.add_argument("--register-switchboard", default=None, help="Switchboard to register to")
    parser.add_argument("--register-topic", default=None, help="Topic name on which to register.")
    parser.add_argument(
        "--register-namespace",
        default=None,
        help="Prefix of topics to register on switchboard. E.g. --register-namespace=node  only registers node/*",
    )

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
    no_alternatives = parsed.no_alternatives

    tunnel = parsed.tunnel
    registrations: List[Registration] = []
    if parsed.register_switchboard is not None:
        switchboard_url = URLIndexer(parse_url_unescape(parsed.register_switchboard))
        if parsed.register_topic is None:
            msg = "Please specify --register-topic"
            logger.error(msg)
            sys.exit(msg)

        if parsed.register_namespace is not None:
            namespace = TopicNameV.root()
        else:
            namespace = TopicNameV.from_dash_sep(parsed.register_namespace)

        registrations.append(
            Registration(
                switchboard_url=switchboard_url,
                topic=TopicNameV.from_dash_sep(parsed.register_topic),
                namespace=namespace,
            )
        )

    dtps.add_registrations(registrations)
    async with app_start(
        dtps,
        tcp=tcp,
        unix_path=unix_path,
        tunnel=tunnel,
        no_alternatives=no_alternatives,
        extra_advertise=parsed.advertise,
    ):
        await never.wait()


@asynccontextmanager
async def app_start(
    s: DTPSServer,
    tcp: Optional[Tuple[str, int]] = None,
    unix_path: Optional[str] = None,
    tunnel: Optional[str] = None,
    no_alternatives: bool = False,
    extra_advertise: Optional[List[str]] = None,
) -> AsyncIterator[None]:
    runner = web.AppRunner(s.app)
    await runner.setup()

    tunnel_process = None

    available_urls: List[str] = []
    if tcp is not None:
        tcp_host, port = tcp
        the_url0 = f"http://{tcp_host}:{port}/"
        logger.info(f"Starting TCP server - the URL is {the_url0!r}")

        tcp_site = web.TCPSite(runner, tcp_host, port)
        await tcp_site.start()

        if tcp_host != "0.0.0.0":
            available_urls.append(the_url0)

        else:
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

            add_weird_addresses = False
            # add a weird address
            if add_weird_addresses:
                # TODO: add a non-existent path
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
                the_url = f"{the_url}/wrong/path/"
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

        if tunnel is not None:
            # run the cloudflare tunnel
            with open(tunnel) as f:
                data = json.load(f)

            tunnel_name = data["TunnelName"]
            cmd = [
                "cloudflared",
                "tunnel",
                "run",
                "--cred-file",
                tunnel,
                "--url",
                f"http://127.0.0.1:{port}/",
                tunnel_name,
            ]

            # run this in a subprocess using asyncio
            logger.info(f"starting cloudflared tunnel - {cmd!r}")
            tunnel_process = await asyncio.create_subprocess_exec(*cmd)

            #  cloudflared tunnel run --cred-file test-dtps1-tunnel.json --url 127.0.0.1:8000 test-dtps1

    else:
        if tunnel is not None:
            logger.error("cannot start cloudflared tunnel without TCP server")
            sys.exit(1)
        logger.info("not starting TCP server. Use --tcp-port to start one.")

    unix_paths: List[str] = []

    tmpdir = tempfile.gettempdir()
    unix_paths.append(os.path.join(tmpdir, f"dtps-{s.node_id}"))
    if unix_path is not None:
        unix_paths.append(unix_path)

    for up in unix_paths:
        dn = os.path.dirname(up)
        os.makedirs(dn, exist_ok=True)

        path = up.replace("/", "%2F")
        the_url = f"http+unix://{path}/"

        logger.info(f"starting Unix server on path {up!r} - the URL is {the_url!r}")
        unix_site = web.UnixSite(runner, up)
        await unix_site.start()

        available_urls.append(the_url)

    if not available_urls:
        msg = "Please specify at least one of --tcp-port or --unix-path"
        logger.error(msg)
        sys.exit(1)

    if extra_advertise is not None:
        available_urls.extend(extra_advertise)

    if not no_alternatives:
        for url in available_urls:
            s.add_available_url(url)
        logger.info("available URLs\n" + "".join("* " + _ + "\n" for _ in available_urls))
    # wait for finish signal
    try:
        yield
    finally:
        if tunnel_process is not None:
            logger.info("terminating cloudflared tunnel")
            tunnel_process.terminate()
            await tunnel_process.wait()
        await runner.cleanup()
