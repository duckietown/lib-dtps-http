import argparse
import asyncio
import json
import os
import socket
import sys
import tempfile
from socket import AddressFamily
from typing import cast, Iterator, List, Optional, Sequence, Tuple

import psutil
from aiohttp import web

from . import logger
from .server import DTPSServer
from .structures import Registration
from .types import TopicNameV, URLString
from .urls import make_http_unix_url, parse_url_unescape, url_to_string, URLIndexer

__all__ = [
    "ServerWrapped",
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
    parser.add_argument("--register-as", default=None, help="Topic name on which to register.")
    parser.add_argument(
        "--register-namespace",
        default=None,
        help="Prefix of topics to register on switchboard. E.g. --register-namespace=node  only registers "
        "node/*",
    )

    parsed = parser.parse_args(args)

    if parsed.tcp_port is None and parsed.unix_path is None:
        msg = "Please specify at least one of --tcp-port or --unix-path"
        logger.error(msg)
        sys.exit(msg)

    tcps = []
    if parsed.tcp_port is not None:
        tcps.append((parsed.tcp_host, parsed.tcp_port))

    if parsed.unix_path is not None:
        unix_paths = [parsed.unix_path]
    else:
        unix_paths = []

    never = asyncio.Event()
    no_alternatives = parsed.no_alternatives

    tunnel = parsed.tunnel
    registrations: List[Registration] = []
    if parsed.register_switchboard is not None:
        switchboard_url = URLIndexer(parse_url_unescape(parsed.register_switchboard))
        if parsed.register_as is None:
            msg = "Please specify --register-as"
            logger.error(msg)
            sys.exit(msg)

        namespace = TopicNameV.from_dash_sep_or_none(parsed.register_namespace)

        registrations.append(
            Registration(
                switchboard_url=switchboard_url,
                topic=TopicNameV.from_dash_sep(parsed.register_as),
                namespace=namespace,
            )
        )

    dtps.add_registrations(registrations)

    s = await app_start(
        dtps,
        tcps=tcps,
        unix_paths=unix_paths,
        tunnel=tunnel,
        no_alternatives=no_alternatives,
        extra_advertise=parsed.advertise,
    )
    async with s:
        await never.wait()


class ServerWrapped:
    def __init__(
        self, server: DTPSServer, runner: web.AppRunner, tunnel_process: Optional[asyncio.subprocess.Process]
    ) -> None:
        self.server = server
        self.runner = runner
        self.tunnel_process = tunnel_process

    async def __aenter__(self) -> DTPSServer:
        await self.server.started.wait()
        return self.server

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self.server.aclose()

        if self.tunnel_process is not None:
            logger.info("terminating cloudflared tunnel")
            self.tunnel_process.terminate()
            # await self.tunnel_process.wait()

        self.server.logger.debug("closing runner")

        try:
            await asyncio.wait_for(self.runner.shutdown(), timeout=2)
        except asyncio.exceptions.TimeoutError:
            logger.warning("timeout waiting for runner cleanup")
        self.server.logger.debug("closing runner: done")


async def app_start(
    s: DTPSServer,
    *,
    tcps: Sequence[Tuple[str, int]] = (),
    unix_paths: Sequence[str] = (),
    tunnel: Optional[str] = None,
    no_alternatives: bool = False,
    extra_advertise: Optional[List[URLString]] = None,
) -> ServerWrapped:
    runner = web.AppRunner(s.app)
    await runner.setup()

    tunnel_process = None

    available_urls: List[URLString] = []
    for tcp in tcps:
        tcp_host, port = tcp

        tcp_site = web.TCPSite(runner, tcp_host, port)

        await tcp_site.start()
        port = tcp_site._server.sockets[0].getsockname()[1]
        the_url0 = cast(URLString, f"http://{tcp_host}:{port}/")
        logger.info(f"Starting TCP server - the URL is {the_url0!r}")

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

                the_url = cast(URLString, f"http://{address}:{port}/")
                available_urls.append(the_url)

            the_url = cast(URLString, f"http://{socket.gethostname()}:{port}/")
            available_urls.append(the_url)

            add_weird_addresses = False
            # add a weird address: for testing purposes
            if add_weird_addresses:
                the_url = cast(URLString, f"http://8.8.12.2:{port}/")
                available_urls.append(the_url)
                # add a non-existente hostname
                the_url = cast(URLString, f"http://dewde.invalid.com:{port}/")
                available_urls.append(the_url)
                # add a wrong port
                the_url = cast(URLString, f"http://localhost:12345/")
                available_urls.append(the_url)
                # add a wrong host
                the_url = cast(URLString, f"http://google.com/")
                available_urls.append(the_url)
                the_url = cast(URLString, f"{the_url}/wrong/path/")
                available_urls.append(the_url)

            for interface, family, address in get_ip_addresses():
                if family != socket.AF_INET6:
                    continue

                if address.startswith("::1") or address.startswith("fe80:"):
                    continue

                the_url = cast(URLString, f"http://[{address}]:{port}/")

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

    if not tcps and tunnel is not None:
        logger.error("cannot start cloudflared tunnel without TCP server")
        sys.exit(1)
    # logger.info("not starting TCP server. Use --tcp-port to start one.")

    unix_paths = list(unix_paths)

    tmpdir = tempfile.gettempdir()
    unix_paths.append(os.path.join(tmpdir, f"dtps-{s.node_id}"))

    for up in unix_paths:
        if ("%" in up) or not up:
            msg = f"Unix path {up!r} is invalid"
            raise Exception(msg)

        the_url = make_http_unix_url(up)

        logger.info(f"starting Unix server on path {up!r} - the URL is {the_url!r}")

        dn = os.path.dirname(up)
        os.makedirs(dn, exist_ok=True)

        unix_site = web.UnixSite(runner, up)
        await unix_site.start()

        available_urls.append(url_to_string(the_url))

    if not available_urls:
        msg = "Please specify at least one of --tcp-port or --unix-path"
        logger.error(msg)
        sys.exit(1)

    if extra_advertise is not None:
        available_urls.extend(extra_advertise)

    if not no_alternatives:
        for url in sorted(available_urls):
            await s.add_available_url(url)
        logger.info("available URLs\n" + "".join("* " + _ + "\n" for _ in available_urls))

    await s.started.wait()
    return ServerWrapped(s, runner, tunnel_process)
