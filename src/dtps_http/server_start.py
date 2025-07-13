"""Server start."""

__all__ = ["ServerWrapped", "app_start", "interpret_command_line_and_start"]

import argparse
import asyncio
import json
import socket
import sys
import tempfile
from asyncio import Event
from collections.abc import Iterator, Sequence
from pathlib import Path
from socket import AddressFamily
from types import TracebackType
from typing import cast

import psutil
from aiohttp import ClientResponseError
from aiohttp.web import AppRunner, TCPSite, UnixSite

from dtps_http import logger
from dtps_http.client import DTPSClient
from dtps_http.server import DTPSServer
from dtps_http.structures import Registration
from dtps_http.types_ import TopicNameV, URLString
from dtps_http.urls import (
    URLIndexer,
    make_http_unix_url,
    parse_url_unescape,
    url_to_string,
)


class ServerWrapped:
    """Server wrapped."""

    def __init__(
        self,
        server: DTPSServer,
        runner: AppRunner,
        tunnel_process: asyncio.subprocess.Process | None,
        unix_paths_to_clean_up: list[str],
    ) -> None:
        """Initialize server wrapped."""
        self.server = server
        self.runner = runner
        self.tunnel_process = tunnel_process
        self.unix_paths_to_clean_up = unix_paths_to_clean_up

    async def __aenter__(self) -> DTPSServer:
        """Enter asynchronously."""
        await self.server.started.wait()
        return self.server

    async def __aexit__(
        self,
        _: type[BaseException] | None,
        __: BaseException | None,
        ___: TracebackType | None,
    ) -> None:
        """Exit asynchronously."""
        await self.aclose()

    async def aclose(self) -> None:
        """Close asynchronously."""
        await self.server.aclose()
        for unix_path_to_clean_up in self.unix_paths_to_clean_up:
            path = Path(unix_path_to_clean_up)
            if path.exists():
                path.unlink()
        if self.tunnel_process is not None:
            logger.info("Terminating cloudflared tunnel...")
            self.tunnel_process.terminate()
        self.server.logger.debug("Closing runner...")
        try:
            future = self.runner.shutdown()
            await asyncio.wait_for(future, 2)
        except asyncio.exceptions.TimeoutError:
            logger.warning("Timeout waiting for runner cleanup.")
        self.server.logger.debug("Closing runner: done")


async def app_start(
    server: DTPSServer,
    /,
    *,
    tcps: Sequence[tuple[str, int]] = (),
    unix_paths: Sequence[str] = (),
    tunnel: str | None = None,
    no_alternatives: bool = False,
    extra_advertise: list[URLString] | None = None,
) -> ServerWrapped:
    """Start app."""
    runner = AppRunner(server.app)
    await runner.setup()
    tunnel_process = None
    available_urls: list[URLString] = []
    for tcp in tcps:
        tcp_host, port = tcp
        tcp_site = TCPSite(runner, tcp_host, port)
        await tcp_site.start()
        if port == 0:
            socket_name = tcp_site._server.sockets[0].getsockname()
            port = socket_name[1]
        the_url0 = cast(URLString, f"http://{tcp_host}:{port}/")
        logger.info(f"Starting TCP server - the URL is {the_url0!r}")
        if tcp_host == "0.0.0.0":
            available_urls = get_available_urls(available_urls, port)
        else:
            available_urls.append(the_url0)
        if tunnel is not None:
            # Run the cloudflare tunnel
            path = Path(tunnel)
            with path.open() as file:
                data = json.load(file)
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
            # Run this in a subprocess using asyncio
            logger.info(f"starting cloudflared tunnel - {cmd!r}")
            tunnel_process = await asyncio.create_subprocess_exec(*cmd)
            # cloudflared tunnel run --cred-file test-dtps1-tunnel.json
            # --url 127.0.0.1:8000 test-dtps1
    if not tcps and tunnel is not None:
        logger.exception("cannot start cloudflared tunnel without TCP server")
        sys.exit(1)
    unix_paths = list(unix_paths)
    tmpdir = tempfile.gettempdir()
    path = Path(tmpdir) / f"dtps-{server.node_id}"
    path_string = path.as_posix()
    unix_paths.append(path_string)
    for unix_path in unix_paths:
        available_url = await get_available_url_from_unix_path(
            unix_path,
            runner,
        )
        available_urls.append(available_url)
    if not available_urls:
        logger.exception(
            "Please specify at least one of --tcp-port or --unix-path",
        )
        sys.exit(1)
    if extra_advertise is not None:
        available_urls.extend(extra_advertise)
    if not no_alternatives:
        for url in sorted(available_urls):
            await server.add_available_url(url)
        available_urls_string = "".join(
            "* " + available_url + "\n" for available_url in available_urls
        )
        logger.info("Available URLs:\n%s", available_urls_string)
    await server.started.wait()
    return ServerWrapped(server, runner, tunnel_process, unix_paths)


async def get_available_url_from_unix_path(
    unix_path: str,
    runner: AppRunner,
) -> URLString:
    if ("%" in unix_path) or not unix_path:
        message = f"Unix path {unix_path!r} is invalid."
        raise Exception(message)
    url0 = make_http_unix_url(unix_path)
    path = Path(unix_path)
    if path.exists():
        try:
            async with DTPSClient.create(
                nickname="none",
                shutdown_event=None,
            ) as client:
                try:
                    await client.get_metadata(url0)
                except ClientResponseError:
                    # TODO: Check 404
                    pass
                else:
                    logger.exception(
                        "There is already a node listening at the path %s",
                        unix_path,
                    )
                    sys.exit(1)
        except Exception as exception:
            logger.exception(exception)
        path.unlink()
    logger.info("Starting Unix server on path %s", unix_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    unix_site = UnixSite(runner, unix_path)
    await unix_site.start()
    return url_to_string(url0)


def get_available_urls(
    available_urls: list[URLString],
    port: int,
) -> list[URLString]:
    for _, family, address in get_ip_addresses():
        if family != socket.AF_INET:
            continue
        if address.startswith("127."):
            continue
        the_url = cast(URLString, f"http://{address}:{port}/")
        available_urls.append(the_url)
    host_name = socket.gethostname()
    the_url = cast(URLString, f"http://{host_name}:{port}/")
    available_urls.append(the_url)
    add_weird_addresses = False
    # Add a weird address: for testing purposes
    if add_weird_addresses:
        the_url = cast(URLString, f"http://8.8.12.2:{port}/")
        available_urls.append(the_url)
        # Add a non-existente hostname
        the_url = cast(URLString, f"http://dewde.invalid.com:{port}/")
        available_urls.append(the_url)
        # Add a wrong port
        the_url = cast(URLString, "http://localhost:12345/")
        available_urls.append(the_url)
        # Add a wrong host
        the_url = cast(URLString, "http://google.com/")
        available_urls.append(the_url)
        the_url = cast(URLString, f"{the_url}/wrong/path/")
        available_urls.append(the_url)
    for _, family, address in get_ip_addresses():
        if family != socket.AF_INET6:
            continue
        if address.startswith(("::1", "fe80:")):
            continue
        the_url = cast(URLString, f"http://[{address}]:{port}/")
        available_urls.append(the_url)
    return available_urls


def get_ip_addresses() -> Iterator[tuple[str, AddressFamily, str]]:
    addresses = psutil.net_if_addrs()
    for interface, snics in addresses.items():
        for snic in snics:
            yield (interface, snic.family, snic.address)


async def interpret_command_line_and_start(
    dtps: DTPSServer,
    args: list[str] | None = None,
) -> None:
    """Interpret command line and start."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--tcp-port", type=int, default=None, required=False)
    parser.add_argument("--tcp-host", required=False, default="0.0.0.0")
    parser.add_argument("--unix-path", required=False, default=None)
    parser.add_argument(
        "--no-alternatives",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--tunnel",
        required=False,
        default=None,
        help="cloudflare credentials",
    )
    parser.add_argument(
        "--advertise",
        action="append",
        help="extra advertisement URLS",
    )
    parser.add_argument(
        "--register-switchboard",
        default=None,
        help="Switchboard to register to",
    )
    parser.add_argument(
        "--register-as",
        default=None,
        help="Topic name on which to register.",
    )
    parser.add_argument(
        "--register-namespace",
        default=None,
        help="Prefix of topics to register on switchboard. E.g. "
        "--register-namespace=node  only registers "
        "node/*",
    )
    parsed = parser.parse_args(args)
    if parsed.tcp_port is None and parsed.unix_path is None:
        message = (
            "Please specify at least one of `--tcp-port` or `--unix-path`."
        )
        logger.exception(message)
        sys.exit(message)
    tcps: list[tuple[str, int]] = []
    if parsed.tcp_port is not None:
        tcps.append((parsed.tcp_host, parsed.tcp_port))
    unix_paths = [parsed.unix_path] if parsed.unix_path is not None else []
    never = Event()
    no_alternatives = parsed.no_alternatives
    tunnel = parsed.tunnel
    registrations: list[Registration] = []
    if parsed.register_switchboard is not None:
        url = parse_url_unescape(parsed.register_switchboard)
        switchboard_url = URLIndexer(url)
        if parsed.register_as is None:
            message = "Please specify --register-as"
            logger.exception(message)
            sys.exit(message)
        topic_name = TopicNameV.from_dash_sep(parsed.register_as)
        namespace = TopicNameV.from_dash_sep_or_none(parsed.register_namespace)
        registration = Registration(switchboard_url, topic_name, namespace)
        registrations.append(registration)
    dtps.add_registrations(registrations)
    server_wrapped = await app_start(
        dtps,
        tcps=tcps,
        unix_paths=unix_paths,
        tunnel=tunnel,
        no_alternatives=no_alternatives,
        extra_advertise=parsed.advertise,
    )
    async with server_wrapped:
        await never.wait()
