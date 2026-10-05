"""Run a gateway over a real UDP socket with asyncio.

This is the adapter between the pure core and the outside world. It is the
only place, together with `udp_proxy`, that touches the event loop or the
network. The core is used unchanged:

* `AsyncioClock` gives the gateway `now` and `call_later` on the event loop's
  monotonic clock, and can cancel everything it scheduled. That is how a
  gateway is stopped, since its send loop reschedules itself forever.
* `open_udp_gateway` opens a UDP socket tied to one peer and builds a
  `Gateway` on it. The socket is connected, so the OS drops datagrams from
  anyone other than the peer.

Socket errors (for example "connection refused" when the peer is not running
yet) are counted and otherwise ignored. To the gateway a missing peer looks
like a lossy link, which it already handles.

Two workarounds for asyncio's Windows (Proactor) UDP transport, both checked
against CPython 3.12:

* After a failed receive it reports the error and never reads again, so one
  ICMP "port unreachable" from a peer that starts late would leave the gateway
  deaf for good. `ignore_icmp_resets` turns those reports off on the socket.
* If a send is still in flight when it is closed, it never calls
  `connection_lost` or closes the socket. `close` therefore uses `abort`,
  which always does. For UDP that only drops queued datagrams, which is loss.
"""

from __future__ import annotations

import asyncio
import logging
import random
import socket
import sys
from types import TracebackType
from typing import Any, Callable

from .gateway import DeliverFn, Gateway, GatewayConfig

log = logging.getLogger(__name__)

Address = tuple[Any, ...]  # (host, port) for IPv4, (host, port, flow, scope) for IPv6


def ignore_icmp_resets(transport: asyncio.BaseTransport) -> None:
    """Stop Windows reporting ICMP "port unreachable" as a receive error.

    Does nothing on other platforms, where asyncio keeps reading after an error.
    """
    if sys.platform != "win32":
        return
    import ctypes
    from ctypes import wintypes

    sio_udp_connreset = 0x9800000C  # _WSAIOW(IOC_VENDOR, 12)
    sock = transport.get_extra_info("socket")
    off = wintypes.BOOL(False)
    returned = wintypes.DWORD(0)
    result = ctypes.windll.ws2_32.WSAIoctl(
        ctypes.c_size_t(sock.fileno()),
        wintypes.DWORD(sio_udp_connreset),
        ctypes.byref(off),
        ctypes.sizeof(off),
        None,
        0,
        ctypes.byref(returned),
        None,
        None,
    )
    if result != 0:
        log.warning("could not disable UDP connection resets on %r", sock)


class AsyncioClock:
    """`Clock` (and `netsim.Scheduler`) backed by an asyncio event loop.

    `now` counts seconds from when the clock was made, so `LinkParams.outages`
    read as offsets from the start, the same as in the simulator.
    """

    def __init__(self, seed: int = 0, loop: asyncio.AbstractEventLoop | None = None) -> None:
        self._loop = loop or asyncio.get_running_loop()
        self._epoch = self._loop.time()
        self._rng = random.Random(seed)
        self._handles: set[asyncio.TimerHandle] = set()
        self._closed = False

    @property
    def now(self) -> float:
        return self._loop.time() - self._epoch

    @property
    def rng(self) -> random.Random:
        return self._rng

    @property
    def pending(self) -> int:
        """Timers scheduled and not yet run."""
        return len(self._handles)

    def call_later(self, delay: float, fn: Callable[..., None], *args: Any) -> None:
        self.call_at(self.now + delay, fn, *args)

    def call_at(self, when: float, fn: Callable[..., None], *args: Any) -> None:
        if self._closed:
            return
        holder: list[asyncio.TimerHandle] = []

        def run() -> None:
            self._handles.discard(holder[0])
            fn(*args)

        handle = self._loop.call_at(self._epoch + when, run)
        holder.append(handle)
        self._handles.add(handle)

    def close(self) -> None:
        """Cancel every pending timer and ignore any scheduled from now on."""
        self._closed = True
        for handle in self._handles:
            handle.cancel()
        self._handles.clear()


class _GatewayProtocol(asyncio.DatagramProtocol):
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.gateway: Gateway | None = None
        self.socket_errors = 0
        self.closed: asyncio.Future[None] = loop.create_future()

    def datagram_received(self, data: bytes, addr: Address) -> None:
        if self.gateway is not None:
            self.gateway.receive(data)

    def error_received(self, exc: Exception) -> None:
        self.socket_errors += 1
        log.debug("udp socket error (treated as loss): %r", exc)

    def connection_lost(self, exc: Exception | None) -> None:
        if not self.closed.done():
            self.closed.set_result(None)


class UdpGateway:
    """A running gateway on a UDP socket. Use `.gateway` for the usual API."""

    def __init__(
        self,
        gateway: Gateway,
        clock: AsyncioClock,
        transport: asyncio.DatagramTransport,
        protocol: _GatewayProtocol,
    ) -> None:
        self.gateway = gateway
        self.clock = clock
        self._transport = transport
        self._protocol = protocol

    @property
    def local_addr(self) -> Address:
        addr: Address = self._transport.get_extra_info("sockname")
        return addr

    @property
    def socket_errors(self) -> int:
        """Send or receive errors from the OS, each treated as a lost packet."""
        return self._protocol.socket_errors

    async def close(self) -> None:
        """Stop the send loop and close the socket. Safe to call twice."""
        self.clock.close()
        self._transport.abort()  # not close(): see the module notes
        await self._protocol.closed

    async def __aenter__(self) -> UdpGateway:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()


async def open_udp_gateway(
    remote_addr: Address,
    local_addr: Address | None = None,
    *,
    sock: socket.socket | None = None,
    config: GatewayConfig | None = None,
    on_deliver: DeliverFn | None = None,
    on_acked: Callable[[int], None] | None = None,
    on_gave_up: Callable[[int, bytes], None] | None = None,
) -> UdpGateway:
    """Start a gateway that talks to the gateway at `remote_addr` over UDP.

    Either bind to `local_addr` (port 0 picks a free port), or pass an already
    bound `sock`, which is useful when both ends must know each other's
    address before either starts.
    """
    loop = asyncio.get_running_loop()
    protocol = _GatewayProtocol(loop)
    peer: Address | None = None  # set when the transport does not know its peer
    if sock is not None:
        if local_addr is not None:
            raise ValueError("pass either local_addr or sock, not both")
        sock.setblocking(False)
        sock.connect(remote_addr)  # the OS now filters out other senders
        peer = sock.getpeername()
        transport, _ = await loop.create_datagram_endpoint(lambda: protocol, sock=sock)
    else:
        transport, _ = await loop.create_datagram_endpoint(
            lambda: protocol, local_addr=local_addr, remote_addr=remote_addr
        )
    ignore_icmp_resets(transport)

    def send_raw(data: bytes) -> None:
        if not transport.is_closing():
            transport.sendto(data, peer)

    clock = AsyncioClock(loop=loop)
    gateway = Gateway(clock, send_raw, config, on_deliver, on_acked, on_gave_up)
    protocol.gateway = gateway  # anything that arrived before this was dropped, like loss
    return UdpGateway(gateway, clock, transport, protocol)
