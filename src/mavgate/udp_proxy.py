"""A lossy UDP proxy: `netsim.Channel` impairments applied to real datagrams.

Put it between two UDP endpoints to get loss, delay, jitter, duplication,
corruption, a bandwidth cap and scheduled outages on a real socket path. It
reuses `netsim.Channel`, so a link here behaves the same way as the link of
the same name in the simulator. It is a stand-in for `tc netem` that also runs
on Windows and macOS.

    air --> a_addr [proxy] b_addr <-- ground

Each side learns its peer from the first datagram it receives, unless given
one, and ignores datagrams from anyone else after that.

Impairment decisions come from a seeded generator per direction. Real timing
still decides how many packets each direction carries, so a run is not
replayed bit for bit the way a simulator run is.

Run it from the command line:

    python -m mavgate.udp_proxy --a-listen 127.0.0.1:14560 \\
        --b-listen 127.0.0.1:14561 --loss 0.3 --delay 0.08 --jitter 0.03
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Any

from .net_asyncio import Address, AsyncioClock, ignore_icmp_resets
from .netsim import Channel, LinkParams

log = logging.getLogger(__name__)


class _Side(asyncio.DatagramProtocol):
    """One listening socket of the proxy."""

    def __init__(self, peer: Address | None) -> None:
        self.peer = peer
        self.transport: asyncio.DatagramTransport | None = None
        self.channel: Channel | None = None  # where datagrams from this side go
        self.foreign = 0  # datagrams from someone other than the peer
        self.no_peer = 0  # datagrams to this side dropped because no peer is known yet
        self.closed: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        assert isinstance(transport, asyncio.DatagramTransport)
        self.transport = transport
        ignore_icmp_resets(transport)

    def datagram_received(self, data: bytes, addr: Address) -> None:
        if self.peer is None:
            self.peer = addr
            log.info("learned peer %s", addr)
        elif addr != self.peer:
            self.foreign += 1
            return
        if self.channel is not None:
            self.channel.send(data)

    def send_to_peer(self, data: bytes) -> None:
        if self.peer is None:
            self.no_peer += 1
        elif self.transport is not None and not self.transport.is_closing():
            self.transport.sendto(data, self.peer)

    def error_received(self, exc: Exception) -> None:
        log.debug("proxy socket error (treated as loss): %r", exc)

    def connection_lost(self, exc: Exception | None) -> None:
        if not self.closed.done():
            self.closed.set_result(None)


class LossyUdpProxy:
    """Two UDP sockets joined by two impaired channels. Build with `start`."""

    def __init__(
        self,
        side_a: _Side,
        side_b: _Side,
        a_to_b: Channel,
        b_to_a: Channel,
        clocks: tuple[AsyncioClock, AsyncioClock],
    ) -> None:
        self._a = side_a
        self._b = side_b
        self.a_to_b = a_to_b
        self.b_to_a = b_to_a
        self._clocks = clocks

    @classmethod
    async def start(
        cls,
        a_to_b: LinkParams,
        b_to_a: LinkParams | None = None,
        *,
        seed: int = 0,
        a_listen: Address = ("127.0.0.1", 0),
        b_listen: Address = ("127.0.0.1", 0),
        a_peer: Address | None = None,
        b_peer: Address | None = None,
    ) -> LossyUdpProxy:
        """Listen on `a_listen` and `b_listen` and start relaying."""
        loop = asyncio.get_running_loop()
        side_a, side_b = _Side(a_peer), _Side(b_peer)
        await loop.create_datagram_endpoint(lambda: side_a, local_addr=a_listen)
        await loop.create_datagram_endpoint(lambda: side_b, local_addr=b_listen)
        clock_ab = AsyncioClock(seed=seed, loop=loop)
        clock_ba = AsyncioClock(seed=seed + 1, loop=loop)
        ch_ab = Channel(clock_ab, a_to_b, side_b.send_to_peer)
        ch_ba = Channel(clock_ba, b_to_a or a_to_b, side_a.send_to_peer)
        side_a.channel = ch_ab
        side_b.channel = ch_ba
        return cls(side_a, side_b, ch_ab, ch_ba, (clock_ab, clock_ba))

    @property
    def a_addr(self) -> Address:
        """Where the A end should send."""
        return self._sockname(self._a)

    @property
    def b_addr(self) -> Address:
        """Where the B end should send."""
        return self._sockname(self._b)

    @property
    def a_peer(self) -> Address | None:
        return self._a.peer

    @property
    def b_peer(self) -> Address | None:
        return self._b.peer

    def summary(self) -> dict[str, Any]:
        return {
            "a_to_b": vars(self.a_to_b.stats),
            "b_to_a": vars(self.b_to_a.stats),
            "foreign": self._a.foreign + self._b.foreign,
            "no_peer": self._a.no_peer + self._b.no_peer,
        }

    async def close(self) -> None:
        """Drop anything still in flight and close both sockets."""
        for clock in self._clocks:
            clock.close()
        for side in (self._a, self._b):
            if side.transport is not None:
                side.transport.abort()  # see the notes in net_asyncio
        await asyncio.gather(self._a.closed, self._b.closed)

    @staticmethod
    def _sockname(side: _Side) -> Address:
        assert side.transport is not None
        addr: Address = side.transport.get_extra_info("sockname")
        return addr


# ---- command line ---------------------------------------------------------


def _addr(text: str) -> Address:
    host, _, port = text.rpartition(":")
    if not host or not port.isdigit():
        raise argparse.ArgumentTypeError(f"expected host:port, got {text!r}")
    return (host.strip("[]"), int(port))


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m mavgate.udp_proxy",
        description="Relay UDP between two endpoints through a lossy link.",
    )
    p.add_argument("--a-listen", type=_addr, required=True, help="host:port the A end sends to")
    p.add_argument("--b-listen", type=_addr, required=True, help="host:port the B end sends to")
    p.add_argument("--a-peer", type=_addr, help="A end's address (default: learn it)")
    p.add_argument("--b-peer", type=_addr, help="B end's address (default: learn it)")
    p.add_argument("--loss", type=float, default=0.0, help="drop probability, both ways")
    p.add_argument("--delay", type=float, default=0.05, help="one-way delay in seconds")
    p.add_argument("--jitter", type=float, default=0.0, help="extra random delay in seconds")
    p.add_argument("--dup", type=float, default=0.0, help="duplication probability")
    p.add_argument("--corrupt", type=float, default=0.0, help="bit-flip probability")
    p.add_argument("--bandwidth", type=float, help="bytes per second, each way")
    p.add_argument(
        "--outage",
        action="append",
        default=[],
        metavar="START:END",
        help="silence both ways between these seconds after start; repeatable",
    )
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--report", type=float, default=5.0, help="seconds between stats lines")
    return p


def _outage(text: str) -> tuple[float, float]:
    start, _, end = text.partition(":")
    return (float(start), float(end))


async def _main(args: argparse.Namespace) -> None:
    link = LinkParams(
        loss=args.loss,
        delay=args.delay,
        jitter=args.jitter,
        dup=args.dup,
        corrupt=args.corrupt,
        bandwidth=args.bandwidth,
        outages=[_outage(o) for o in args.outage],
    )
    proxy = await LossyUdpProxy.start(
        link,
        seed=args.seed,
        a_listen=args.a_listen,
        b_listen=args.b_listen,
        a_peer=args.a_peer,
        b_peer=args.b_peer,
    )
    print(f"relaying {proxy.a_addr} <-> {proxy.b_addr} with {link}", flush=True)
    try:
        while True:
            await asyncio.sleep(args.report)
            print(proxy.summary(), flush=True)
    finally:
        await proxy.close()


def main(argv: list[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        asyncio.run(_main(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
