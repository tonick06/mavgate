"""Put a gateway between a MAVLink program and the radio link.

One bridge runs at each end:

    autopilot <-UDP-> bridge <== gateway link ==> bridge <-UDP-> ground station

Each bridge listens for MAVLink on a local UDP port, cuts every datagram into
frames (ArduPilot packs several into one), classifies each frame with
`classify` and submits it to its gateway. Frames the gateway delivers go out
of the same local port to the MAVLink program.

The vehicle's system id is used as the gateway flow, so vehicles sharing one
link get a fair share each. Uplink traffic from a ground station is one flow
(its own system id); per-target fairness would need the target id of every
message type and is not done.

The local MAVLink peer is either given (`mavlink_peer`), which suits a ground
station listening on 14550, or learned from the first datagram, which suits
an autopilot started with `--serial0 udpclient:<bridge address>`.

Command line, one per end:

    python -m mavgate.mavlink_bridge --mavlink-listen 127.0.0.1:14551 \\
        --link-local 0.0.0.0:14600 --link-remote 10.0.0.2:14600
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass

from .classify import classify, parse_header, split_frames
from .gateway import GatewayConfig, Kind
from .net_asyncio import Address, UdpGateway, ignore_icmp_resets, open_udp_gateway

log = logging.getLogger(__name__)


@dataclass
class BridgeStats:
    frames_in: int = 0  # from the local MAVLink program into the link
    frames_out: int = 0  # from the link to the local MAVLink program
    junk_bytes: int = 0  # bytes that were not part of a whole frame
    no_peer: int = 0  # frames from the link dropped because no local peer is known
    foreign: int = 0  # datagrams from someone other than the local peer


class _MavlinkSide(asyncio.DatagramProtocol):
    def __init__(self, peer: Address | None, stats: BridgeStats) -> None:
        self.peer = peer
        self.stats = stats
        self.transport: asyncio.DatagramTransport | None = None
        self.link: UdpGateway | None = None
        self.ttl: float | None = None
        self.closed: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        assert isinstance(transport, asyncio.DatagramTransport)
        self.transport = transport
        ignore_icmp_resets(transport)

    def datagram_received(self, data: bytes, addr: Address) -> None:
        if self.peer is None:
            self.peer = addr
            log.info("learned MAVLink peer %s", addr)
        elif addr != self.peer:
            self.stats.foreign += 1
            return
        if self.link is None:
            return
        frames, _ = split_frames(data)  # a partial tail cannot be completed: UDP
        self.stats.junk_bytes += len(data) - sum(map(len, frames))
        for frame in frames:
            kind, key = classify(frame)
            header = parse_header(frame)
            flow = header[0] if header is not None else 0
            ttl = self.ttl if kind is Kind.CRITICAL else None
            self.link.gateway.submit(frame, kind, key, ttl, flow)
            self.stats.frames_in += 1

    def deliver(self, kind: Kind, key: int | None, payload: bytes) -> None:
        if self.peer is None:
            self.stats.no_peer += 1
        elif self.transport is not None and not self.transport.is_closing():
            self.transport.sendto(payload, self.peer)
            self.stats.frames_out += 1

    def error_received(self, exc: Exception) -> None:
        log.debug("MAVLink socket error (treated as loss): %r", exc)

    def connection_lost(self, exc: Exception | None) -> None:
        if not self.closed.done():
            self.closed.set_result(None)


class MavlinkBridge:
    """A running bridge. `link.gateway` is the gateway, for stats and health."""

    def __init__(self, side: _MavlinkSide, link: UdpGateway) -> None:
        self._side = side
        self.link = link
        self.stats = side.stats

    @property
    def mavlink_addr(self) -> Address:
        """Where the local MAVLink program should send."""
        assert self._side.transport is not None
        addr: Address = self._side.transport.get_extra_info("sockname")
        return addr

    @property
    def mavlink_peer(self) -> Address | None:
        return self._side.peer

    async def close(self) -> None:
        if self._side.transport is not None:
            self._side.transport.abort()  # see the notes in net_asyncio
        await asyncio.gather(self.link.close(), self._side.closed)


async def open_bridge(
    link_remote: Address,
    *,
    mavlink_listen: Address,
    link_local: Address | None = None,
    mavlink_peer: Address | None = None,
    config: GatewayConfig | None = None,
    ttl: float | None = None,
) -> MavlinkBridge:
    """Start a bridge. `ttl` overrides the gateway's default for critical frames."""
    loop = asyncio.get_running_loop()
    side = _MavlinkSide(mavlink_peer, BridgeStats())
    side.ttl = ttl
    await loop.create_datagram_endpoint(lambda: side, local_addr=mavlink_listen)
    side.link = await open_udp_gateway(
        link_remote, link_local, config=config, on_deliver=side.deliver
    )
    return MavlinkBridge(side, side.link)


# ---- command line ---------------------------------------------------------


def _addr(text: str) -> Address:
    host, _, port = text.rpartition(":")
    if not host or not port.isdigit():
        raise argparse.ArgumentTypeError(f"expected host:port, got {text!r}")
    return (host.strip("[]"), int(port))


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m mavgate.mavlink_bridge",
        description="Carry MAVLink over a mavgate link.",
    )
    p.add_argument("--mavlink-listen", type=_addr, required=True,
                   help="local host:port the MAVLink program talks to")
    p.add_argument("--mavlink-peer", type=_addr,
                   help="send MAVLink here (default: learn from the first packet)")
    p.add_argument("--link-local", type=_addr, default=("0.0.0.0", 0),
                   help="host:port for the gateway link")
    p.add_argument("--link-remote", type=_addr, required=True,
                   help="host:port of the bridge at the other end")
    p.add_argument("--rate", type=float, help="link budget in bytes per second")
    p.add_argument("--ttl", type=float, help="seconds a critical frame may wait")
    p.add_argument("--adaptive", action="store_true", help="adapt the telemetry rate")
    p.add_argument("--report", type=float, default=5.0, help="seconds between stats lines")
    return p


async def _main(args: argparse.Namespace) -> None:
    cfg = GatewayConfig(adaptive_telemetry=args.adaptive)
    if args.rate is not None:
        cfg.rate_bytes_per_s = args.rate
    bridge = await open_bridge(
        args.link_remote,
        mavlink_listen=args.mavlink_listen,
        link_local=args.link_local,
        mavlink_peer=args.mavlink_peer,
        config=cfg,
        ttl=args.ttl,
    )
    print(f"MAVLink on {bridge.mavlink_addr}, link {bridge.link.local_addr} -> "
          f"{args.link_remote}", flush=True)
    try:
        while True:
            await asyncio.sleep(args.report)
            health = bridge.link.gateway.link_health()
            print({**vars(bridge.stats), **health}, flush=True)
    finally:
        await bridge.close()


def main(argv: list[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        asyncio.run(_main(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
