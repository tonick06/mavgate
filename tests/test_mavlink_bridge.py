"""The MAVLink bridge on real sockets, with frames built by pymavlink.

    autopilot <-> air bridge <-> lossy proxy <-> ground bridge <-> ground station
"""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from pymavlink.dialects.v20 import common as mavlink2

from mavgate.mavlink_bridge import MavlinkBridge, open_bridge
from mavgate.netsim import LinkParams
from mavgate.udp_proxy import LossyUdpProxy

from test_net_asyncio import LOCAL, eventually, run


def mav(sysid: int) -> Any:
    return mavlink2.MAVLink(None, srcSystem=sysid, srcComponent=1)


class Endpoint:
    """A plain UDP socket playing autopilot or ground station."""

    def __init__(self) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(LOCAL)
        self.sock.setblocking(False)
        self.parser = mavlink2.MAVLink(None)
        self.parser.robust_parsing = True
        self.got: list[Any] = []

    @property
    def addr(self) -> tuple[str, int]:
        name: tuple[str, int] = self.sock.getsockname()
        return name

    def poll(self) -> list[Any]:
        while True:
            try:
                data = self.sock.recv(4096)
            except (BlockingIOError, ConnectionResetError):
                break
            self.got.extend(m for m in self.parser.parse_buffer(data) or [] if m.get_type() != "BAD_DATA")
        return self.got

    def of_type(self, name: str) -> list[Any]:
        return [m for m in self.poll() if m.get_type() == name]


@dataclass
class Rig:
    autopilot: Endpoint
    gcs: Endpoint
    air: MavlinkBridge
    ground: MavlinkBridge
    proxy: LossyUdpProxy


@asynccontextmanager
async def rig(link: LinkParams) -> AsyncIterator[Rig]:
    proxy = await LossyUdpProxy.start(link, seed=5)
    autopilot, gcs = Endpoint(), Endpoint()
    air = await open_bridge(proxy.a_addr, link_local=LOCAL, mavlink_listen=LOCAL)
    ground = await open_bridge(
        proxy.b_addr, link_local=LOCAL, mavlink_listen=LOCAL, mavlink_peer=gcs.addr
    )
    try:
        yield Rig(autopilot, gcs, air, ground, proxy)
    finally:
        await asyncio.gather(air.close(), ground.close(), proxy.close())
        autopilot.sock.close()
        gcs.sock.close()


def test_frames_flow_both_ways_and_packed_datagrams_are_split() -> None:
    async def scenario() -> None:
        async with rig(LinkParams(delay=0.01)) as r:
            ap = mav(1)
            packed = (
                ap.heartbeat_encode(2, 3, 0, 0, 4).pack(ap)
                + ap.global_position_int_encode(1000, 1, 2, 3, 4, 0, 0, 0, 0).pack(ap)
                + ap.statustext_encode(6, b"PreArm: good").pack(ap)
            )
            r.autopilot.sock.sendto(b"\x00junk" + packed, r.air.mavlink_addr)
            await eventually(lambda: len(r.gcs.poll()) >= 3, timeout=3)
            kinds = sorted(m.get_type() for m in r.gcs.got)
            assert kinds == ["GLOBAL_POSITION_INT", "HEARTBEAT", "STATUSTEXT"]
            assert r.air.stats.junk_bytes == 5

            gcs = mav(255)
            cmd = gcs.command_long_encode(1, 1, 400, 0, 1, 0, 0, 0, 0, 0, 0).pack(gcs)  # arm
            r.gcs.sock.sendto(cmd, r.ground.mavlink_addr)
            await eventually(lambda: len(r.autopilot.of_type("COMMAND_LONG")) == 1)
            assert r.autopilot.of_type("COMMAND_LONG")[0].command == 400

    run(scenario())


def test_parameter_writes_survive_heavy_loss_exactly_once() -> None:
    async def scenario() -> None:
        link = LinkParams(loss=0.3, delay=0.01, jitter=0.01)
        async with rig(link) as r:
            hb = mav(1)  # the air bridge learns the autopilot from its first packet
            r.autopilot.sock.sendto(hb.heartbeat_encode(2, 3, 0, 0, 4).pack(hb), r.air.mavlink_addr)
            await eventually(lambda: r.air.mavlink_peer is not None)
            gcs = mav(255)
            for i in range(20):
                name = f"P{i}".encode()
                frame = gcs.param_set_encode(1, 1, name, float(i), 9).pack(gcs)
                r.gcs.sock.sendto(frame, r.ground.mavlink_addr)
            await eventually(lambda: len(r.autopilot.of_type("PARAM_SET")) == 20, timeout=6)
            await asyncio.sleep(0.2)
            got = sorted(m.param_value for m in r.autopilot.of_type("PARAM_SET"))
            assert got == [float(i) for i in range(20)]  # all of them, each once
            assert r.ground.link.gateway.stats.crit_retx > 0

    run(scenario())


def test_bridge_uses_the_vehicle_id_as_flow() -> None:
    async def scenario() -> None:
        async with rig(LinkParams(delay=0.01)) as r:
            seen: list[int] = []
            original = r.air.link.gateway.submit

            def spy(payload: bytes, kind: Any, key: Any = None, ttl: Any = None, flow: int = 0) -> Any:
                seen.append(flow)
                return original(payload, kind, key, ttl, flow)

            r.air.link.gateway.submit = spy  # type: ignore[method-assign]
            for sysid in (1, 7):
                m = mav(sysid)
                r.autopilot.sock.sendto(m.heartbeat_encode(2, 3, 0, 0, 4).pack(m), r.air.mavlink_addr)
            await eventually(lambda: len(seen) == 2)
            assert seen == [1, 7]

    run(scenario())
