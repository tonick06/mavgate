"""Loopback tests for the asyncio UDP adapter and the lossy proxy.

These run on real sockets and the real clock, so they are kept short. The
proxy's impairment decisions come from a seeded generator, but how many packets
each direction carries depends on timing, so a run is not replayed bit for bit.
"""

from __future__ import annotations

import asyncio
import socket
from collections.abc import Callable, Coroutine
from typing import Any

from mavgate import protocol as P
from mavgate.gateway import GatewayConfig, Kind
from mavgate.net_asyncio import AsyncioClock, UdpGateway, open_udp_gateway
from mavgate.netsim import LinkParams
from mavgate.udp_proxy import LossyUdpProxy

LOCAL = ("127.0.0.1", 0)
Delivered = list[tuple[Kind, "int | None", bytes]]


def run(coro: Coroutine[Any, Any, None], timeout: float = 10.0) -> None:
    asyncio.run(asyncio.wait_for(coro, timeout))


async def eventually(cond: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not cond():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


def bound_pair() -> tuple[socket.socket, socket.socket]:
    """Two UDP sockets on loopback that know each other's address up front."""
    a = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    b = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    a.bind(LOCAL)
    b.bind(LOCAL)
    return a, b


def recorder(store: Delivered) -> Callable[[Kind, int | None, bytes], None]:
    return lambda kind, key, payload: store.append((kind, key, payload))


async def closing(*things: UdpGateway | LossyUdpProxy) -> None:
    await asyncio.gather(*(t.close() for t in things))


def test_clock_runs_and_cancels_timers() -> None:
    async def scenario() -> None:
        clock = AsyncioClock(seed=1)
        fired: list[str] = []
        clock.call_later(0.01, fired.append, "later")
        clock.call_at(clock.now + 0.02, fired.append, "at")
        clock.call_later(5.0, fired.append, "never")
        await eventually(lambda: len(fired) == 2)
        assert fired == ["later", "at"]
        assert clock.pending == 1
        clock.close()
        assert clock.pending == 0
        clock.call_later(0.0, fired.append, "after close")
        await asyncio.sleep(0.05)
        assert fired == ["later", "at"]
        assert AsyncioClock(seed=1).rng.random() == AsyncioClock(seed=1).rng.random()

    run(scenario())


def test_clean_loopback_delivers_critical_and_telemetry() -> None:
    async def scenario() -> None:
        sa, sb = bound_pair()
        at_b: Delivered = []
        acked: list[int] = []
        a = await open_udp_gateway(sb.getsockname(), sock=sa, on_acked=acked.append)
        b = await open_udp_gateway(sa.getsockname(), sock=sb, on_deliver=recorder(at_b))
        try:
            assert a.local_addr == sa.getsockname()
            seq = a.gateway.submit(b"ARM", Kind.CRITICAL)
            a.gateway.submit(b"pos", Kind.TELEMETRY, key=33)
            await eventually(lambda: acked == [seq] and len(at_b) == 2)
            assert (Kind.CRITICAL, None, b"ARM") in at_b
            assert (Kind.TELEMETRY, 33, b"pos") in at_b
            await eventually(lambda: a.gateway.link_up() and b.gateway.link_up())
        finally:
            await closing(a, b)

    run(scenario())


def test_lossy_proxy_does_not_break_the_guarantees() -> None:
    async def scenario() -> None:
        link = LinkParams(loss=0.3, dup=0.1, corrupt=0.1, delay=0.01, jitter=0.02)
        proxy = await LossyUdpProxy.start(link, seed=7)
        cfg = GatewayConfig(max_rto=0.4)  # more retries per second keeps the test short
        at_b: Delivered = []
        acked: list[int] = []
        gave_up: list[int] = []
        a = await open_udp_gateway(
            proxy.a_addr,
            local_addr=LOCAL,
            config=cfg,
            on_acked=acked.append,
            on_gave_up=lambda seq, payload: gave_up.append(seq),
        )
        b = await open_udp_gateway(
            proxy.b_addr, local_addr=LOCAL, config=cfg, on_deliver=recorder(at_b)
        )
        try:
            sent = [f"cmd{i}".encode() for i in range(20)]
            for payload in sent:
                a.gateway.submit(payload, Kind.CRITICAL, ttl=1.5)
            await eventually(lambda: a.gateway.link_health()["unacked"] == 0)

            got = [p for kind, _, p in at_b if kind is Kind.CRITICAL]
            assert len(got) == len(set(got))  # at most once
            assert set(got) <= set(sent)  # nothing corrupted got through
            assert not set(acked) & set(gave_up)  # acked or given up, never both
            assert sorted(acked + gave_up) == list(range(20))
            assert set(acked) <= {sent.index(p) for p in got}  # an ack means delivered
            assert a.gateway.stats.crit_retx > 0  # the loss really forced retransmits
            corrupted = proxy.a_to_b.stats.corrupted + proxy.b_to_a.stats.corrupted
            bad = a.gateway.stats.bad_packets + b.gateway.stats.bad_packets
            assert corrupted > 0 and bad > 0
        finally:
            await closing(a, b, proxy)

    run(scenario())


def test_dead_peer_is_survived_and_commands_expire() -> None:
    async def scenario() -> None:
        dead = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        dead.bind(LOCAL)
        dead_addr = dead.getsockname()
        dead.close()  # nothing listens here now, so the OS may report errors back
        gave_up: list[bytes] = []
        a = await open_udp_gateway(
            dead_addr,
            local_addr=LOCAL,
            config=GatewayConfig(heartbeat_interval=0.05),
            on_gave_up=lambda seq, payload: gave_up.append(payload),
        )
        try:
            a.gateway.submit(b"GOTO", Kind.CRITICAL, ttl=0.3)
            await eventually(lambda: gave_up == [b"GOTO"])
            sent_before = a.gateway.stats.bytes_tx
            await eventually(lambda: a.gateway.stats.bytes_tx > sent_before)  # still pumping
            assert not a.gateway.link_up()
        finally:
            await a.close()

    run(scenario())


def test_peer_that_starts_late_is_still_heard() -> None:
    # On Windows one "port unreachable" used to leave the socket deaf for good.
    async def scenario() -> None:
        spare = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        spare.bind(LOCAL)
        b_addr = spare.getsockname()
        spare.close()  # B is not up yet, so A's packets bounce
        cfg = GatewayConfig(heartbeat_interval=0.02)
        a = await open_udp_gateway(b_addr, local_addr=LOCAL, config=cfg)
        await asyncio.sleep(0.2)
        late = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        late.bind(b_addr)
        b = await open_udp_gateway(a.local_addr, sock=late, config=cfg)
        try:
            await eventually(lambda: a.gateway.link_up() and b.gateway.link_up(), timeout=2.0)
        finally:
            await closing(a, b)

    run(scenario())


def test_close_stops_the_gateway() -> None:
    async def scenario() -> None:
        sa, sb = bound_pair()
        a = await open_udp_gateway(sb.getsockname(), sock=sa)
        await asyncio.sleep(0.05)
        await a.close()
        await a.close()  # closing twice is harmless
        assert a.clock.pending == 0
        sent = a.gateway.stats.bytes_tx
        a.gateway.submit(b"late", Kind.CRITICAL)
        await asyncio.sleep(0.1)
        assert a.gateway.stats.bytes_tx == sent
        sb.close()

    run(scenario())


def test_packets_from_a_stranger_are_ignored() -> None:
    async def scenario() -> None:
        sa, sb = bound_pair()
        at_a: Delivered = []
        a = await open_udp_gateway(sb.getsockname(), sock=sa, on_deliver=recorder(at_a))
        stranger = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        stranger.bind(LOCAL)
        try:
            forged = P.encode(P.Crit(0, 5000, b"DISARM"))
            stranger.sendto(forged, a.local_addr)
            sb.sendto(P.encode(P.Tele(1, 0, b"real")), a.local_addr)
            await eventually(lambda: len(at_a) > 0)
            await asyncio.sleep(0.05)
            assert at_a == [(Kind.TELEMETRY, 1, b"real")]
        finally:
            stranger.close()
            sb.close()
            await a.close()

    run(scenario())


def test_proxy_one_way_outage_leaves_only_the_ground_side_down() -> None:
    async def scenario() -> None:
        proxy = await LossyUdpProxy.start(
            LinkParams(delay=0.0, outages=[(0.0, 60.0)]),  # air to ground is dead
            LinkParams(delay=0.0),  # ground to air works
        )
        cfg = GatewayConfig(heartbeat_interval=0.05)
        a = await open_udp_gateway(proxy.a_addr, local_addr=LOCAL, config=cfg)
        b = await open_udp_gateway(proxy.b_addr, local_addr=LOCAL, config=cfg)
        try:
            await eventually(lambda: a.gateway.link_up())
            assert not b.gateway.link_up()
            assert proxy.a_to_b.stats.dropped_outage > 0
            assert proxy.a_peer == a.local_addr and proxy.b_peer == b.local_addr
        finally:
            await closing(a, b, proxy)

    run(scenario())

