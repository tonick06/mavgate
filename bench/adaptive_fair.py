"""Adaptive telemetry rate and multi-vehicle fairness (simulation).

Scenario A, a radio slower than configured: the gateway assumes a 57,600 baud
radio (5,184 B/s budget) but the radio carries 2,880 B/s. 5% loss, 80 ms
delay, 8 telemetry streams at 20 Hz, one critical message per second with an
8 second time to live. Adaptive rate off and on.

Scenario B, two vehicles on one radio (5,760 B/s): vehicle 1 sends 24
telemetry streams, vehicle 2 sends 4, all at 20 Hz. At t=30 s vehicle 1
queues 100 critical messages (a mission's worth) and half a second later
vehicle 2 sends one command. "Single queue" submits everything as one flow,
which is how the gateway behaved before flows; "per vehicle" uses the system
id as the flow, as the MAVLink bridge does.

Mean of 20 seeds, 120 s sessions, virtual clock. The numbers describe the
protocol logic under this link model, not a real radio.

Run:  python bench/adaptive_fair.py
"""

from __future__ import annotations

import statistics
import struct
from typing import Callable

from mavgate.gateway import GatewayConfig, Kind
from mavgate.harness import connect
from mavgate.netsim import LinkParams
from mavgate.sim import Simulation

DURATION = 120.0
SEEDS = range(20)
PAD = b"\x00" * 33


def pct(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))]


def stamp(t: float, extra: int = 0) -> bytes:
    return struct.pack("!dI", t, extra)


def slow_radio(adaptive: bool, seed: int) -> dict[str, float]:
    sim = Simulation(seed)
    link = LinkParams(loss=0.05, delay=0.08, bandwidth=2880.0, max_queue_delay=1.0)
    pair = connect(sim, link, config=GatewayConfig(adaptive_telemetry=adaptive))
    for i in range(int(DURATION)):
        sim.call_at(float(i), pair.a.submit, stamp(i) + PAD[:12], Kind.CRITICAL, None, 8.0)
    for tick in range(int(DURATION * 20)):
        t = tick / 20
        for k in range(8):
            sim.call_at(t, pair.a.submit, stamp(t) + PAD, Kind.TELEMETRY, k)
    sim.run_until(DURATION + 10)
    crit: list[float] = []
    tele: list[float] = []
    for d in pair.at_b:
        age = d.time - struct.unpack("!d", d.payload[:8])[0]
        (crit if d.kind is Kind.CRITICAL else tele).append(age)
    return {
        "crit_ok": 100 * len(crit) / int(DURATION),
        "crit_p50": pct(crit, 0.5),
        "crit_p99": pct(crit, 0.99),
        "tele_n": float(len(tele)),
        "tele_p50": pct(tele, 0.5),
        "tele_p95": pct(tele, 0.95),
    }


def two_vehicles(per_vehicle: bool, seed: int) -> dict[str, float]:
    sim = Simulation(seed)
    pair = connect(sim, LinkParams(loss=0.05, delay=0.08, bandwidth=5760.0))

    def flow(vehicle: int) -> int:
        return vehicle if per_vehicle else 0

    for tick in range(int(DURATION * 20)):
        t = tick / 20
        for vehicle, streams in ((1, 24), (2, 4)):
            for k in range(streams):
                key = (vehicle << 24) | k
                sim.call_at(t, pair.a.submit, stamp(t) + PAD, Kind.TELEMETRY, key, None,
                            flow(vehicle))
    for i in range(100):
        sim.call_at(30.0, pair.a.submit, stamp(30.0, i) + b"\x01" + PAD[:30], Kind.CRITICAL,
                    None, 30.0, flow(1))
    sim.call_at(30.5, pair.a.submit, stamp(30.5) + b"\x02", Kind.CRITICAL, None, 30.0, flow(2))
    sim.run_until(DURATION + 10)

    bytes_by: dict[int, int] = {1: 0, 2: 0}
    age_by: dict[int, list[float]] = {1: [], 2: []}
    v2_cmd = float("nan")
    for d in pair.at_b:
        produced = struct.unpack("!d", d.payload[:8])[0]
        if d.kind is Kind.TELEMETRY:
            vehicle = (d.key or 0) >> 24
            bytes_by[vehicle] += len(d.payload)
            age_by[vehicle].append(d.time - produced)
        elif d.payload[12:13] == b"\x02":
            v2_cmd = d.time - produced
    total = bytes_by[1] + bytes_by[2]
    return {
        "v2_share": 100 * bytes_by[2] / total if total else float("nan"),
        "v1_age": pct(age_by[1], 0.5),
        "v2_age": pct(age_by[2], 0.5),
        "v2_cmd": v2_cmd,
    }


def mean(runs: list[dict[str, float]], key: str) -> float:
    return statistics.fmean(r[key] for r in runs)


def table(label: str, runner: Callable[[int], dict[str, float]], cols: list[tuple[str, str]]) -> str:
    runs = [runner(s) for s in SEEDS]
    return f"| {label} | " + " | ".join(fmt.format(mean(runs, key)) for key, fmt in cols) + " |"


def main() -> None:
    print(f"Mean over {len(SEEDS)} seeds, {DURATION:.0f} s sessions (simulation)\n")
    print("A. Radio carries 2,880 B/s, gateway configured for 5,184 B/s\n")
    print("| Telemetry rate | Critical delivered | Critical p50 | Critical p99 "
          "| Telemetry delivered | Age p50 | Age p95 |")
    print("|---|---|---|---|---|---|---|")
    cols_a = [("crit_ok", "{:5.1f}%"), ("crit_p50", "{:5.2f} s"), ("crit_p99", "{:5.2f} s"),
              ("tele_n", "{:6.0f}"), ("tele_p50", "{:5.2f} s"), ("tele_p95", "{:5.2f} s")]
    print(table("Fixed", lambda s: slow_radio(False, s), cols_a))
    print(table("Adaptive", lambda s: slow_radio(True, s), cols_a))
    print("\nB. Two vehicles on one radio: vehicle 1 has 24 streams, vehicle 2 has 4\n")
    print("| Scheduling | Vehicle 2 share of telemetry | Vehicle 1 age p50 "
          "| Vehicle 2 age p50 | Vehicle 2 command latency |")
    print("|---|---|---|---|---|")
    cols_b = [("v2_share", "{:5.1f}%"), ("v1_age", "{:5.2f} s"), ("v2_age", "{:5.2f} s"),
              ("v2_cmd", "{:5.2f} s")]
    print(table("Single queue", lambda s: two_vehicles(False, s), cols_b))
    print(table("Per vehicle", lambda s: two_vehicles(True, s), cols_b))


if __name__ == "__main__":
    main()
