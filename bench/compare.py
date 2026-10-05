"""Compare the gateway with a naive relay over the same bad link.

Scenario (one direction, air to ground):
  * radio capped at 5,760 B/s (57,600 baud), 20% loss, 80 ms delay, 30 ms jitter
  * a 10 second outage from t=40s to t=50s
  * telemetry: 8 streams at 20 Hz, which is about 9,600 B/s and exceeds the radio
  * critical messages: one every second with an 8 second time to live

Everything runs on the virtual clock, so the numbers are repeatable. They describe
the protocol logic under this model, not a real radio.

Run:  python bench/compare.py
"""

from __future__ import annotations

import statistics
import struct
from dataclasses import dataclass
from typing import Any

from mavgate.gateway import Gateway, Kind
from mavgate.harness import connect
from mavgate.naive import NaiveGateway
from mavgate.netsim import LinkParams
from mavgate.sim import Simulation

DURATION = 120.0
TELE_KEYS = 8
TELE_HZ = 20.0
CRIT_TTL = 8.0
PAD = b"\x00" * 33  # pads the 12 byte header to a 45 byte payload


def percentile(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(q * len(ordered)))
    return ordered[idx]


@dataclass
class Result:
    crit_sent: int
    crit_delivered: int
    crit_p50: float
    crit_p99: float
    tele_delivered: int
    tele_age_p50: float
    tele_age_p95: float
    packets_sent: int


def run(gateway_cls: Any, seed: int) -> Result:
    sim = Simulation(seed)
    params = LinkParams(
        loss=0.2,
        delay=0.08,
        jitter=0.03,
        bandwidth=5760.0,
        max_queue_delay=1.0,
        outages=[(40.0, 50.0)],
    )
    pair = connect(sim, params, gateway_cls=gateway_cls)

    n_crit = 0
    for i in range(int(DURATION)):
        t = float(i)
        payload = struct.pack("!dI", t, i) + PAD[:12]
        sim.call_at(t, pair.a.submit, payload, Kind.CRITICAL, None, CRIT_TTL)
        n_crit += 1

    n_ticks = int(DURATION * TELE_HZ)
    for tick in range(n_ticks):
        t = tick / TELE_HZ
        for k in range(TELE_KEYS):
            payload = struct.pack("!dI", t, tick) + PAD
            sim.call_at(t, pair.a.submit, payload, Kind.TELEMETRY, k)

    sim.run_until(DURATION + 15)

    crit_lat: list[float] = []
    tele_age: list[float] = []
    for d in pair.at_b:
        produced, _ = struct.unpack("!dI", d.payload[:12])
        if d.kind is Kind.CRITICAL:
            crit_lat.append(d.time - produced)
        else:
            tele_age.append(d.time - produced)

    return Result(
        crit_sent=n_crit,
        crit_delivered=len(crit_lat),
        crit_p50=percentile(crit_lat, 0.50),
        crit_p99=percentile(crit_lat, 0.99),
        tele_delivered=len(tele_age),
        tele_age_p50=percentile(tele_age, 0.50),
        tele_age_p95=percentile(tele_age, 0.95),
        packets_sent=pair.a_to_b.stats.sent,
    )


def summarise(label: str, results: list[Result]) -> str:
    def mean(f: Any) -> float:
        return statistics.fmean(f(r) for r in results)

    crit_ok = mean(lambda r: r.crit_delivered / r.crit_sent) * 100
    return (
        f"| {label} | {crit_ok:5.1f}% | {mean(lambda r: r.crit_p50):6.2f} s "
        f"| {mean(lambda r: r.crit_p99):6.2f} s | {mean(lambda r: r.tele_delivered):7.0f} "
        f"| {mean(lambda r: r.tele_age_p50):5.2f} s | {mean(lambda r: r.tele_age_p95):5.2f} s |"
    )


def main() -> None:
    seeds = range(20)
    gw = [run(Gateway, s) for s in seeds]
    nv = [run(NaiveGateway, s) for s in seeds]
    print(f"Mean over {len(list(seeds))} seeds, {DURATION:.0f} s sessions\n")
    print("| Relay | Critical delivered | Critical p50 | Critical p99 | Telemetry delivered | Age p50 | Age p95 |")
    print("|---|---|---|---|---|---|---|")
    print(summarise("Naive relay", nv))
    print(summarise("mavgate gateway", gw))


if __name__ == "__main__":
    main()
