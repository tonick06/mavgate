"""Mission upload benchmark: 100 waypoints over a 30% loss link (simulation).

A ground station uploads a 100 item mission to a vehicle, with and without
the gateway, while the vehicle streams telemetry over the same radio.

The two ends speak a simplified MAVLink mission protocol, timed after
ArduPilot and common ground stations:

  * ground: MISSION_COUNT, then one MISSION_ITEM_INT per MISSION_REQUEST_INT,
    done on MISSION_ACK. If it hears nothing for 1.5 s it resends whatever it
    sent last, and gives up after 5 resends in a row.
  * vehicle: requests item 0, then each next item. It re-requests after 1 s of
    silence and abandons the upload after 8 s without progress. It answers a
    repeated last item with MISSION_ACK again, so a lost ACK is recoverable.

Frames carry real MAVLink 2 headers and payload sizes, so `classify` sorts
them exactly as the bridge would.

Radio: 5,760 B/s, 30% loss each way, 80 ms delay, 30 ms jitter.
Telemetry: 8 streams at 20 Hz from the vehicle (about 9,600 B/s, more than
the radio carries), or none.

Everything runs on the virtual clock. The numbers describe this protocol model
under this link model, not a particular autopilot or radio.

Run:  python bench/mission.py
"""

from __future__ import annotations

import statistics
import struct
from dataclasses import dataclass
from typing import Any, Callable

from mavgate.classify import classify
from mavgate.gateway import Gateway, Kind
from mavgate.harness import connect
from mavgate.naive import NaiveGateway
from mavgate.netsim import LinkParams
from mavgate.sim import Simulation

ITEMS = 100
CAP = 300.0  # seconds before a run counts as failed
SEEDS = range(20)
CRIT_TTL = 5.0

MISSION_COUNT, MISSION_REQUEST_INT, MISSION_ITEM_INT, MISSION_ACK = 44, 51, 73, 47
GCS_SYSID, VEHICLE_SYSID = 255, 1
PAYLOAD_LEN = {MISSION_COUNT: 5, MISSION_REQUEST_INT: 5, MISSION_ITEM_INT: 38, MISSION_ACK: 4}


def frame(sysid: int, msgid: int, value: int) -> bytes:
    """A MAVLink 2 frame with `value` (a count or an item number) in the payload."""
    payload = struct.pack("<H", value).ljust(PAYLOAD_LEN.get(msgid, 28), b"\x00")
    head = bytes([0xFD, len(payload), 0, 0, 0, sysid, 1]) + msgid.to_bytes(3, "little")
    return head + payload + b"\x00\x00"


def unpack(data: bytes) -> tuple[int, int]:
    return int.from_bytes(data[7:10], "little"), struct.unpack("<H", data[10:12])[0]


Send = Callable[[bytes], None]


class GroundStation:
    def __init__(self, sim: Simulation, send: Send) -> None:
        self.sim, self.send = sim, send
        self.last: bytes = b""
        self.retries = 0
        self.done_at: float | None = None
        self.failed = False
        self._timer_gen = 0

    def start(self) -> None:
        self._out(frame(GCS_SYSID, MISSION_COUNT, ITEMS))

    def _out(self, data: bytes) -> None:
        self.last = data
        self.send(data)
        self._arm()

    def _arm(self) -> None:
        self._timer_gen += 1
        self.sim.call_later(1.5, self._timeout, self._timer_gen)

    def _timeout(self, gen: int) -> None:
        if gen != self._timer_gen or self.done_at is not None or self.failed:
            return
        self.retries += 1
        if self.retries > 5:
            self.failed = True
            return
        self._out(self.last)

    def receive(self, data: bytes) -> None:
        if self.done_at is not None or self.failed:
            return
        msgid, value = unpack(data)
        if msgid == MISSION_REQUEST_INT:
            self.retries = 0
            self._out(frame(GCS_SYSID, MISSION_ITEM_INT, value))
        elif msgid == MISSION_ACK:
            self.done_at = self.sim.now


class Vehicle:
    def __init__(self, sim: Simulation, send: Send) -> None:
        self.sim, self.send = sim, send
        self.expected: int | None = None  # next item wanted; None when not receiving
        self.count = 0
        self.complete = False
        self.last_progress = 0.0
        self._timer_gen = 0

    def _request(self) -> None:
        assert self.expected is not None
        self.send(frame(VEHICLE_SYSID, MISSION_REQUEST_INT, self.expected))
        self._timer_gen += 1
        self.sim.call_later(1.0, self._timeout, self._timer_gen)

    def _timeout(self, gen: int) -> None:
        if gen != self._timer_gen or self.expected is None:
            return
        if self.sim.now - self.last_progress > 8.0:
            self.expected = None  # abandon; the ground station must start again
            return
        self._request()

    def receive(self, data: bytes) -> None:
        msgid, value = unpack(data)
        if msgid == MISSION_COUNT:
            self.count, self.expected, self.complete = value, 0, False
            self.last_progress = self.sim.now
            self._request()
        elif msgid == MISSION_ITEM_INT:
            if self.expected is None:
                if self.complete and value == self.count - 1:
                    self.send(frame(VEHICLE_SYSID, MISSION_ACK, 0))  # our ACK was lost
                return
            if value != self.expected:
                return
            self.expected += 1
            self.last_progress = self.sim.now
            if self.expected == self.count:
                self.expected, self.complete = None, True
                self._timer_gen += 1
                self.send(frame(VEHICLE_SYSID, MISSION_ACK, 0))
            else:
                self._request()


@dataclass
class Result:
    ok: bool
    seconds: float


def run(gateway_cls: Any, seed: int, telemetry: bool) -> Result:
    sim = Simulation(seed)
    link = LinkParams(loss=0.3, delay=0.08, jitter=0.03, bandwidth=5760.0, max_queue_delay=1.0)
    pair = connect(sim, link, gateway_cls=gateway_cls)
    air, ground = pair.a, pair.b

    def submitter(gw: Any) -> Send:
        def send(data: bytes) -> None:
            kind, key = classify(data)
            ttl = CRIT_TTL if kind is Kind.CRITICAL else None
            gw.submit(data, kind, key, ttl)

        return send

    gcs = GroundStation(sim, submitter(ground))
    vehicle = Vehicle(sim, submitter(air))
    ground.on_deliver = lambda kind, key, payload: gcs.receive(payload)
    air.on_deliver = lambda kind, key, payload: vehicle.receive(payload)

    if telemetry:
        for tick in range(int(CAP * 20)):
            for msgid in (24, 30, 33, 1, 74, 62, 65, 36):  # GPS, ATTITUDE, POSITION, ...
                sim.call_at(tick / 20, submitter(air), frame(VEHICLE_SYSID, msgid, tick & 0xFFFF))

    start = 2.0  # let the link settle and the gateways measure an RTT
    sim.call_at(start, gcs.start)
    sim.run_until(start + CAP)
    if gcs.done_at is None:
        return Result(False, CAP)
    return Result(True, gcs.done_at - start)


def summarise(label: str, results: list[Result]) -> str:
    ok = [r.seconds for r in results if r.ok]
    rate = 100 * len(ok) / len(results)
    if not ok:
        return f"| {label} | {rate:3.0f}% | - | - | - |"
    ok.sort()
    p90 = ok[min(len(ok) - 1, int(0.9 * len(ok)))]
    return (f"| {label} | {rate:3.0f}% | {statistics.fmean(ok):6.1f} s "
            f"| {statistics.median(ok):6.1f} s | {p90:6.1f} s |")


def main() -> None:
    print(f"{ITEMS} item mission upload, 30% loss each way, {len(SEEDS)} seeds, "
          f"{CAP:.0f} s cap (simulation)\n")
    print("| Relay | Completed | Mean time | Median | p90 |")
    print("|---|---|---|---|---|")
    for telemetry in (False, True):
        tag = "with telemetry" if telemetry else "no telemetry"
        for label, cls in (("Naive relay", NaiveGateway), ("mavgate gateway", Gateway)):
            results = [run(cls, s, telemetry) for s in SEEDS]
            print(summarise(f"{label}, {tag}", results))


if __name__ == "__main__":
    main()
