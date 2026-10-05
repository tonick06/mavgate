"""A one-direction lossy radio link.

It normally runs on the virtual-clock simulator, but it only needs a
`Scheduler`, so `udp_proxy` reuses it to impair real UDP traffic the same way.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol


class Scheduler(Protocol):
    """What a channel needs: a clock, a seeded generator and a timer."""

    @property
    def now(self) -> float: ...

    @property
    def rng(self) -> random.Random: ...

    def call_at(self, when: float, fn: Callable[..., None], *args: Any) -> None: ...


@dataclass
class LinkParams:
    """Behaviour of one direction of the link. All times are in seconds."""

    loss: float = 0.0  # probability a packet is dropped at send time
    delay: float = 0.05  # fixed one-way latency
    jitter: float = 0.0  # extra random latency in [0, jitter]; causes reordering
    dup: float = 0.0  # probability a packet is delivered twice
    corrupt: float = 0.0  # probability one byte of a packet is flipped
    bandwidth: float | None = None  # bytes per second, None means unlimited
    max_queue_delay: float = 1.0  # tail-drop once the radio queue exceeds this
    outages: list[tuple[float, float]] = field(default_factory=list)  # (start, end)


@dataclass
class ChannelStats:
    sent: int = 0
    delivered: int = 0
    dropped_loss: int = 0
    dropped_outage: int = 0
    dropped_queue: int = 0
    duplicated: int = 0
    corrupted: int = 0


class Channel:
    """Carries bytes one way, applying the impairments in `LinkParams`."""

    def __init__(
        self,
        sim: Scheduler,
        params: LinkParams,
        deliver: Callable[[bytes], None],
    ) -> None:
        self.sim = sim
        self.params = params
        self._deliver = deliver
        self._busy_until = 0.0
        self.stats = ChannelStats()

    def _in_outage(self, t: float) -> bool:
        return any(start <= t < end for start, end in self.params.outages)

    def send(self, data: bytes) -> None:
        p = self.params
        now = self.sim.now
        self.stats.sent += 1

        if self._in_outage(now):
            self.stats.dropped_outage += 1
            return
        if self.sim.rng.random() < p.loss:
            self.stats.dropped_loss += 1
            return

        leave = now
        if p.bandwidth is not None:
            start = max(now, self._busy_until)
            if start - now > p.max_queue_delay:
                self.stats.dropped_queue += 1
                return
            leave = start + len(data) / p.bandwidth
            self._busy_until = leave

        copies = 1
        if self.sim.rng.random() < p.dup:
            copies = 2
            self.stats.duplicated += 1

        for _ in range(copies):
            payload = data
            if self.sim.rng.random() < p.corrupt and payload:
                idx = self.sim.rng.randrange(len(payload))
                flipped = payload[idx] ^ (1 << self.sim.rng.randrange(8))
                payload = payload[:idx] + bytes([flipped]) + payload[idx + 1 :]
                self.stats.corrupted += 1
            arrive = leave + p.delay + self.sim.rng.uniform(0.0, p.jitter)
            self.sim.call_at(arrive, self._arrive, payload)

    def _arrive(self, data: bytes) -> None:
        if self._in_outage(self.sim.now):
            self.stats.dropped_outage += 1
            return
        self.stats.delivered += 1
        self._deliver(data)
