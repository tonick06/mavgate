"""A deterministic discrete-event simulator with a virtual clock.

Nothing here ever sleeps. Time only moves when `run_until` pops the next
scheduled event, so a 10 minute scenario runs in milliseconds and a failing
seed can be replayed exactly.
"""

from __future__ import annotations

import heapq
import random
from typing import Any, Callable


class Simulation:
    """Virtual clock plus a seeded random generator and an event queue."""

    def __init__(self, seed: int = 0) -> None:
        self._now = 0.0
        self.rng = random.Random(seed)
        self._queue: list[tuple[float, int, Callable[..., None], tuple[Any, ...]]] = []
        self._counter = 0

    @property
    def now(self) -> float:
        return self._now

    def call_at(self, when: float, fn: Callable[..., None], *args: Any) -> None:
        heapq.heappush(self._queue, (when, self._counter, fn, args))
        self._counter += 1

    def call_later(self, delay: float, fn: Callable[..., None], *args: Any) -> None:
        self.call_at(self._now + delay, fn, *args)

    def run_until(self, end: float) -> None:
        """Run every event scheduled up to and including time `end`."""
        while self._queue and self._queue[0][0] <= end:
            when, _, fn, args = heapq.heappop(self._queue)
            self._now = when
            fn(*args)
        self._now = max(self._now, end)
