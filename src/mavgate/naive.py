"""The baseline: forward every message once, as soon as it arrives.

This is roughly what a plain serial or UDP relay does. No acknowledgements, no
retries, no priorities, no rate limit. It exists so benchmarks have something
honest to compare against.
"""

from __future__ import annotations

from typing import Any, Callable

from . import protocol as P
from .gateway import Clock, DeliverFn, Kind


class NaiveGateway:
    def __init__(
        self,
        clock: Clock,
        send_raw: Callable[[bytes], None],
        config: Any = None,
        on_deliver: DeliverFn | None = None,
        **_: Any,
    ) -> None:
        self.clock = clock
        self.send_raw = send_raw
        self.on_deliver = on_deliver or (lambda kind, key, payload: None)
        self._seq = 0

    def submit(
        self,
        payload: bytes,
        kind: Kind,
        key: int | None = None,
        ttl: float | None = None,
    ) -> int | None:
        seq = self._seq
        self._seq += 1
        # Everything goes out as a bare data packet; the kind is only carried in the key.
        marker = 0xFFFFFFFF if kind is Kind.CRITICAL else (key or 0)
        self.send_raw(P.encode(P.Tele(marker, seq, payload)))
        return seq if kind is Kind.CRITICAL else None

    def receive(self, data: bytes) -> None:
        try:
            pkt = P.decode(data)
        except P.ProtocolError:
            return
        if isinstance(pkt, P.Tele):
            kind = Kind.CRITICAL if pkt.key == 0xFFFFFFFF else Kind.TELEMETRY
            self.on_deliver(kind, None if kind is Kind.CRITICAL else pkt.key, pkt.payload)
