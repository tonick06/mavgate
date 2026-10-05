"""The gateway: one end of a resilient link.

Two message classes share one rate-limited radio link:

* CRITICAL (commands, mission items, parameter changes): acknowledged,
  retransmitted with backoff, delivered at most once, and abandoned once the
  time-to-live runs out. A stale command is never replayed after an outage.
* TELEMETRY (position, attitude, status): fire and forget, and "latest value
  wins" per key. A newer reading replaces an unsent older one, and a reading
  older than one already delivered is discarded.

Critical traffic has strict priority. A token bucket keeps the total below the
radio's capacity so the radio's own queue never builds up.

Several vehicles (flows) can share one link. Within each class the flows take
turns: critical messages round robin, telemetry by deficit round robin on
bytes, so a vehicle with many streams cannot crowd out one with few.

With `adaptive_telemetry`, the telemetry rate also follows the link. Heartbeats
are echoed, which gives an RTT sample every second. When the RTT climbs above
its recent minimum by more than `queue_delay_target`, something downstream is
queueing (usually a radio slower than `rate_bytes_per_s`), so the telemetry
rate is cut. It creeps back up while the queue stays short.

The gateway never reads the clock or the network directly. It takes a `Clock`
and a `send_raw` callable, so the same code runs under the deterministic
simulator and on real sockets (`net_asyncio`).
"""

from __future__ import annotations

import enum
from collections import OrderedDict, deque
from dataclasses import dataclass
from typing import Any, Callable, Protocol

from . import protocol as P


class Clock(Protocol):
    @property
    def now(self) -> float: ...

    def call_later(self, delay: float, fn: Callable[..., None], *args: Any) -> None: ...


class Kind(enum.Enum):
    CRITICAL = "critical"
    TELEMETRY = "telemetry"


@dataclass
class GatewayConfig:
    rate_bytes_per_s: float = 5184.0  # 90% of a 57,600 baud serial radio (5,760 B/s)
    burst_bytes: float = 600.0
    tick: float = 0.02  # how often the send loop runs
    initial_rto: float = 0.5  # first retransmit timeout, before any RTT sample
    min_rto: float = 0.15
    max_rto: float = 3.0
    heartbeat_interval: float = 1.0
    link_timeout: float = 3.5  # link counts as down after this long in silence
    default_ttl: float = 15.0  # seconds a critical message may stay undelivered
    fair_quantum: int = 300  # bytes of telemetry a flow may send per turn
    adaptive_telemetry: bool = False
    queue_delay_target: float = 0.25  # RTT above the minimum that counts as queueing
    min_telemetry_share: float = 0.1  # adaptive rate never drops below this share
    rtt_window: float = 30.0  # seconds of RTT samples the minimum is taken over


@dataclass
class Stats:
    crit_submitted: int = 0
    crit_acked: int = 0
    crit_gave_up: int = 0
    crit_tx: int = 0
    crit_retx: int = 0
    crit_delivered: int = 0
    crit_dup_rx: int = 0
    tele_submitted: int = 0
    tele_superseded: int = 0
    tele_tx: int = 0
    tele_delivered: int = 0
    tele_stale_rx: int = 0
    bad_packets: int = 0
    bytes_tx: int = 0
    tele_rate_cuts: int = 0


@dataclass
class _Pending:
    seq: int
    payload: bytes
    submitted_at: float
    expires_at: float
    flow: int = 0
    attempts: int = 0
    next_send: float = 0.0
    last_sent: float = 0.0


DeliverFn = Callable[[Kind, "int | None", bytes], None]


class Gateway:
    def __init__(
        self,
        clock: Clock,
        send_raw: Callable[[bytes], None],
        config: GatewayConfig | None = None,
        on_deliver: DeliverFn | None = None,
        on_acked: Callable[[int], None] | None = None,
        on_gave_up: Callable[[int, bytes], None] | None = None,
    ) -> None:
        self.clock = clock
        self.send_raw = send_raw
        self.cfg = config or GatewayConfig()
        self.on_deliver = on_deliver or (lambda kind, key, payload: None)
        self.on_acked = on_acked or (lambda seq: None)
        self.on_gave_up = on_gave_up or (lambda seq, payload: None)
        self.stats = Stats()

        self._crit: OrderedDict[int, _Pending] = OrderedDict()
        self._crit_seq = 0
        self._crit_last_flow: int | None = None
        # telemetry waiting to go, per flow, then per key: (seq, payload)
        self._tele: dict[int, OrderedDict[int, tuple[int, bytes]]] = {}
        self._tele_turns: deque[int] = deque()  # flows with telemetry waiting, in turn order
        self._deficit: dict[int, int] = {}
        self._tele_tx_seq: dict[int, int] = {}

        self._seen_crit: set[int] = set()
        self._max_seen_crit = -1
        self._tele_rx_seq: dict[int, int] = {}

        self._tokens = self.cfg.burst_bytes
        self._tele_tokens = self.cfg.burst_bytes
        self._tele_rate = self.cfg.rate_bytes_per_s
        self._last_refill = clock.now
        self._srtt: float | None = None
        self._rttvar = 0.0
        self._last_heard: float | None = None
        self._hb_counter = 0
        self._next_hb = clock.now
        self._hb_sent: OrderedDict[int, float] = OrderedDict()
        self._path_rtt: float | None = None  # smoothed heartbeat RTT
        self._rtt_samples: deque[tuple[float, float]] = deque()  # (time, rtt) for the minimum
        self._last_cut = -1e9

        clock.call_later(self.cfg.tick, self._pump)

    # ---- public API -------------------------------------------------------

    def submit(
        self,
        payload: bytes,
        kind: Kind,
        key: int | None = None,
        ttl: float | None = None,
        flow: int = 0,
    ) -> int | None:
        """Queue a message. Returns the sequence number for CRITICAL messages.

        `flow` names who the message belongs to (for MAVLink, the vehicle's
        system id). Flows share the link fairly; it is never sent on the wire.
        """
        if len(payload) > P.MAX_PAYLOAD:
            raise ValueError("payload too large")
        now = self.clock.now
        if kind is Kind.CRITICAL:
            seq = self._crit_seq
            self._crit_seq += 1
            lifetime = self.cfg.default_ttl if ttl is None else ttl
            self._crit[seq] = _Pending(seq, payload, now, now + lifetime, flow, next_send=now)
            self.stats.crit_submitted += 1
            return seq
        if key is None:
            raise ValueError("telemetry needs a key")
        seq = self._tele_tx_seq.get(key, 0)
        self._tele_tx_seq[key] = seq + 1
        queue = self._tele.get(flow)
        if queue is None:
            queue = self._tele[flow] = OrderedDict()
            self._tele_turns.append(flow)
        if key in queue:
            self.stats.tele_superseded += 1
        queue[key] = (seq, payload)  # an existing key keeps its place in line
        self.stats.tele_submitted += 1
        return None

    def receive(self, data: bytes) -> None:
        """Feed raw bytes from the radio into the gateway."""
        try:
            pkt = P.decode(data)
        except P.ProtocolError:
            self.stats.bad_packets += 1
            return
        now = self.clock.now
        self._last_heard = now

        if isinstance(pkt, P.Crit):
            self._on_crit(pkt)
        elif isinstance(pkt, P.Tele):
            self._on_tele(pkt)
        elif isinstance(pkt, P.Ack):
            self._on_ack(pkt, now)
        elif isinstance(pkt, P.Heartbeat):
            self._tx(P.encode(P.HeartbeatEcho(pkt.counter)))
        else:
            self._on_echo(pkt, now)

    def link_up(self) -> bool:
        return self._last_heard is not None and (
            self.clock.now - self._last_heard < self.cfg.link_timeout
        )

    def link_health(self) -> dict[str, float | bool]:
        s = self.stats
        loss_estimate = 1.0 - (s.crit_acked / s.crit_tx) if s.crit_tx else 0.0
        return {
            "up": self.link_up(),
            "rto": self._rto(),
            "rtt": self._path_rtt if self._path_rtt is not None else float("nan"),
            "retransmit_ratio": (s.crit_retx / s.crit_tx) if s.crit_tx else 0.0,
            "unacked": float(len(self._crit)),
            "crit_loss_estimate": max(0.0, loss_estimate),
            "tele_rate": self._tele_rate,
        }

    # ---- receiving --------------------------------------------------------

    def _on_crit(self, pkt: P.Crit) -> None:
        if pkt.ttl_ms <= 0:
            return  # expired in flight: neither delivered nor acknowledged
        self._tx(P.encode(P.Ack(pkt.seq)))  # always ack, so the sender stops resending
        if pkt.seq in self._seen_crit:
            self.stats.crit_dup_rx += 1
            return
        self._seen_crit.add(pkt.seq)
        self._max_seen_crit = max(self._max_seen_crit, pkt.seq)
        if len(self._seen_crit) > 8192:
            floor = self._max_seen_crit - 4096
            self._seen_crit = {s for s in self._seen_crit if s >= floor}
        self.stats.crit_delivered += 1
        self.on_deliver(Kind.CRITICAL, None, pkt.payload)

    def _on_tele(self, pkt: P.Tele) -> None:
        if pkt.seq <= self._tele_rx_seq.get(pkt.key, -1):
            self.stats.tele_stale_rx += 1
            return
        self._tele_rx_seq[pkt.key] = pkt.seq
        self.stats.tele_delivered += 1
        self.on_deliver(Kind.TELEMETRY, pkt.key, pkt.payload)

    def _on_ack(self, pkt: P.Ack, now: float) -> None:
        entry = self._crit.pop(pkt.seq, None)
        if entry is None:
            return
        self.stats.crit_acked += 1
        if entry.attempts == 1:  # only unambiguous samples feed the RTT estimate
            self._update_rtt(now - entry.last_sent)
        self.on_acked(pkt.seq)

    def _on_echo(self, pkt: P.HeartbeatEcho, now: float) -> None:
        sent = self._hb_sent.pop(pkt.counter, None)
        if sent is None:
            return  # duplicate, or too old to remember
        sample = now - sent
        self._path_rtt = sample if self._path_rtt is None else 0.875 * self._path_rtt + 0.125 * sample
        samples = self._rtt_samples
        samples.append((now, sample))
        while samples and samples[0][0] < now - self.cfg.rtt_window:
            samples.popleft()
        if self.cfg.adaptive_telemetry:
            self._adapt(now, sample, min(rtt for _, rtt in samples))

    def _adapt(self, now: float, sample: float, base: float) -> None:
        nominal = self.cfg.rate_bytes_per_s
        if sample - base > self.cfg.queue_delay_target:
            if now - self._last_cut > sample:  # at most one cut per round trip
                floor = nominal * self.cfg.min_telemetry_share
                self._tele_rate = max(floor, self._tele_rate * 0.7)
                self._last_cut = now
                self.stats.tele_rate_cuts += 1
        else:
            self._tele_rate = min(nominal, self._tele_rate + nominal * 0.05)

    # ---- sending ----------------------------------------------------------

    def _rto(self) -> float:
        if self._srtt is None:
            return self.cfg.initial_rto
        rto = self._srtt + max(4 * self._rttvar, 0.05)
        return min(self.cfg.max_rto, max(self.cfg.min_rto, rto))

    def _update_rtt(self, sample: float) -> None:
        if self._srtt is None:
            self._srtt = sample
            self._rttvar = sample / 2
        else:
            self._rttvar = 0.75 * self._rttvar + 0.25 * abs(self._srtt - sample)
            self._srtt = 0.875 * self._srtt + 0.125 * sample

    def _tx(self, data: bytes) -> None:
        self._tokens -= len(data)
        self.stats.bytes_tx += len(data)
        self.send_raw(data)

    def _pump(self) -> None:
        # Reschedule even if a callback or send_raw raises, or the link goes dead for good.
        try:
            now = self.clock.now
            elapsed = now - self._last_refill
            self._last_refill = now
            burst = self.cfg.burst_bytes
            self._tokens = min(burst, self._tokens + self.cfg.rate_bytes_per_s * elapsed)
            self._tele_tokens = min(burst, self._tele_tokens + self._tele_rate * elapsed)

            self._give_up_expired(now)
            self._send_heartbeat(now)
            blocked = self._send_critical(now)
            if not blocked:
                self._send_telemetry()
        finally:
            self.clock.call_later(self.cfg.tick, self._pump)

    def _give_up_expired(self, now: float) -> None:
        for seq, entry in list(self._crit.items()):
            if now >= entry.expires_at:
                del self._crit[seq]
                self.stats.crit_gave_up += 1
                self.on_gave_up(seq, entry.payload)

    def _send_heartbeat(self, now: float) -> None:
        if now >= self._next_hb:
            self._next_hb = now + self.cfg.heartbeat_interval
            self._hb_counter += 1
            self._hb_sent[self._hb_counter] = now
            while len(self._hb_sent) > 16:
                self._hb_sent.popitem(last=False)
            self._tx(P.encode(P.Heartbeat(self._hb_counter)))

    def _due_in_turn(self, now: float) -> list[_Pending]:
        """Due critical messages, oldest first within a flow, flows taking turns."""
        by_flow: dict[int, deque[_Pending]] = {}
        for entry in self._crit.values():
            if entry.next_send <= now:
                by_flow.setdefault(entry.flow, deque()).append(entry)
        if len(by_flow) <= 1:
            return [e for q in by_flow.values() for e in q]
        flows = sorted(by_flow)
        if self._crit_last_flow is not None:  # start after whoever went last
            later = [f for f in flows if f > self._crit_last_flow]
            flows = later + [f for f in flows if f <= self._crit_last_flow]
        order: list[_Pending] = []
        while flows:
            for f in list(flows):
                order.append(by_flow[f].popleft())
                if not by_flow[f]:
                    flows.remove(f)
        return order

    def _send_critical(self, now: float) -> bool:
        """Send due critical messages. Returns True if starved of tokens."""
        for entry in self._due_in_turn(now):
            ttl_ms = int((entry.expires_at - now) * 1000)
            if ttl_ms <= 0:
                continue
            pkt = P.encode(P.Crit(entry.seq, ttl_ms, entry.payload))
            if self._tokens < len(pkt):
                return True
            self._tx(pkt)
            self._crit_last_flow = entry.flow
            entry.attempts += 1
            entry.last_sent = now
            backoff = min(self.cfg.max_rto, self._rto() * 2 ** (entry.attempts - 1))
            entry.next_send = now + backoff
            self.stats.crit_tx += 1
            if entry.attempts > 1:
                self.stats.crit_retx += 1
        return False

    def _send_telemetry(self) -> None:
        """Deficit round robin over flows; each flow sends its keys in queue order."""
        turns = self._tele_turns
        while turns:
            flow = turns[0]
            queue = self._tele[flow]
            granted = False
            while queue:
                key, (seq, payload) = next(iter(queue.items()))
                pkt = P.encode(P.Tele(key, seq, payload))
                size = len(pkt)
                deficit = self._deficit.get(flow, 0)
                if deficit < size:
                    if granted and len(turns) > 1:
                        break  # one quantum per turn while others are waiting
                    deficit += self.cfg.fair_quantum
                    self._deficit[flow] = deficit
                    granted = True
                    if deficit < size:
                        break  # quantum smaller than this packet: it builds up over turns
                if self._tokens < size or self._tele_tokens < size:
                    turns.rotate(-1)  # out of budget: this flow's turn ends here
                    return
                del queue[key]
                self._deficit[flow] = deficit - size
                self._tele_tokens -= size
                self._tx(pkt)
                self.stats.tele_tx += 1
            if queue:
                turns.rotate(-1)
            else:
                turns.popleft()
                del self._tele[flow]
                self._deficit.pop(flow, None)
