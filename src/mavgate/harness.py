"""Helpers that wire two gateways together through the simulator."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .gateway import Gateway, GatewayConfig, Kind
from .netsim import Channel, LinkParams
from .sim import Simulation


@dataclass
class Delivery:
    time: float
    kind: Kind
    key: int | None
    payload: bytes


@dataclass
class Pair:
    sim: Simulation
    a: Any  # the "air" gateway
    b: Any  # the "ground" gateway
    a_to_b: Channel
    b_to_a: Channel
    at_a: list[Delivery] = field(default_factory=list)  # what the air side received
    at_b: list[Delivery] = field(default_factory=list)  # what the ground side received
    acked_at_a: list[int] = field(default_factory=list)
    gave_up_at_a: list[tuple[int, bytes]] = field(default_factory=list)


def connect(
    sim: Simulation,
    a_to_b: LinkParams,
    b_to_a: LinkParams | None = None,
    config: GatewayConfig | None = None,
    gateway_cls: Any = Gateway,
) -> Pair:
    """Build two gateways joined by two simulated channels."""
    pair = Pair(sim=sim, a=None, b=None, a_to_b=None, b_to_a=None)  # type: ignore[arg-type]

    def record(store: list[Delivery]) -> Any:
        def on_deliver(kind: Kind, key: int | None, payload: bytes) -> None:
            store.append(Delivery(sim.now, kind, key, payload))

        return on_deliver

    extra_a: dict[str, Any] = {}
    if gateway_cls is Gateway:
        extra_a = {
            "on_acked": pair.acked_at_a.append,
            "on_gave_up": lambda seq, payload: pair.gave_up_at_a.append((seq, payload)),
        }

    pair.a = gateway_cls(
        sim, lambda d: pair.a_to_b.send(d), config, on_deliver=record(pair.at_a), **extra_a
    )
    pair.b = gateway_cls(sim, lambda d: pair.b_to_a.send(d), config, on_deliver=record(pair.at_b))
    pair.a_to_b = Channel(sim, a_to_b, lambda d: pair.b.receive(d))
    pair.b_to_a = Channel(sim, b_to_a or a_to_b, lambda d: pair.a.receive(d))
    return pair
