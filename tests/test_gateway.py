from collections import Counter

from mavgate.classify import classify
from mavgate.gateway import GatewayConfig, Kind
from mavgate.harness import connect
from mavgate.netsim import LinkParams
from mavgate.sim import Simulation


def test_critical_message_arrives_once_on_a_clean_link() -> None:
    sim = Simulation(1)
    pair = connect(sim, LinkParams(delay=0.05))
    pair.a.submit(b"ARM", Kind.CRITICAL)
    sim.run_until(5)
    assert [d.payload for d in pair.at_b] == [b"ARM"]
    assert pair.acked_at_a == [0]


def test_critical_messages_survive_heavy_loss() -> None:
    sim = Simulation(2)
    pair = connect(sim, LinkParams(loss=0.3, delay=0.05, jitter=0.05))
    for i in range(50):
        pair.a.submit(f"cmd{i}".encode(), Kind.CRITICAL, ttl=60)
    sim.run_until(60)
    got = sorted(d.payload for d in pair.at_b)
    assert got == sorted(f"cmd{i}".encode() for i in range(50))
    assert pair.a.stats.crit_retx > 0  # the loss really did force retransmits


def test_duplicate_packets_do_not_cause_double_delivery() -> None:
    sim = Simulation(3)
    pair = connect(sim, LinkParams(dup=1.0, delay=0.02))
    for i in range(20):
        pair.a.submit(bytes([i]), Kind.CRITICAL)
    sim.run_until(10)
    assert len(pair.at_b) == 20
    assert pair.b.stats.crit_dup_rx > 0


def test_stale_command_is_not_replayed_after_an_outage() -> None:
    sim = Simulation(4)
    pair = connect(sim, LinkParams(delay=0.05, outages=[(0.0, 20.0)]))
    sim.call_at(1.0, pair.a.submit, b"GOTO-OLD", Kind.CRITICAL, None, 5.0)
    sim.call_at(21.0, pair.a.submit, b"GOTO-NEW", Kind.CRITICAL, None, 5.0)
    sim.run_until(40)
    assert [d.payload for d in pair.at_b] == [b"GOTO-NEW"]
    assert [p for _, p in pair.gave_up_at_a] == [b"GOTO-OLD"]


def test_telemetry_is_latest_wins_when_the_radio_is_busy() -> None:
    sim = Simulation(5)
    cfg = GatewayConfig(rate_bytes_per_s=400, burst_bytes=100)
    pair = connect(sim, LinkParams(delay=0.02), config=cfg)
    key = (1 << 24) | 33
    for i in range(200):  # far more readings than the rate allows
        sim.call_at(i * 0.01, pair.a.submit, i.to_bytes(4, "big"), Kind.TELEMETRY, key)
    sim.run_until(10)
    seen = [int.from_bytes(d.payload, "big") for d in pair.at_b if d.kind is Kind.TELEMETRY]
    assert seen == sorted(seen)
    assert len(seen) < 200
    assert pair.a.stats.tele_superseded > 0
    assert seen[-1] == 199  # the newest reading is what the ground ends up with


def test_critical_traffic_goes_ahead_of_telemetry() -> None:
    sim = Simulation(6)
    cfg = GatewayConfig(rate_bytes_per_s=500, burst_bytes=60)
    pair = connect(sim, LinkParams(delay=0.02), config=cfg)
    for k in range(40):  # a wall of telemetry on many keys
        pair.a.submit(b"t" * 40, Kind.TELEMETRY, key=k)
    pair.a.submit(b"RTL", Kind.CRITICAL)
    sim.run_until(10)
    order = [d.payload for d in pair.at_b]
    assert b"RTL" in order
    assert order.index(b"RTL") <= 2  # at most the packet already in flight beats it


def test_link_health_reports_down_after_silence() -> None:
    sim = Simulation(7)
    pair = connect(sim, LinkParams(delay=0.02, outages=[(5.0, 30.0)]))
    sim.run_until(4)
    assert pair.b.link_up()
    sim.run_until(15)
    assert not pair.b.link_up()


def test_classifier_splits_mavlink_messages() -> None:
    def v2(msgid: int, sysid: int = 1) -> bytes:
        return bytes([0xFD, 0, 0, 0, 0, sysid, 1, msgid & 0xFF, (msgid >> 8) & 0xFF, 0]) + b"\x00" * 4

    assert classify(v2(76))[0] is Kind.CRITICAL  # COMMAND_LONG
    kind, key = classify(v2(33, sysid=2))  # GLOBAL_POSITION_INT from vehicle 2
    assert kind is Kind.TELEMETRY
    assert key == (2 << 24) | 33
    assert classify(b"garbage")[0] is Kind.CRITICAL  # unknown frames take the safe path


def test_send_loop_survives_a_callback_that_raises() -> None:
    sim = Simulation(8)
    pair = connect(sim, LinkParams(delay=0.02, outages=[(0.0, 2.0)]))

    def broken(seq: int, payload: bytes) -> None:
        raise RuntimeError("application bug")

    pair.a.on_gave_up = broken
    pair.a.submit(b"OLD", Kind.CRITICAL, ttl=1.0)
    try:
        sim.run_until(5)
    except RuntimeError:
        pass
    sim.run_until(5)  # resume after the exception surfaced
    pair.a.submit(b"NEW", Kind.CRITICAL)
    sim.run_until(10)
    assert [d.payload for d in pair.at_b] == [b"NEW"]


def test_a_vehicle_with_few_streams_still_gets_a_fair_share() -> None:
    sim = Simulation(9)
    cfg = GatewayConfig(rate_bytes_per_s=2000, burst_bytes=200)
    pair = connect(sim, LinkParams(delay=0.02), config=cfg)
    busy = [(1 << 24) | k for k in range(30)]  # vehicle 1: 30 streams
    quiet = [(2 << 24) | k for k in range(3)]  # vehicle 2: 3 streams
    for tick in range(500):  # both want far more than the link carries
        t = tick * 0.02
        for key in busy:
            sim.call_at(t, pair.a.submit, b"x" * 40, Kind.TELEMETRY, key, None, 1)
        for key in quiet:
            sim.call_at(t, pair.a.submit, b"x" * 40, Kind.TELEMETRY, key, None, 2)
    sim.run_until(10)
    per_vehicle = Counter((d.key or 0) >> 24 for d in pair.at_b)
    share = per_vehicle[2] / (per_vehicle[1] + per_vehicle[2])
    assert 0.4 < share < 0.6  # without fairness it would be 3/33, about 9%


def test_one_vehicles_command_burst_does_not_hold_up_another() -> None:
    sim = Simulation(10)
    cfg = GatewayConfig(rate_bytes_per_s=1000, burst_bytes=100)
    pair = connect(sim, LinkParams(delay=0.02), config=cfg)
    for i in range(100):  # e.g. a mission upload for vehicle 1
        pair.a.submit(b"M" + bytes([i]) + b"." * 30, Kind.CRITICAL, None, 60, 1)
    sim.call_at(0.5, pair.a.submit, b"RTL", Kind.CRITICAL, None, 60, 2)
    sim.run_until(30)
    order = [d.payload for d in pair.at_b]
    assert len(order) == 101
    assert order.index(b"RTL") < 30  # served on its turn, not after all 100


def test_heartbeat_echoes_give_an_rtt_without_any_commands() -> None:
    sim = Simulation(11)
    pair = connect(sim, LinkParams(delay=0.1))
    sim.run_until(5)
    health = pair.a.link_health()
    assert 0.19 < health["rtt"] < 0.3


def _stale_radio_run(adaptive: bool) -> tuple[float, int]:
    """Radio carries 2,500 B/s but the gateway is configured for 5,184 B/s."""
    sim = Simulation(12)
    cfg = GatewayConfig(adaptive_telemetry=adaptive)
    link = LinkParams(delay=0.05, bandwidth=2500, max_queue_delay=1.0)
    pair = connect(sim, link, config=cfg)
    for tick in range(60 * 20):
        t = tick / 20
        for k in range(8):
            sim.call_at(t, pair.a.submit, int(t * 1000).to_bytes(4, "big") + b"." * 41,
                        Kind.TELEMETRY, k)
    for i in range(60):
        sim.call_at(float(i), pair.a.submit, int(i * 1000).to_bytes(4, "big"), Kind.CRITICAL)
    sim.run_until(60)
    ages = sorted(d.time - int.from_bytes(d.payload[:4], "big") / 1000
                  for d in pair.at_b if d.kind is Kind.TELEMETRY and d.time > 20)
    crit = sum(1 for d in pair.at_b if d.kind is Kind.CRITICAL)
    return ages[len(ages) // 2], crit


def test_adaptive_rate_keeps_telemetry_fresh_when_the_radio_is_slower_than_configured() -> None:
    fixed_age, fixed_crit = _stale_radio_run(adaptive=False)
    adaptive_age, adaptive_crit = _stale_radio_run(adaptive=True)
    assert fixed_age > 0.5  # the radio queue sits full
    assert adaptive_age < fixed_age / 3
    assert adaptive_crit >= fixed_crit


def test_adaptive_rate_recovers_to_full_speed_on_a_clean_link() -> None:
    sim = Simulation(13)
    cfg = GatewayConfig(adaptive_telemetry=True)
    pair = connect(sim, LinkParams(delay=0.05), config=cfg)
    sim.run_until(30)
    assert pair.a.link_health()["tele_rate"] == cfg.rate_bytes_per_s
