from mavgate.netsim import Channel, LinkParams
from mavgate.sim import Simulation


def run_channel(seed: int, params: LinkParams, n: int = 2000) -> list[tuple[float, bytes]]:
    sim = Simulation(seed)
    got: list[tuple[float, bytes]] = []
    ch = Channel(sim, params, lambda d: got.append((sim.now, d)))
    for i in range(n):
        sim.call_at(i * 0.01, ch.send, i.to_bytes(4, "big"))
    sim.run_until(n * 0.01 + 10)
    return got


def test_same_seed_gives_identical_runs() -> None:
    p = LinkParams(loss=0.2, jitter=0.1, dup=0.1)
    assert run_channel(5, p) == run_channel(5, p)


def test_different_seeds_differ() -> None:
    p = LinkParams(loss=0.2, jitter=0.1)
    assert run_channel(1, p) != run_channel(2, p)


def test_loss_rate_is_roughly_as_configured() -> None:
    got = run_channel(3, LinkParams(loss=0.3), n=5000)
    assert 0.65 < len(got) / 5000 < 0.75


def test_outage_drops_everything_inside_the_window() -> None:
    got = run_channel(1, LinkParams(delay=0.01, outages=[(5.0, 10.0)]), n=2000)
    assert all(not (5.0 <= t < 10.0) for t, _ in got)
    assert len(got) < 2000


def test_bandwidth_cap_tail_drops_a_flood() -> None:
    sim = Simulation(0)
    got: list[bytes] = []
    ch = Channel(sim, LinkParams(bandwidth=1000, max_queue_delay=0.5), got.append)
    for i in range(100):  # 100 packets of 100 bytes in an instant is 10 s of traffic
        ch.send(bytes(100))
    sim.run_until(30)
    assert 0 < len(got) < 100
    assert ch.stats.dropped_queue > 0
