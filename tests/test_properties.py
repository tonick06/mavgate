"""Property tests: the guarantees hold for any link the simulator can produce.

Each example runs a whole simulated session under randomly chosen impairments.
If one fails, Hypothesis prints the exact parameters, and the seed makes the run
replayable.
"""

from collections import Counter

from hypothesis import given, settings
from hypothesis import strategies as st

from mavgate.gateway import Kind
from mavgate.harness import connect
from mavgate.netsim import LinkParams
from mavgate.sim import Simulation

link_params = st.builds(
    LinkParams,
    loss=st.floats(0.0, 0.6),
    delay=st.floats(0.01, 0.3),
    jitter=st.floats(0.0, 0.5),
    dup=st.floats(0.0, 0.4),
    corrupt=st.floats(0.0, 0.3),
)

commands = st.lists(
    # (submit time, time to live, flow): flows are vehicles sharing the link
    st.tuples(st.floats(0.0, 10.0), st.floats(1.0, 20.0), st.integers(0, 2)),
    min_size=1,
    max_size=25,
)


@settings(max_examples=150, deadline=None)
@given(params=link_params, cmds=commands, seed=st.integers(0, 2**31))
def test_critical_guarantees(
    params: LinkParams, cmds: list[tuple[float, float, int]], seed: int
) -> None:
    sim = Simulation(seed)
    pair = connect(sim, params)
    submitted: dict[int, tuple[float, float]] = {}
    seq_to_index: dict[int, int] = {}

    def submit(i: int, ttl: float, flow: int) -> None:
        seq = pair.a.submit(i.to_bytes(4, "big") + b"cmd", Kind.CRITICAL, None, ttl, flow)
        assert seq is not None
        seq_to_index[seq] = i  # sequence numbers follow submit order, not list order

    for i, (t, ttl, flow) in enumerate(cmds):
        sim.call_at(t, submit, i, ttl, flow)
        submitted[i] = (t, ttl)
    sim.run_until(60)

    delivered = [(d.time, int.from_bytes(d.payload[:4], "big"), d.payload) for d in pair.at_b]

    # 1. Nothing is delivered twice, even with duplicated packets.
    counts = Counter(i for _, i, _ in delivered)
    assert all(c == 1 for c in counts.values())

    # 2. Payloads are never corrupted.
    for _, i, payload in delivered:
        assert payload == i.to_bytes(4, "big") + b"cmd"

    # 3. Nothing is delivered after its time to live, apart from packets already in flight.
    slack = params.delay + params.jitter + 0.05
    for when, i, _ in delivered:
        t, ttl = submitted[i]
        assert when <= t + ttl + slack

    # 4. Every message ends up acknowledged or given up on, never both and never neither.
    acked = set(pair.acked_at_a)
    gave_up = {seq for seq, _ in pair.gave_up_at_a}
    assert not (acked & gave_up)
    assert acked | gave_up == set(seq_to_index)

    # 5. An acknowledged message really was delivered.
    for seq in acked:
        assert seq_to_index[seq] in counts


@settings(max_examples=100, deadline=None)
@given(params=link_params, seed=st.integers(0, 2**31), n=st.integers(5, 150))
def test_telemetry_never_goes_backwards(params: LinkParams, seed: int, n: int) -> None:
    sim = Simulation(seed)
    pair = connect(sim, params)
    key = (1 << 24) | 33
    for i in range(n):
        sim.call_at(i * 0.05, pair.a.submit, i.to_bytes(4, "big"), Kind.TELEMETRY, key)
    sim.run_until(n * 0.05 + 20)
    seen = [int.from_bytes(d.payload, "big") for d in pair.at_b]
    assert seen == sorted(seen)
    assert len(seen) == len(set(seen))
