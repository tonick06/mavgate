# mavgate

A link-resilience gateway for MAVLink traffic over lossy, low-bandwidth radio links.

Drones talk to their ground station over radios that drop packets, run at low data rates and sometimes go silent. A plain relay treats every message the same, so a flood of position updates can push a critical command off the link, and a command queued during an outage can fire minutes late. `mavgate` sits on both ends of the radio and treats the two kinds of traffic differently.

## How it works

| Class | Examples | Behaviour |
|---|---|---|
| Critical | commands, mission items, parameter changes | acknowledged, retransmitted with backoff, delivered at most once, abandoned when the time to live runs out |
| Telemetry | position, attitude, status | no retries, newest reading per stream replaces an unsent older one, readings older than one already delivered are discarded |

Other design points:

- **Strict priority.** Critical traffic goes first. A token bucket holds the total below the radio's capacity, so the radio's own queue stays short and telemetry stays fresh.
- **Expiry instead of replay.** A command that could not be delivered within its time to live is dropped and reported, never sent late. A "fly to this point" command that was queued during a 20 second outage is the wrong thing to run when the link comes back.
- **No clock sync.** The time to live travels as time remaining, so the two ends do not need synchronised clocks.
- **Integrity.** Every packet carries a CRC32. Corrupted packets are dropped, never delivered.
- **Link health.** Heartbeats are echoed, so each end has a round-trip time even with no commands in flight. With acknowledgement statistics this gives an up/down state, RTT, retransmit ratio and loss estimate.
- **Adaptive telemetry rate (optional).** With `adaptive_telemetry=True`, a rise in RTT above its recent minimum means something downstream is queueing, usually a radio slower than the configured budget. The telemetry rate is then cut by 30% and creeps back up while the queue stays short. Critical traffic is never throttled.
- **Several vehicles on one link.** `submit(..., flow=...)` names the vehicle. Critical messages take turns between vehicles, and telemetry is shared by deficit round robin on bytes. A vehicle with 24 streams no longer gets six times the bandwidth of one with 4.
- **Core is pure Python and takes a clock.** The gateway never sleeps or touches the network directly, so the same code runs under the simulator and on real sockets.

## Quick start

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest                      # unit and property tests
mypy                        # strict type checking
python bench/compare.py     # gateway vs naive relay
```

## Running with an autopilot

`mavlink_bridge.py` puts a gateway between a MAVLink program and the link. Run one at each end:

```bash
# vehicle side: the autopilot sends MAVLink to 127.0.0.1:14560
python -m mavgate.mavlink_bridge --mavlink-listen 127.0.0.1:14560 --link-local 0.0.0.0:14600 --link-remote GROUND_IP:14600
# ground side: the ground station listens on 14550 as usual
python -m mavgate.mavlink_bridge --mavlink-listen 127.0.0.1:14580 --mavlink-peer 127.0.0.1:14550 --link-local 0.0.0.0:14600 --link-remote VEHICLE_IP:14600
```

Each datagram is cut into MAVLink frames (ArduPilot packs several into one). Each frame is classified by message id, and the sending vehicle's system id becomes its flow. Add `--adaptive` for the adaptive telemetry rate. For ArduPilot SITL, start `arducopter` with `--serial0 udpclient:127.0.0.1:14560`.

## Running over UDP

`net_asyncio.py` runs a gateway on a real UDP socket. The core is unchanged; the adapter supplies an event-loop clock and a socket.

```python
import asyncio
from mavgate.gateway import Kind
from mavgate.net_asyncio import open_udp_gateway

async def main() -> None:
    async with await open_udp_gateway(
        remote_addr=("192.168.1.20", 14600),  # the gateway at the other end
        local_addr=("0.0.0.0", 14600),
        on_deliver=lambda kind, key, payload: print(kind, key, payload),
    ) as air:
        air.gateway.submit(b"ARM", Kind.CRITICAL, ttl=5.0)
        await asyncio.sleep(10)
        print(air.gateway.link_health())

asyncio.run(main())
```

The socket is connected to the peer, so datagrams from anyone else are dropped by the OS. Socket errors, such as the peer not running yet, count as loss.

To impair a real link without `tc netem` (for example on Windows or macOS), put `udp_proxy.py` in the middle. It applies the same `LinkParams` model as the simulator, with one seeded generator per direction:

```bash
python -m mavgate.udp_proxy --a-listen 127.0.0.1:14560 --b-listen 127.0.0.1:14561 --loss 0.3 --delay 0.08 --jitter 0.03 --outage 40:50
```

Point one gateway at port 14560 and the other at 14561. The proxy learns each side's address from its first packet. Unlike the simulator, real timing decides how many packets go each way, so a run cannot be replayed exactly from its seed.

## Testing approach

The network simulator (`sim.py`, `netsim.py`) uses a virtual clock and a seeded random generator. Nothing sleeps, so a two minute scenario runs in milliseconds and any failure replays from its seed.

The property tests in `tests/test_properties.py` generate random links (loss up to 60%, duplication, corruption, heavy jitter) and random command schedules, then check these guarantees:

1. No message is delivered twice.
2. No payload is delivered corrupted.
3. No message is delivered after its time to live, apart from packets already in flight.
4. Every critical message ends as acknowledged or given up, never both and never neither.
5. An acknowledged message was really delivered.
6. Telemetry for a stream never goes backwards.

## Benchmark results

### Mixed traffic through an outage (simulation, `bench/compare.py`)

Scenario, air to ground: radio capped at 5,760 B/s (57,600 baud), 20% loss, 80 ms delay, 30 ms jitter, a 10 second outage from t=40 s, eight telemetry streams at 20 Hz (about 9,600 B/s, more than the radio carries), and one critical message per second with an 8 second time to live. Mean of 20 seeds, 120 second sessions.

| Relay | Critical delivered | Critical p50 | Critical p99 | Telemetry delivered | Telemetry age p50 | Telemetry age p95 |
|---|---|---|---|---|---|---|
| Naive relay | 72.8% | 1.06 s | 1.07 s | 10,585 | 1.08 s | 1.11 s |
| mavgate gateway | 96.0% | 0.12 s | 6.47 s | 7,492 | 0.14 s | 0.17 s |

How to read it:

- **Critical delivery.** The best possible result here is about 97.5%, because the few messages submitted at the start of the outage expire before the link returns. The gateway reports those as given up.
- **Critical p99.** The gateway's 6.5 s comes from messages held during the outage and delivered once the link returned, within their time to live. The naive relay loses those messages outright.
- **Telemetry.** The gateway delivers fewer readings and they arrive about eight times fresher. It deliberately drops readings that a newer one has replaced. The naive relay's queue sits full, so everything it delivers is about a second old.

Caveats:

- These are simulated numbers under one model. They show the protocol logic works, not how a real radio behaves.
- The simulator drops lost packets before they use link time, which helps the naive relay's throughput. Real radios spend airtime on packets that are then lost.

### 100 waypoint mission upload at 30% loss (simulation, `bench/mission.py`)

A ground station uploads a 100 item mission while the radio (5,760 B/s, 80 ms delay, 30 ms jitter) loses 30% of packets each way. The two ends run a simplified mission protocol with ArduPilot-like timeouts; the script header lists them. With telemetry, the vehicle also streams 8 messages at 20 Hz. 20 seeds, 300 s cap.

| Relay | Completed | Mean time | Median | p90 |
|---|---|---|---|---|
| Naive relay, no telemetry | 100% | 114.7 s | 116.9 s | 132.9 s |
| mavgate gateway, no telemetry | 100% | 52.6 s | 51.9 s | 57.8 s |
| Naive relay, with telemetry | 75% | 215.3 s | 218.0 s | 228.6 s |
| mavgate gateway, with telemetry | 100% | 51.1 s | 51.5 s | 56.8 s |

Without the gateway, a lost request or item costs a full protocol timeout (about 1 s), and with telemetry the mission messages also wait behind a full radio queue. The gateway retransmits after about one RTT and sends mission traffic ahead of telemetry.

### The same upload against ArduPilot SITL (real time, `bench/sitl_mission.py`)

ArduPilot Copter SITL (built from `master`, October 2026), with real sockets throughout. The rig is SITL -> air bridge -> `udp_proxy` -> ground bridge -> a pymavlink ground station script; "direct" is SITL -> `udp_proxy` -> script. The proxy applies 30% loss each way, 80 ms delay, 30 ms jitter and 5,760 B/s. The script asks SITL for all telemetry streams at 10 Hz. After each upload, the mission is downloaded over a second, clean SITL port and compared item by item. Three runs per mode, run under WSL2.

| Path | Completed and verified | Times |
|---|---|---|
| Direct | 2 of 3 | 112.9 s, 97.4 s (third run failed after 4 attempts in 300 s) |
| Through mavgate | 3 of 3 | 53.5 s, 52.5 s, 52.9 s |

Three runs per mode is a small sample. The proxy logs show that ArduPilot's 10 Hz telemetry never overflowed the simulated radio queue in direct mode. So the speed-up here comes from fast retransmission, not from protecting mission traffic against telemetry congestion.

A flight was also flown through the gateway at 30% loss, with `--fly`. The mission was takeoff, six waypoints and RTL. In two flights the upload took 8.8 s and 7.8 s and was verified each time. Each time the vehicle armed, took off, ran the mission in AUTO, reached all 8 items, landed and disarmed. On the second flight the clean channel put the landing 0.0 m from home (SITL has no wind or GPS noise by default). All commands from the ground script went through the lossy link.

### Adaptive rate and fairness (simulation, `bench/adaptive_fair.py`)

A. The radio carries 2,880 B/s but the gateway is configured for 5,184 B/s. 5% loss, 8 telemetry streams at 20 Hz, one critical message per second.

| Telemetry rate | Critical delivered | Critical p50 | Critical p99 | Telemetry delivered | Age p50 | Age p95 |
|---|---|---|---|---|---|---|
| Fixed | 100.0% | 1.10 s | 2.31 s | 5,639 | 1.12 s | 1.14 s |
| Adaptive | 100.0% | 0.14 s | 1.11 s | 5,047 | 0.19 s | 0.88 s |

With a fixed rate the radio's queue sits full, so every message waits about a second, commands included. Adaptation keeps the queue mostly empty. The p95 age shows the sawtooth as the rate probes back up.

B. Two vehicles share a 5,760 B/s radio. Vehicle 1 has 24 telemetry streams and vehicle 2 has 4. Vehicle 1 queues 100 critical messages at t=30 s, and vehicle 2 sends one command half a second later.

| Scheduling | Vehicle 2 share of telemetry | Vehicle 1 age p50 | Vehicle 2 age p50 | Vehicle 2 command latency |
|---|---|---|---|---|
| Single queue | 13.2% | 0.12 s | 0.11 s | 0.84 s |
| Per vehicle | 50.0% | 0.12 s | 0.12 s | 0.15 s |

## Layout

```
src/mavgate/
  sim.py        virtual clock and event queue
  netsim.py     lossy, rate-limited one-way channel
  protocol.py   wire format and CRC
  gateway.py    the gateway itself
  naive.py      baseline relay for comparison
  classify.py   MAVLink message ids to critical or telemetry
  harness.py    wires two gateways through the simulator
  net_asyncio.py  runs a gateway on a real UDP socket (asyncio)
  udp_proxy.py    lossy UDP proxy using the simulator's link model, with a CLI
  mavlink_bridge.py  local MAVLink UDP <-> classify <-> gateway, with a CLI
tests/          unit, property, real-socket and bridge tests
bench/          benchmark scripts (simulation and SITL)
fuzz/           fuzz target for every parser that faces outside bytes
```

## Known limitations

- Critical messages are delivered at most once but not in order. A mission upload that needs order should add an in-order mode.
- Sequence numbers are 32 bit and do not wrap. Duplicate tracking keeps a sliding window of recent numbers.
- A message that is delivered but whose acknowledgements are all lost is reported to the sender as "given up", because the sender cannot know it arrived.
- No authentication or encryption. Do not use this on a real vehicle as it stands.
- MAVLink message ids in `classify.py` should be checked against the dialect your autopilot uses.
- The UDP adapter works around two bugs in asyncio's Windows UDP transport (see the notes at the top of `net_asyncio.py`). Re-check them when moving to a newer Python.
- There is no serial adapter yet. A serial radio needs packet framing, because a byte stream does not keep packet boundaries.
- Telemetry keys are (system id, message id). Messages that share an id but are separate instances (BATTERY_STATUS by battery, NAMED_VALUE_FLOAT by name, HEARTBEAT from several components) replace one another when the link is congested.
- The bridge treats all ground-to-vehicle traffic as one flow, the ground station's. Fairness between vehicles applies to vehicle-to-ground traffic.
- The fuzz target has run without Atheris here (200,000 seeded inputs, no failures) but not under Atheris itself.

## Roadmap

1. ~~An asyncio adapter so the gateway runs over real UDP links.~~ Done: `net_asyncio.py`, with `udp_proxy.py` for impairments.
2. ~~Connect to ArduPilot SITL with `pymavlink`, and fly a mission through the gateway.~~ Done: `mavlink_bridge.py`, `bench/sitl_mission.py --fly`.
3. ~~A mission upload benchmark at 30% loss, with and without the gateway.~~ Done, in simulation and against SITL.
4. Re-run the benchmarks with Linux `tc netem` impairments instead of `udp_proxy`.
5. ~~Adaptive telemetry rate based on the link health estimate.~~ Done (`adaptive_telemetry`).
6. ~~Several vehicles sharing one link, with fair bandwidth between them.~~ Done (flows).
7. Fuzz target written (`fuzz/fuzz_decode.py`). Still to do: run it under Atheris.
8. A serial adapter with packet framing.
9. An optional in-order delivery mode for critical messages.
