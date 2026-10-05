"""Fuzz target for everything that parses bytes from the radio or the autopilot.

With Atheris (Linux or macOS, `pip install atheris`):

    python fuzz/fuzz_decode.py -max_total_time=600 fuzz/corpus

Without it, the same checks run on seeded random inputs and mutations of valid
packets, which is weaker but runs anywhere:

    python fuzz/fuzz_decode.py --no-atheris 200000

Checks, for any input:
  * `protocol.decode` raises nothing but ProtocolError, and anything it
    accepts encodes back to exactly the same bytes (one encoding per packet);
  * `classify.split_frames` returns frames that are slices of the input, each
    starting with a MAVLink magic byte, and leaves only an incomplete tail;
  * `Gateway.receive` never raises, whatever arrives.
"""

from __future__ import annotations

import random
import sys
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from mavgate import protocol as P  # noqa: E402
from mavgate.classify import classify, split_frames  # noqa: E402
from mavgate.gateway import Gateway  # noqa: E402


class _StillClock:
    now = 0.0

    def call_later(self, delay: float, fn: Callable[..., None], *args: Any) -> None:
        pass  # the send loop never runs; only receive is under test


_gateway = Gateway(_StillClock(), lambda data: None)


def check(data: bytes) -> None:
    try:
        pkt = P.decode(data)
    except P.ProtocolError:
        pass
    else:
        assert P.encode(pkt) == data, "decode accepted a non-canonical packet"

    frames, rest = split_frames(data)
    assert data.endswith(rest)
    assert sum(map(len, frames)) + len(rest) <= len(data)
    for frame in frames:
        assert frame[0] in (0xFD, 0xFE) and frame in data
        classify(frame)

    _gateway.receive(data)


def seeds() -> list[bytes]:
    return [
        P.encode(P.Crit(7, 1500, b"\xfd\x09\x00\x00\x00\x01\x01\x4c\x00\x00arm")),
        P.encode(P.Tele((1 << 24) | 33, 3, b"\x00" * 28)),
        P.encode(P.Ack(99)),
        P.encode(P.Heartbeat(5)),
        P.encode(P.HeartbeatEcho(5)),
        bytes([0xFE, 2, 0, 1, 1, 30, 1, 2, 0xAA, 0xBB]),
        bytes([0xFD, 3, 1, 0, 0, 1, 1, 76, 0, 0, 1, 2, 3, 0xAA, 0xBB]) + b"S" * 13,
    ]


def mutate(rng: random.Random, data: bytes) -> bytes:
    buf = bytearray(data)
    for _ in range(rng.randint(1, 4)):
        op = rng.randrange(4)
        if op == 0 and buf:
            buf[rng.randrange(len(buf))] ^= 1 << rng.randrange(8)
        elif op == 1:
            buf.insert(rng.randrange(len(buf) + 1), rng.randrange(256))
        elif op == 2 and buf:
            del buf[rng.randrange(len(buf))]
        else:
            buf += rng.choice(seeds())
    return bytes(buf)


def run_without_atheris(iterations: int, seed: int = 0) -> None:
    rng = random.Random(seed)
    pool = seeds()
    for i in range(iterations):
        if i % 3 == 0:
            data = rng.randbytes(rng.randrange(0, 400))
        else:
            data = mutate(rng, rng.choice(pool))
        check(data)
        check(P.encode(P.Crit(i, i, data[: P.MAX_PAYLOAD])))  # valid wrapper, odd payload


def main() -> None:
    if len(sys.argv) >= 2 and sys.argv[1] == "--no-atheris":
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 100_000
        run_without_atheris(n)
        print(f"{n} inputs, no failures")
        return
    import atheris

    atheris.Setup(sys.argv, check)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
