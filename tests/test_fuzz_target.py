"""Keep the fuzz target honest: run it on generated and mutated inputs."""

import importlib.util
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

_spec = importlib.util.spec_from_file_location(
    "fuzz_decode", Path(__file__).parent.parent / "fuzz" / "fuzz_decode.py"
)
assert _spec is not None and _spec.loader is not None
fuzz = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fuzz)


@settings(max_examples=300)
@given(st.one_of(st.binary(max_size=400), st.sampled_from(fuzz.seeds()), st.data()))
def test_fuzz_checks_hold(data: object) -> None:
    if isinstance(data, bytes):
        fuzz.check(data)
    else:
        seed = data.draw(st.sampled_from(fuzz.seeds()))  # type: ignore[attr-defined]
        flip = data.draw(st.integers(0, len(seed) * 8 - 1))  # type: ignore[attr-defined]
        raw = bytearray(seed)
        raw[flip // 8] ^= 1 << (flip % 8)
        fuzz.check(bytes(raw))


def test_seeded_mutation_run() -> None:
    fuzz.run_without_atheris(3000)
