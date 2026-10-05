import pytest
from hypothesis import given
from hypothesis import strategies as st

from mavgate import protocol as P


def test_roundtrip_each_packet_type() -> None:
    packets: list[P.Packet] = [
        P.Crit(seq=7, ttl_ms=1500, payload=b"arm"),
        P.Tele(key=(1 << 24) | 33, seq=3, payload=b"\x00" * 28),
        P.Ack(seq=99),
        P.Heartbeat(counter=5),
    ]
    for pkt in packets:
        assert P.decode(P.encode(pkt)) == pkt


@given(st.binary(max_size=400))
def test_decode_never_raises_anything_but_protocol_error(data: bytes) -> None:
    try:
        P.decode(data)
    except P.ProtocolError:
        pass


@given(
    st.binary(min_size=0, max_size=P.MAX_PAYLOAD),
    st.integers(min_value=0, max_value=2**32 - 1),
    st.data(),
)
def test_any_single_bit_flip_is_detected(payload: bytes, seq: int, data: st.DataObject) -> None:
    raw = bytearray(P.encode(P.Crit(seq=seq, ttl_ms=1000, payload=payload)))
    idx = data.draw(st.integers(min_value=0, max_value=len(raw) - 1))
    bit = data.draw(st.integers(min_value=0, max_value=7))
    raw[idx] ^= 1 << bit
    with pytest.raises(P.ProtocolError):
        P.decode(bytes(raw))


def test_oversized_payload_is_rejected() -> None:
    big = P.encode(P.Crit(seq=1, ttl_ms=10, payload=b"x" * (P.MAX_PAYLOAD + 1)))
    with pytest.raises(P.ProtocolError):
        P.decode(big)


def test_heartbeat_echo_roundtrip() -> None:
    assert P.decode(P.encode(P.HeartbeatEcho(counter=42))) == P.HeartbeatEcho(counter=42)
