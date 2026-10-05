from mavgate.classify import classify, split_frames
from mavgate.gateway import Kind


def v1(msgid: int, payload: bytes = b"\x01\x02") -> bytes:
    return bytes([0xFE, len(payload), 0, 1, 1, msgid]) + payload + b"\xaa\xbb"


def v2(msgid: int, payload: bytes = b"\x01\x02\x03", signed: bool = False) -> bytes:
    head = bytes([0xFD, len(payload), 0x01 if signed else 0, 0, 0, 1, 1])
    frame = head + msgid.to_bytes(3, "little") + payload + b"\xaa\xbb"
    return frame + (b"S" * 13 if signed else b"")


def test_split_returns_each_frame_of_a_packed_datagram() -> None:
    frames = [v2(0), v1(30), v2(33, b"x" * 28), v2(76, signed=True)]
    got, rest = split_frames(b"".join(frames))
    assert got == frames
    assert rest == b""


def test_split_skips_junk_and_keeps_a_partial_tail() -> None:
    a, b = v2(33), v2(76)
    got, rest = split_frames(b"junk" + a + b"\x00\x13" + b[:5])
    assert got == [a]
    assert rest == b[:5]


def test_split_handles_empty_and_magic_only_input() -> None:
    assert split_frames(b"") == ([], b"")
    assert split_frames(b"\xfd") == ([], b"\xfd")


def test_mission_and_param_traffic_is_critical() -> None:
    for msgid in (21, 22, 38, 41, 46, 80, 110, 253):  # params, mission, ftp, statustext
        assert classify(v2(msgid))[0] is Kind.CRITICAL, msgid


def test_streamed_setpoints_and_rc_stay_telemetry() -> None:
    for msgid in (69, 70, 84, 86):  # MANUAL_CONTROL, RC override, position targets
        assert classify(v2(msgid))[0] is Kind.TELEMETRY, msgid
