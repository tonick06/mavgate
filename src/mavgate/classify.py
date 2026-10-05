"""Decide whether a MAVLink message is CRITICAL or TELEMETRY.

The gateway core treats payloads as opaque bytes. This module is the only place
that knows about MAVLink. It reads the message id straight from the frame header
so it has no dependency on pymavlink.

The ids below come from MAVLink's `common.xml` dialect. Check them against the
dialect your autopilot actually uses before trusting them.
"""

from __future__ import annotations

from .gateway import Kind

# MAVLink 1 frames start with 0xFE, MAVLink 2 frames with 0xFD.
_V1_MAGIC = 0xFE
_V2_MAGIC = 0xFD

_V1_OVERHEAD = 8  # magic, len, seq, sysid, compid, msgid, crc(2)
_V2_OVERHEAD = 12  # magic, len, incompat, compat, seq, sysid, compid, msgid(3), crc(2)
_V2_SIGNATURE = 13
_V2_SIGNED = 0x01  # incompat flag

# Messages that change vehicle state, carry a mission or parameters, or are
# one-off events. Lose one and the plan breaks. Each one is also distinct, so
# "latest wins" would be wrong: a parameter download sends hundreds of
# PARAM_VALUE messages that share one id.
CRITICAL_MSG_IDS: frozenset[int] = frozenset(
    {
        11,  # SET_MODE
        20,  # PARAM_REQUEST_READ
        21,  # PARAM_REQUEST_LIST
        22,  # PARAM_VALUE
        23,  # PARAM_SET
        38,  # MISSION_WRITE_PARTIAL_LIST
        39,  # MISSION_ITEM
        40,  # MISSION_REQUEST
        41,  # MISSION_SET_CURRENT
        43,  # MISSION_REQUEST_LIST
        44,  # MISSION_COUNT
        45,  # MISSION_CLEAR_ALL
        46,  # MISSION_ITEM_REACHED
        47,  # MISSION_ACK
        51,  # MISSION_REQUEST_INT
        73,  # MISSION_ITEM_INT
        75,  # COMMAND_INT
        76,  # COMMAND_LONG
        77,  # COMMAND_ACK
        80,  # COMMAND_CANCEL
        110,  # FILE_TRANSFER_PROTOCOL
        253,  # STATUSTEXT
    }
)


def split_frames(data: bytes) -> tuple[list[bytes], bytes]:
    """Cut a buffer into whole MAVLink frames.

    Returns the frames and any incomplete frame left at the end. Bytes that
    cannot start a frame are skipped. A UDP datagram can hold several frames
    (ArduPilot packs them), so frames are split before they are classified.
    Checksums are not verified here; the receiving autopilot or ground station
    does that.
    """
    frames: list[bytes] = []
    i, n = 0, len(data)
    while i < n:
        magic = data[i]
        if magic == _V1_MAGIC:
            if i + 2 > n:
                break
            size = _V1_OVERHEAD + data[i + 1]
        elif magic == _V2_MAGIC:
            if i + 3 > n:
                break
            size = _V2_OVERHEAD + data[i + 1]
            if data[i + 2] & _V2_SIGNED:
                size += _V2_SIGNATURE
        else:
            i += 1
            continue
        if i + size > n:
            break
        frames.append(data[i : i + size])
        i += size
    return frames, data[i:]


def parse_header(frame: bytes) -> tuple[int, int, int] | None:
    """Return (system_id, component_id, message_id), or None if not a MAVLink frame."""
    if len(frame) >= 8 and frame[0] == _V1_MAGIC:
        return frame[3], frame[4], frame[5]
    if len(frame) >= 10 and frame[0] == _V2_MAGIC:
        msgid = frame[7] | (frame[8] << 8) | (frame[9] << 16)
        return frame[5], frame[6], msgid
    return None


def classify(frame: bytes) -> tuple[Kind, int | None]:
    """Return the kind and, for telemetry, the key used for latest-wins replacement.

    The key packs the system id into the top byte so each vehicle's streams stay
    separate. Unknown or malformed frames are treated as critical, which is the
    safe default: they are delivered reliably rather than silently thinned out.
    """
    header = parse_header(frame)
    if header is None:
        return Kind.CRITICAL, None
    sysid, _, msgid = header
    if msgid in CRITICAL_MSG_IDS:
        return Kind.CRITICAL, None
    return Kind.TELEMETRY, ((sysid & 0xFF) << 24) | (msgid & 0xFFFFFF)
