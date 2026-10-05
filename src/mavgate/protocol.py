"""Wire format between the two gateways.

Every packet ends in a CRC32 so corrupted packets are dropped, never delivered.
Layouts (network byte order):

    CRIT       type u8 | seq u32 | ttl_ms u32 | len u16 | payload | crc u32
    TELE       type u8 | key u32 | seq u32 | len u16 | payload | crc u32
    ACK        type u8 | seq u32 | crc u32
    HEARTBEAT  type u8 | counter u32 | crc u32
    HB_ECHO    type u8 | counter u32 | crc u32   (reply to a heartbeat, for RTT)

`ttl_ms` is the time the message has left to live, measured when the packet
was sent. It is relative so the two ends never need synchronised clocks.
"""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass

T_CRIT = 1
T_TELE = 2
T_ACK = 3
T_HB = 4
T_HB_ECHO = 5

MAX_PAYLOAD = 280  # largest MAVLink 2 frame

_HEAD_DATA = struct.Struct("!BIIH")
_HEAD_SMALL = struct.Struct("!BI")
_CRC = struct.Struct("!I")


class ProtocolError(ValueError):
    """Raised for any packet that is malformed or fails its checksum."""


@dataclass(frozen=True)
class Crit:
    seq: int
    ttl_ms: int
    payload: bytes


@dataclass(frozen=True)
class Tele:
    key: int
    seq: int
    payload: bytes


@dataclass(frozen=True)
class Ack:
    seq: int


@dataclass(frozen=True)
class Heartbeat:
    counter: int


@dataclass(frozen=True)
class HeartbeatEcho:
    counter: int


Packet = Crit | Tele | Ack | Heartbeat | HeartbeatEcho


def encode(pkt: Packet) -> bytes:
    if isinstance(pkt, Crit):
        body = _HEAD_DATA.pack(T_CRIT, pkt.seq, pkt.ttl_ms, len(pkt.payload)) + pkt.payload
    elif isinstance(pkt, Tele):
        body = _HEAD_DATA.pack(T_TELE, pkt.key, pkt.seq, len(pkt.payload)) + pkt.payload
    elif isinstance(pkt, Ack):
        body = _HEAD_SMALL.pack(T_ACK, pkt.seq)
    elif isinstance(pkt, Heartbeat):
        body = _HEAD_SMALL.pack(T_HB, pkt.counter)
    else:
        body = _HEAD_SMALL.pack(T_HB_ECHO, pkt.counter)
    return body + _CRC.pack(zlib.crc32(body))


def decode(data: bytes) -> Packet:
    """Parse one packet. Raises ProtocolError on anything wrong, never anything else."""
    if len(data) < _HEAD_SMALL.size + _CRC.size:
        raise ProtocolError("too short")
    body, crc_bytes = data[: -_CRC.size], data[-_CRC.size :]
    if _CRC.unpack(crc_bytes)[0] != zlib.crc32(body):
        raise ProtocolError("bad checksum")

    ptype = body[0]
    if ptype in (T_ACK, T_HB, T_HB_ECHO):
        if len(body) != _HEAD_SMALL.size:
            raise ProtocolError("bad length")
        value = _HEAD_SMALL.unpack(body)[1]
        if ptype == T_ACK:
            return Ack(value)
        return Heartbeat(value) if ptype == T_HB else HeartbeatEcho(value)

    if ptype in (T_CRIT, T_TELE):
        if len(body) < _HEAD_DATA.size:
            raise ProtocolError("short header")
        _, a, b, length = _HEAD_DATA.unpack(body[: _HEAD_DATA.size])
        payload = body[_HEAD_DATA.size :]
        if length != len(payload) or length > MAX_PAYLOAD:
            raise ProtocolError("bad payload length")
        return Crit(a, b, payload) if ptype == T_CRIT else Tele(a, b, payload)

    raise ProtocolError("unknown type")
