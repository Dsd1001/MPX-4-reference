from __future__ import annotations

from .errors import DecodeError, NeedMoreData

MAX_VARINT = (1 << 62) - 1


def encoded_length(value: int) -> int:
    if value < 0 or value > MAX_VARINT:
        raise ValueError("MPX VarInt out of range")
    if value <= 63:
        return 1
    if value <= 16383:
        return 2
    if value <= 1073741823:
        return 4
    return 8


def encode_varint(value: int) -> bytes:
    width = encoded_length(value)
    if width == 1:
        return bytes([value])
    if width == 2:
        return (value | 0x4000).to_bytes(2, "big")
    if width == 4:
        return (value | 0x80000000).to_bytes(4, "big")
    return (value | 0xC000000000000000).to_bytes(8, "big")


def width_from_first_octet(first: int) -> int:
    return 1 << (first >> 6)


def decode_varint(data: bytes | bytearray | memoryview, offset: int = 0) -> tuple[int, int]:
    if offset < 0:
        raise ValueError("negative offset")
    if offset >= len(data):
        raise NeedMoreData("need VarInt first octet")

    width = width_from_first_octet(data[offset])
    end = offset + width
    if end > len(data):
        raise NeedMoreData("truncated MPX VarInt")

    raw = int.from_bytes(data[offset:end], "big")
    if width == 1:
        value = raw & 0x3F
    elif width == 2:
        value = raw & 0x3FFF
    elif width == 4:
        value = raw & 0x3FFFFFFF
    else:
        value = raw & MAX_VARINT

    if encoded_length(value) != width:
        raise DecodeError("non-canonical MPX VarInt")

    return value, end


def decode_varint_exact(data: bytes | bytearray | memoryview) -> int:
    value, end = decode_varint(data, 0)
    if end != len(data):
        raise DecodeError("trailing octets after MPX VarInt")
    return value
