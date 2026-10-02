from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .constants import (
    FrameType,
    HandshakeType,
    MAX_HANDSHAKE_MESSAGE,
    ParameterType,
)
from .errors import DecodeError, NeedMoreData
from .varint import decode_varint, decode_varint_exact, encode_varint


@dataclass(frozen=True)
class Parameter:
    type: int
    flags: int
    value: bytes


@dataclass(frozen=True)
class HandshakeMessage:
    type: int
    body: bytes
    raw: bytes


@dataclass(frozen=True)
class Frame:
    type: int
    body: bytes


@dataclass(frozen=True)
class StreamData:
    stream_id: int
    offset: int
    transmission_id: int
    data: bytes


@dataclass(frozen=True)
class TransmissionAck:
    stream_id: int
    transmission_id: int
    receiver_timestamp_us: int


@dataclass(frozen=True)
class StreamCredit:
    stream_id: int
    consumed_offset: int
    maximum_offset: int


@dataclass(frozen=True)
class SessionCredit:
    consumed_bytes: int
    maximum_bytes: int


@dataclass(frozen=True)
class StreamTerminal:
    stream_id: int
    transmission_id: int
    final_offset: int


def encode_parameter(parameter: Parameter) -> bytes:
    if parameter.flags & 0xFE:
        raise ValueError("reserved Parameter flag bits must be zero")
    return (
        encode_varint(parameter.type)
        + bytes([parameter.flags])
        + encode_varint(len(parameter.value))
        + parameter.value
    )


def encode_parameters(parameters: Iterable[Parameter]) -> bytes:
    items = list(parameters)
    previous = -1
    out = bytearray()
    for parameter in items:
        if parameter.type <= previous:
            raise ValueError("Parameters must be strictly increasing by type")
        previous = parameter.type
        out += encode_parameter(parameter)
    return bytes(out)


def decode_parameters(body: bytes) -> list[Parameter]:
    out: list[Parameter] = []
    offset = 0
    previous = -1

    while offset < len(body):
        ptype, offset = decode_varint(body, offset)
        if ptype <= previous:
            raise DecodeError("duplicate or out-of-order Parameter")
        previous = ptype

        if offset >= len(body):
            raise NeedMoreData("truncated Parameter flags")
        flags = body[offset]
        offset += 1
        if flags & 0xFE:
            raise DecodeError("reserved Parameter flag bits are non-zero")

        length, offset = decode_varint(body, offset)
        end = offset + length
        if end > len(body):
            raise NeedMoreData("truncated Parameter value")

        out.append(Parameter(ptype, flags, bytes(body[offset:end])))
        offset = end

    return out


def parameter_bytes(ptype: int | ParameterType, value: bytes, *, critical: bool = False) -> Parameter:
    return Parameter(int(ptype), 0x01 if critical else 0x00, bytes(value))


def parameter_varint(ptype: int | ParameterType, value: int, *, critical: bool = False) -> Parameter:
    return parameter_bytes(ptype, encode_varint(value), critical=critical)


def parameter_map(parameters: Iterable[Parameter]) -> dict[int, Parameter]:
    return {parameter.type: parameter for parameter in parameters}


def parameter_varint_value(parameter: Parameter) -> int:
    return decode_varint_exact(parameter.value)


def encode_handshake_message(message_type: int | HandshakeType, body: bytes) -> bytes:
    if len(body) > MAX_HANDSHAKE_MESSAGE:
        raise ValueError("handshake message exceeds Draft 03 limit")
    return encode_varint(int(message_type)) + encode_varint(len(body)) + body


def decode_handshake_message(data: bytes, offset: int = 0) -> tuple[HandshakeMessage, int]:
    start = offset
    mtype, offset = decode_varint(data, offset)
    length, offset = decode_varint(data, offset)
    if length > MAX_HANDSHAKE_MESSAGE:
        raise DecodeError("handshake message exceeds Draft 03 limit")
    end = offset + length
    if end > len(data):
        raise NeedMoreData("truncated handshake message")
    raw = bytes(data[start:end])
    return HandshakeMessage(mtype, bytes(data[offset:end]), raw), end


def encode_frame(frame_type: int | FrameType, body: bytes) -> bytes:
    return encode_varint(int(frame_type)) + encode_varint(len(body)) + body


def decode_frame(data: bytes, offset: int = 0) -> tuple[Frame, int]:
    ftype, offset = decode_varint(data, offset)
    length, offset = decode_varint(data, offset)
    end = offset + length
    if end > len(data):
        raise NeedMoreData("truncated Frame")
    return Frame(ftype, bytes(data[offset:end])), end


def decode_frames(data: bytes) -> list[Frame]:
    frames: list[Frame] = []
    offset = 0
    while offset < len(data):
        frame, offset = decode_frame(data, offset)
        frames.append(frame)
    return frames


def _decode_exact_varints(body: bytes, count: int) -> tuple[int, ...]:
    values: list[int] = []
    offset = 0
    for _ in range(count):
        value, offset = decode_varint(body, offset)
        values.append(value)
    if offset != len(body):
        raise DecodeError("unexpected trailing Frame body octets")
    return tuple(values)


def _expect_frame(frame: Frame, expected: FrameType) -> None:
    if frame.type != int(expected):
        raise DecodeError(f"expected {expected.name}")


def encode_ping(token: int) -> bytes:
    return encode_frame(FrameType.PING, encode_varint(token))


def encode_pong(token: int) -> bytes:
    return encode_frame(FrameType.PONG, encode_varint(token))


def decode_token_frame(frame: Frame) -> int:
    if frame.type not in (int(FrameType.PING), int(FrameType.PONG)):
        raise DecodeError("Frame is not PING/PONG")
    return decode_varint_exact(frame.body)


def encode_stream_open(stream_id: int, transmission_id: int) -> bytes:
    if stream_id <= 0 or transmission_id <= 0:
        raise ValueError("invalid STREAM_OPEN fields")
    return encode_frame(
        FrameType.STREAM_OPEN,
        encode_varint(stream_id) + encode_varint(transmission_id),
    )


def decode_stream_open(frame: Frame) -> tuple[int, int]:
    _expect_frame(frame, FrameType.STREAM_OPEN)
    stream_id, transmission_id = _decode_exact_varints(frame.body, 2)
    if stream_id <= 0 or transmission_id <= 0:
        raise DecodeError("invalid STREAM_OPEN fields")
    return stream_id, transmission_id


def encode_stream_open_ok(stream_id: int, transmission_id: int) -> bytes:
    if stream_id <= 0 or transmission_id <= 0:
        raise ValueError("invalid STREAM_OPEN_OK fields")
    return encode_frame(
        FrameType.STREAM_OPEN_OK,
        encode_varint(stream_id) + encode_varint(transmission_id),
    )


def decode_stream_open_ok(frame: Frame) -> tuple[int, int]:
    _expect_frame(frame, FrameType.STREAM_OPEN_OK)
    stream_id, transmission_id = _decode_exact_varints(frame.body, 2)
    if stream_id <= 0 or transmission_id <= 0:
        raise DecodeError("invalid STREAM_OPEN_OK fields")
    return stream_id, transmission_id


def encode_stream_open_reject(stream_id: int, transmission_id: int, error_code: int) -> bytes:
    if stream_id <= 0 or transmission_id <= 0:
        raise ValueError("invalid STREAM_OPEN_REJECT fields")
    return encode_frame(
        FrameType.STREAM_OPEN_REJECT,
        encode_varint(stream_id)
        + encode_varint(transmission_id)
        + encode_varint(error_code),
    )


def encode_stream_data(stream_id: int, offset: int, transmission_id: int, data: bytes) -> bytes:
    if stream_id <= 0 or transmission_id <= 0 or not data:
        raise ValueError("invalid STREAM_DATA fields")
    body = (
        encode_varint(stream_id)
        + encode_varint(offset)
        + encode_varint(transmission_id)
        + data
    )
    return encode_frame(FrameType.STREAM_DATA, body)


def decode_stream_data(frame: Frame) -> StreamData:
    _expect_frame(frame, FrameType.STREAM_DATA)
    offset = 0
    stream_id, offset = decode_varint(frame.body, offset)
    stream_offset, offset = decode_varint(frame.body, offset)
    transmission_id, offset = decode_varint(frame.body, offset)
    data = bytes(frame.body[offset:])
    if stream_id <= 0 or transmission_id <= 0 or not data:
        raise DecodeError("invalid STREAM_DATA fields")
    return StreamData(stream_id, stream_offset, transmission_id, data)


def encode_transmission_ack(stream_id: int, transmission_id: int, timestamp_us: int) -> bytes:
    if stream_id <= 0 or transmission_id <= 0:
        raise ValueError("invalid TRANSMISSION_ACK fields")
    body = (
        encode_varint(stream_id)
        + encode_varint(transmission_id)
        + encode_varint(timestamp_us)
    )
    return encode_frame(FrameType.TRANSMISSION_ACK, body)


def decode_transmission_ack(frame: Frame) -> TransmissionAck:
    _expect_frame(frame, FrameType.TRANSMISSION_ACK)
    stream_id, transmission_id, timestamp_us = _decode_exact_varints(frame.body, 3)
    if stream_id <= 0 or transmission_id <= 0:
        raise DecodeError("invalid TRANSMISSION_ACK fields")
    return TransmissionAck(stream_id, transmission_id, timestamp_us)


def encode_stream_credit(stream_id: int, consumed_offset: int, maximum_offset: int) -> bytes:
    if stream_id <= 0:
        raise ValueError("invalid Stream ID")
    if maximum_offset < consumed_offset:
        raise ValueError("decreasing STREAM_CREDIT")
    body = (
        encode_varint(stream_id)
        + encode_varint(consumed_offset)
        + encode_varint(maximum_offset)
    )
    return encode_frame(FrameType.STREAM_CREDIT, body)


def decode_stream_credit(frame: Frame) -> StreamCredit:
    _expect_frame(frame, FrameType.STREAM_CREDIT)
    stream_id, consumed, maximum = _decode_exact_varints(frame.body, 3)
    if stream_id <= 0 or maximum < consumed:
        raise DecodeError("invalid STREAM_CREDIT")
    return StreamCredit(stream_id, consumed, maximum)


def encode_session_credit(consumed_bytes: int, maximum_bytes: int) -> bytes:
    if maximum_bytes < consumed_bytes:
        raise ValueError("decreasing SESSION_CREDIT")
    body = encode_varint(consumed_bytes) + encode_varint(maximum_bytes)
    return encode_frame(FrameType.SESSION_CREDIT, body)


def decode_session_credit(frame: Frame) -> SessionCredit:
    _expect_frame(frame, FrameType.SESSION_CREDIT)
    consumed, maximum = _decode_exact_varints(frame.body, 2)
    if maximum < consumed:
        raise DecodeError("invalid SESSION_CREDIT")
    return SessionCredit(consumed, maximum)


def encode_stream_fin(stream_id: int, transmission_id: int, final_offset: int) -> bytes:
    if stream_id <= 0 or transmission_id <= 0:
        raise ValueError("invalid STREAM_FIN fields")
    return encode_frame(
        FrameType.STREAM_FIN,
        encode_varint(stream_id)
        + encode_varint(transmission_id)
        + encode_varint(final_offset),
    )


def decode_stream_fin(frame: Frame) -> StreamTerminal:
    _expect_frame(frame, FrameType.STREAM_FIN)
    stream_id, transmission_id, final_offset = _decode_exact_varints(frame.body, 3)
    if stream_id <= 0 or transmission_id <= 0:
        raise DecodeError("invalid STREAM_FIN fields")
    return StreamTerminal(stream_id, transmission_id, final_offset)


def encode_stream_consumed(stream_id: int, transmission_id: int, final_offset: int) -> bytes:
    if stream_id <= 0 or transmission_id <= 0:
        raise ValueError("invalid STREAM_CONSUMED fields")
    return encode_frame(
        FrameType.STREAM_CONSUMED,
        encode_varint(stream_id)
        + encode_varint(transmission_id)
        + encode_varint(final_offset),
    )


def decode_stream_consumed(frame: Frame) -> StreamTerminal:
    _expect_frame(frame, FrameType.STREAM_CONSUMED)
    stream_id, transmission_id, final_offset = _decode_exact_varints(frame.body, 3)
    if stream_id <= 0 or transmission_id <= 0:
        raise DecodeError("invalid STREAM_CONSUMED fields")
    return StreamTerminal(stream_id, transmission_id, final_offset)


def encode_reset_stream(stream_id: int, transmission_id: int, final_offset: int, error_code: int) -> bytes:
    if stream_id <= 0 or transmission_id <= 0:
        raise ValueError("invalid RESET_STREAM fields")
    return encode_frame(
        FrameType.RESET_STREAM,
        encode_varint(stream_id)
        + encode_varint(transmission_id)
        + encode_varint(final_offset)
        + encode_varint(error_code),
    )


def encode_stop_sending(stream_id: int, transmission_id: int, error_code: int) -> bytes:
    if stream_id <= 0 or transmission_id <= 0:
        raise ValueError("invalid STOP_SENDING fields")
    return encode_frame(
        FrameType.STOP_SENDING,
        encode_varint(stream_id)
        + encode_varint(transmission_id)
        + encode_varint(error_code),
    )


def encode_credit_probe(stream_id: int) -> bytes:
    return encode_frame(FrameType.CREDIT_PROBE, encode_varint(stream_id))
