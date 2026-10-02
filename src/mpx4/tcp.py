from __future__ import annotations

from dataclasses import dataclass
import socket

from .codec import HandshakeMessage, decode_handshake_message
from .constants import MAGIC, VERSION
from .crypto import WireRecord, decode_wire_record
from .errors import DecodeError, NeedMoreData
from .varint import decode_varint, width_from_first_octet


@dataclass(frozen=True)
class ConnectionPreface:
    version: int
    raw: bytes


class IncrementalBindingParser:
    """Incremental parser for the MPX/4 TCP byte stream.

    The caller controls whether the stream is in handshake or Secure Record mode.
    Unconsumed bytes remain buffered across calls.
    """

    def __init__(self):
        self._buffer = bytearray()
        self._preface_done = False
        self._record_mode = False

    @property
    def pending_bytes(self) -> int:
        return len(self._buffer)

    def feed(self, data: bytes) -> None:
        self._buffer += data

    def pop_preface(self) -> ConnectionPreface | None:
        if self._preface_done:
            raise DecodeError("Connection Preface already parsed")
        if len(self._buffer) < len(MAGIC) + 1:
            return None
        if bytes(self._buffer[:4]) != MAGIC:
            raise DecodeError("invalid MPX magic")
        try:
            version, end = decode_varint(self._buffer, 4)
        except NeedMoreData:
            return None
        raw = bytes(self._buffer[:end])
        del self._buffer[:end]
        self._preface_done = True
        if version != VERSION:
            raise DecodeError(f"unsupported MPX version {version}")
        return ConnectionPreface(version=version, raw=raw)

    def pop_handshake(self) -> HandshakeMessage | None:
        if not self._preface_done:
            raise DecodeError("parse Connection Preface first")
        if self._record_mode:
            raise DecodeError("parser is in Secure Record mode")
        if not self._buffer:
            return None
        try:
            message, end = decode_handshake_message(bytes(self._buffer), 0)
        except NeedMoreData:
            return None
        del self._buffer[:end]
        return message

    def switch_to_records(self) -> None:
        if not self._preface_done:
            raise DecodeError("parse Connection Preface first")
        self._record_mode = True

    def pop_record(self, *, max_record_size: int = 65536) -> WireRecord | None:
        if not self._record_mode:
            raise DecodeError("parser is not in Secure Record mode")
        if not self._buffer:
            return None
        try:
            record, end = decode_wire_record(
                bytes(self._buffer),
                0,
                max_record_size=max_record_size,
            )
        except NeedMoreData:
            return None
        del self._buffer[:end]
        return record


def recv_exact(sock: socket.socket, length: int) -> bytes:
    out = bytearray()
    while len(out) < length:
        chunk = sock.recv(length - len(out))
        if not chunk:
            raise EOFError("TCP EOF before complete MPX unit")
        out += chunk
    return bytes(out)


def recv_varint_raw(sock: socket.socket) -> tuple[int, bytes]:
    first = recv_exact(sock, 1)
    width = width_from_first_octet(first[0])
    rest = recv_exact(sock, width - 1) if width > 1 else b""
    raw = first + rest
    value, end = decode_varint(raw)
    if end != len(raw):
        raise DecodeError("invalid VarInt framing")
    return value, raw


def recv_preface(sock: socket.socket) -> ConnectionPreface:
    magic = recv_exact(sock, 4)
    if magic != MAGIC:
        raise DecodeError("invalid MPX magic")
    version, version_raw = recv_varint_raw(sock)
    if version != VERSION:
        raise DecodeError(f"unsupported MPX version {version}")
    return ConnectionPreface(version=version, raw=magic + version_raw)


def recv_handshake(sock: socket.socket) -> HandshakeMessage:
    mtype, type_raw = recv_varint_raw(sock)
    length, length_raw = recv_varint_raw(sock)
    if length > 4096:
        raise DecodeError("handshake message exceeds Draft 03 limit")
    body = recv_exact(sock, length)
    return HandshakeMessage(
        type=mtype,
        body=body,
        raw=type_raw + length_raw + body,
    )


def recv_wire_record(sock: socket.socket, *, max_record_size: int = 65536) -> WireRecord:
    flags = recv_exact(sock, 1)
    if flags != b"\x00":
        raise DecodeError("non-zero Draft 03 Record Flags")
    length, length_raw = recv_varint_raw(sock)
    if length < 1 or length > max_record_size:
        raise DecodeError("invalid Secure Record ciphertext length")
    ciphertext = recv_exact(sock, length)
    tag = recv_exact(sock, 16)
    return WireRecord(
        flags=0,
        ciphertext=ciphertext,
        tag=tag,
        raw=flags + length_raw + ciphertext + tag,
    )
