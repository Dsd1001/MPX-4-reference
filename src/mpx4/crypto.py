from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .codec import encode_handshake_message
from .constants import (
    AEAD_TAG_LEN,
    HandshakeType,
    MAX_RECORDS_PER_KEY,
)
from .errors import AuthenticationError, DecodeError
from .varint import decode_varint, encode_varint


HASH_LEN = 32


def hkdf_extract(salt: bytes, ikm: bytes) -> bytes:
    return hmac.new(salt, ikm, hashlib.sha256).digest()


def hkdf_expand(prk: bytes, info: bytes, length: int) -> bytes:
    if length < 0 or length > 255 * HASH_LEN:
        raise ValueError("HKDF output length out of range")
    result = bytearray()
    previous = b""
    counter = 1
    while len(result) < length:
        previous = hmac.new(
            prk,
            previous + info + bytes([counter]),
            hashlib.sha256,
        ).digest()
        result += previous
        counter += 1
    return bytes(result[:length])


def mpx_expand_label(secret: bytes, label: str, context: bytes, length: int) -> bytes:
    full_label = ("mpx4 " + label).encode("ascii")
    if len(full_label) > 255:
        raise ValueError("MPX label too long")
    if len(context) > 255:
        raise ValueError("MPX context too long")
    info = (
        length.to_bytes(2, "big")
        + bytes([len(full_label)])
        + full_label
        + bytes([len(context)])
        + context
    )
    return hkdf_expand(secret, info, length)


@dataclass(frozen=True)
class KeySchedule:
    h0: bytes
    early_secret: bytes
    handshake_secret: bytes
    client_finished_key: bytes
    server_finished_key: bytes
    client_verify_data: bytes
    client_finished: bytes
    h1: bytes
    server_verify_data: bytes
    server_finished: bytes
    h2: bytes
    client_application_secret: bytes
    server_application_secret: bytes
    client_traffic_key: bytes
    client_traffic_iv: bytes
    server_traffic_key: bytes
    server_traffic_iv: bytes


def derive_key_schedule(
    transport_key: bytes,
    connection_preface: bytes,
    client_init: bytes,
    server_init: bytes,
) -> KeySchedule:
    if len(transport_key) != 32:
        raise ValueError("transport key must be exactly 32 octets")

    h0 = hashlib.sha256(connection_preface + client_init + server_init).digest()
    early_secret = hkdf_extract(b"\x00" * 32, transport_key)
    handshake_secret = mpx_expand_label(
        early_secret,
        "handshake",
        h0,
        32,
    )
    client_finished_key = mpx_expand_label(
        handshake_secret,
        "client finished",
        b"",
        32,
    )
    server_finished_key = mpx_expand_label(
        handshake_secret,
        "server finished",
        b"",
        32,
    )

    client_verify_data = hmac.new(
        client_finished_key,
        h0,
        hashlib.sha256,
    ).digest()
    client_finished = encode_handshake_message(
        HandshakeType.CLIENT_FINISHED,
        client_verify_data,
    )
    h1 = hashlib.sha256(
        connection_preface + client_init + server_init + client_finished
    ).digest()

    server_verify_data = hmac.new(
        server_finished_key,
        h1,
        hashlib.sha256,
    ).digest()
    server_finished = encode_handshake_message(
        HandshakeType.SERVER_FINISHED,
        server_verify_data,
    )
    h2 = hashlib.sha256(
        connection_preface
        + client_init
        + server_init
        + client_finished
        + server_finished
    ).digest()

    client_application_secret = mpx_expand_label(
        handshake_secret,
        "client application",
        h2,
        32,
    )
    server_application_secret = mpx_expand_label(
        handshake_secret,
        "server application",
        h2,
        32,
    )
    client_traffic_key = mpx_expand_label(
        client_application_secret,
        "key",
        b"",
        32,
    )
    client_traffic_iv = mpx_expand_label(
        client_application_secret,
        "iv",
        b"",
        12,
    )
    server_traffic_key = mpx_expand_label(
        server_application_secret,
        "key",
        b"",
        32,
    )
    server_traffic_iv = mpx_expand_label(
        server_application_secret,
        "iv",
        b"",
        12,
    )

    return KeySchedule(
        h0=h0,
        early_secret=early_secret,
        handshake_secret=handshake_secret,
        client_finished_key=client_finished_key,
        server_finished_key=server_finished_key,
        client_verify_data=client_verify_data,
        client_finished=client_finished,
        h1=h1,
        server_verify_data=server_verify_data,
        server_finished=server_finished,
        h2=h2,
        client_application_secret=client_application_secret,
        server_application_secret=server_application_secret,
        client_traffic_key=client_traffic_key,
        client_traffic_iv=client_traffic_iv,
        server_traffic_key=server_traffic_key,
        server_traffic_iv=server_traffic_iv,
    )


def _nonce(iv: bytes, sequence_number: int) -> bytes:
    if len(iv) != 12:
        raise ValueError("traffic IV must be 12 octets")
    if sequence_number < 0 or sequence_number >= (1 << 64):
        raise ValueError("record sequence number out of range")
    seq96 = b"\x00" * 4 + sequence_number.to_bytes(8, "big")
    return bytes(a ^ b for a, b in zip(iv, seq96))


@dataclass(frozen=True)
class WireRecord:
    flags: int
    ciphertext: bytes
    tag: bytes
    raw: bytes

    @property
    def ciphertext_length(self) -> int:
        return len(self.ciphertext)


def encode_wire_record(
    key: bytes,
    iv: bytes,
    sequence_number: int,
    plaintext: bytes,
    *,
    flags: int = 0,
) -> bytes:
    if flags != 0:
        raise ValueError("Draft 03 Record Flags must be zero")
    if not plaintext:
        raise ValueError("empty Secure Record plaintext is forbidden")
    header = bytes([flags]) + encode_varint(len(plaintext))
    sealed = AESGCM(key).encrypt(
        _nonce(iv, sequence_number),
        plaintext,
        header,
    )
    return header + sealed


def decode_wire_record(data: bytes, offset: int = 0, *, max_record_size: int = 65536) -> tuple[WireRecord, int]:
    start = offset
    if offset >= len(data):
        raise DecodeError("missing Record Flags")
    flags = data[offset]
    offset += 1
    if flags != 0:
        raise DecodeError("non-zero Draft 03 Record Flags")

    length, offset = decode_varint(data, offset)
    if length < 1 or length > max_record_size:
        raise DecodeError("invalid Secure Record ciphertext length")

    end_ciphertext = offset + length
    end = end_ciphertext + AEAD_TAG_LEN
    if end > len(data):
        from .errors import NeedMoreData

        raise NeedMoreData("truncated Secure Record")

    return (
        WireRecord(
            flags=flags,
            ciphertext=bytes(data[offset:end_ciphertext]),
            tag=bytes(data[end_ciphertext:end]),
            raw=bytes(data[start:end]),
        ),
        end,
    )


class RecordCipher:
    def __init__(self, key: bytes, iv: bytes, *, sequence_number: int = 0):
        if len(key) != 32:
            raise ValueError("AES-256-GCM key must be 32 octets")
        if len(iv) != 12:
            raise ValueError("traffic IV must be 12 octets")
        if sequence_number < 0:
            raise ValueError("negative sequence number")
        self.key = bytes(key)
        self.iv = bytes(iv)
        self.sequence_number = sequence_number
        self._aead = AESGCM(self.key)

    def _check_limit(self) -> None:
        if self.sequence_number >= MAX_RECORDS_PER_KEY:
            raise ValueError("Draft 03 Secure Record per-key limit reached")

    def seal(self, plaintext: bytes, *, flags: int = 0) -> bytes:
        self._check_limit()
        if flags != 0:
            raise ValueError("Draft 03 Record Flags must be zero")
        if not plaintext:
            raise ValueError("empty Secure Record plaintext is forbidden")

        header = bytes([flags]) + encode_varint(len(plaintext))
        sealed = self._aead.encrypt(
            _nonce(self.iv, self.sequence_number),
            plaintext,
            header,
        )
        wire = header + sealed
        self.sequence_number += 1
        return wire

    def open(self, record: WireRecord) -> bytes:
        self._check_limit()
        header = bytes([record.flags]) + encode_varint(len(record.ciphertext))
        try:
            plaintext = self._aead.decrypt(
                _nonce(self.iv, self.sequence_number),
                record.ciphertext + record.tag,
                header,
            )
        except Exception as exc:
            raise AuthenticationError("Secure Record authentication failed") from exc
        self.sequence_number += 1
        return plaintext
