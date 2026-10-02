from __future__ import annotations

import json
from pathlib import Path

from .codec import (
    decode_frame,
    encode_credit_probe,
    encode_frame,
    encode_session_credit,
    encode_stream_credit,
    encode_stream_data,
    encode_transmission_ack,
)
from .constants import FrameType, MAGIC, VERSION
from .crypto import RecordCipher, decode_wire_record, derive_key_schedule
from .errors import DecodeError, NeedMoreData
from .tcp import IncrementalBindingParser
from .varint import decode_varint, decode_varint_exact, encode_varint


def _load(spec_dir: Path, relative: str):
    return json.loads((spec_dir / relative).read_text(encoding="utf-8"))


def verify_varints(spec_dir: Path) -> None:
    doc = _load(spec_dir, "test-vectors/varint.json")
    for vector in doc["vectors"]:
        value = int(vector["value"])
        wire = bytes.fromhex(vector["hex"])
        assert encode_varint(value) == wire
        assert decode_varint_exact(wire) == value

    for vector in doc["invalid"]:
        wire = bytes.fromhex(vector["hex"])
        try:
            decode_varint_exact(wire)
        except (DecodeError, NeedMoreData):
            pass
        else:
            raise AssertionError(f"invalid VarInt accepted: {vector['hex']}")


def _frame_from_vector(vector: dict) -> bytes:
    fields = vector["fields"]
    ftype = vector["frame_type"]
    if ftype == "STREAM_DATA":
        return encode_stream_data(
            int(fields["stream_id"]),
            int(fields["offset"]),
            int(fields["transmission_id"]),
            fields["data_utf8"].encode("utf-8"),
        )
    if ftype == "TRANSMISSION_ACK":
        return encode_transmission_ack(
            int(fields["stream_id"]),
            int(fields["transmission_id"]),
            int(fields["receiver_timestamp_us"]),
        )
    if ftype == "STREAM_CREDIT":
        return encode_stream_credit(
            int(fields["stream_id"]),
            int(fields["consumed_offset"]),
            int(fields["maximum_offset"]),
        )
    if ftype == "SESSION_CREDIT":
        return encode_session_credit(
            int(fields["consumed_bytes"]),
            int(fields["maximum_bytes"]),
        )
    if ftype == "CREDIT_PROBE":
        return encode_credit_probe(int(fields["stream_id"]))
    raise AssertionError(f"unsupported Frame vector {ftype}")


def verify_frames(spec_dir: Path) -> None:
    doc = _load(spec_dir, "test-vectors/frame-encoding.json")
    for vector in doc["vectors"]:
        wire = bytes.fromhex(vector["hex"])
        assert len(wire) == vector["length"]
        frame, end = decode_frame(wire)
        assert end == len(wire)
        assert encode_frame(frame.type, frame.body) == wire
        assert _frame_from_vector(vector) == wire


def verify_key_schedule(spec_dir: Path) -> None:
    doc = _load(spec_dir, "test-vectors/key-schedule.json")
    inputs = doc["inputs"]
    expected = doc["derived"]
    schedule = derive_key_schedule(
        bytes.fromhex(inputs["transport_key_hex"]),
        bytes.fromhex(inputs["connection_preface_hex"]),
        bytes.fromhex(inputs["client_init_hex"]),
        bytes.fromhex(inputs["server_init_hex"]),
    )
    fields = {
        "h0_hex": schedule.h0,
        "early_secret_hex": schedule.early_secret,
        "handshake_secret_hex": schedule.handshake_secret,
        "client_finished_key_hex": schedule.client_finished_key,
        "server_finished_key_hex": schedule.server_finished_key,
        "client_verify_data_hex": schedule.client_verify_data,
        "client_finished_hex": schedule.client_finished,
        "h1_hex": schedule.h1,
        "server_verify_data_hex": schedule.server_verify_data,
        "server_finished_hex": schedule.server_finished,
        "h2_hex": schedule.h2,
        "client_application_secret_hex": schedule.client_application_secret,
        "server_application_secret_hex": schedule.server_application_secret,
        "client_traffic_key_hex": schedule.client_traffic_key,
        "client_traffic_iv_hex": schedule.client_traffic_iv,
        "server_traffic_key_hex": schedule.server_traffic_key,
        "server_traffic_iv_hex": schedule.server_traffic_iv,
    }
    for name, value in fields.items():
        assert value.hex() == expected[name], f"{name} mismatch"


def verify_secure_records(spec_dir: Path) -> None:
    doc = _load(spec_dir, "test-vectors/secure-record.json")

    if "records" in doc:
        key = bytes.fromhex(doc["traffic_key_hex"])
        iv = bytes.fromhex(doc["traffic_iv_hex"])
        records = doc["records"]
    else:
        key = bytes.fromhex(doc["inputs"]["traffic_key_hex"])
        iv = bytes.fromhex(doc["inputs"]["traffic_iv_hex"])
        records = [{
            "sequence_number": doc["sequence_number"],
            "plaintext_hex": doc["inputs"]["plaintext_hex"],
            **doc["derived"],
        }]

    sender = RecordCipher(key, iv)
    receiver = RecordCipher(key, iv)
    for vector in records:
        sequence = int(vector["sequence_number"])
        assert sender.sequence_number == sequence
        assert receiver.sequence_number == sequence
        plaintext = bytes.fromhex(vector["plaintext_hex"])
        expected_wire = bytes.fromhex(vector["wire_record_hex"])
        actual_wire = sender.seal(plaintext)
        assert actual_wire == expected_wire

        record, end = decode_wire_record(expected_wire)
        assert end == len(expected_wire)
        assert receiver.open(record) == plaintext


def _collect_preface_handshake(chunks: list[bytes]) -> list[tuple[str, bytes]]:
    parser = IncrementalBindingParser()
    units: list[tuple[str, bytes]] = []
    prefaced = False
    for chunk in chunks:
        parser.feed(chunk)
        if not prefaced:
            preface = parser.pop_preface()
            if preface is not None:
                units.append(("connection_preface", preface.raw))
                prefaced = True
        if prefaced:
            while True:
                message = parser.pop_handshake()
                if message is None:
                    break
                units.append(("handshake_message", message.raw))
    return units


def _collect_records(chunks: list[bytes]) -> tuple[list[tuple[str, bytes]], int]:
    parser = IncrementalBindingParser()
    parser.feed(MAGIC + encode_varint(VERSION))
    assert parser.pop_preface() is not None
    parser.switch_to_records()
    units: list[tuple[str, bytes]] = []
    for chunk in chunks:
        parser.feed(chunk)
        while True:
            record = parser.pop_record()
            if record is None:
                break
            units.append(("secure_record", record.raw))
    return units, parser.pending_bytes


def verify_tcp_binding(spec_dir: Path) -> None:
    doc = _load(spec_dir, "test-vectors/tcp-binding.json")
    for case in doc["cases"]:
        chunks = [bytes.fromhex(value) for value in case["input_chunks_hex"]]
        expected = [
            (unit["type"], bytes.fromhex(unit["hex"]))
            for unit in case.get("expected_units", [])
        ]

        if expected and expected[0][0] in ("connection_preface", "handshake_message"):
            actual = _collect_preface_handshake(chunks)
            pending = 0
        else:
            actual, pending = _collect_records(chunks)

        assert actual == expected, f"TCP binding case failed: {case['name']}"
        if case.get("transport_event") == "EOF":
            assert pending > 0
            assert case.get("expected_sequence_advance") is False


def verify_all(spec_dir: str | Path) -> list[str]:
    root = Path(spec_dir)
    checks = [
        ("varint", verify_varints),
        ("frame-encoding", verify_frames),
        ("key-schedule", verify_key_schedule),
        ("secure-record", verify_secure_records),
        ("tcp-binding", verify_tcp_binding),
    ]
    passed: list[str] = []
    for name, check in checks:
        check(root)
        passed.append(name)
    return passed
