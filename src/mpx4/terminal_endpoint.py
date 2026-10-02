from __future__ import annotations

from dataclasses import dataclass
import os
import socket

from .carrier import client_create_carrier, server_create_carrier
from .codec import (
    TransmissionAck,
    decode_reset_stream,
    decode_stop_sending,
    decode_stream_data,
    decode_stream_open,
    decode_stream_open_reject,
    decode_token_frame,
    decode_transmission_ack,
    encode_ping,
    encode_pong,
    encode_reset_stream,
    encode_stop_sending,
    encode_stream_data,
    encode_stream_open,
    encode_transmission_ack,
)
from .constants import ErrorCode, FrameType, SchedulerID
from .errors import DecodeError
from .terminal import ServerTerminalStateMachine


@dataclass(frozen=True)
class TerminalScenarioResult:
    session_id: bytes
    mode: str
    stream_id: int
    cancellation_acked: bool
    reset_sent: bool
    open_rejected: bool
    application_created: bool
    tombstone_recorded: bool
    retired_identity: bool
    stale_data_ignored: bool
    session_committed_bytes: int


def _require_ack(frame, stream_id: int, transmission_id: int) -> None:
    ack = decode_transmission_ack(frame)
    if (
        ack.stream_id != stream_id
        or ack.transmission_id != transmission_id
    ):
        raise DecodeError("unexpected TRANSMISSION_ACK")


def client_terminal_scenario(
    sock: socket.socket,
    transport_key: bytes,
    *,
    mode: str,
    session_id: bytes | None = None,
    error_code: int = 77,
) -> TerminalScenarioResult:
    if mode not in {"reset", "stop"}:
        raise ValueError("mode must be reset or stop")

    carrier, session = client_create_carrier(
        sock,
        transport_key,
        session_id=session_id or os.urandom(16),
        client_nonce=os.urandom(32),
        scheduler=int(SchedulerID.AGGREGATE),
    )

    stream_id = 1
    cancellation_txid = 1
    open_txid = 2
    stale_data_txid = 3
    reset_sent = False

    if mode == "reset":
        carrier.send(
            encode_reset_stream(
                stream_id,
                cancellation_txid,
                0,
                error_code,
            )
        )
        frames = carrier.recv()
        if len(frames) != 1:
            raise DecodeError("pre-open RESET expected one ACK")
        _require_ack(frames[0], stream_id, cancellation_txid)
        cancellation_acked = True
    else:
        carrier.send(
            encode_stop_sending(
                stream_id,
                cancellation_txid,
                error_code,
            )
        )
        frames = carrier.recv()
        if len(frames) != 2:
            raise DecodeError("pre-open STOP expected ACK plus RESET_STREAM")

        ack_frames = [
            frame
            for frame in frames
            if frame.type == int(FrameType.TRANSMISSION_ACK)
        ]
        reset_frames = [
            frame
            for frame in frames
            if frame.type == int(FrameType.RESET_STREAM)
        ]
        if len(ack_frames) != 1 or len(reset_frames) != 1:
            raise DecodeError("pre-open STOP response Frame set is invalid")
        _require_ack(ack_frames[0], stream_id, cancellation_txid)

        reset = decode_reset_stream(reset_frames[0])
        if (
            reset.stream_id != stream_id
            or reset.final_offset != 0
            or reset.error_code != error_code
        ):
            raise DecodeError("pre-open STOP response RESET_STREAM mismatch")
        carrier.send(
            encode_transmission_ack(
                stream_id,
                reset.transmission_id,
                0,
            )
        )
        cancellation_acked = True
        reset_sent = True

    carrier.send(encode_stream_open(stream_id, open_txid))
    frames = carrier.recv()
    if len(frames) != 1 or frames[0].type != int(FrameType.STREAM_OPEN_REJECT):
        raise DecodeError("late STREAM_OPEN was not rejected")
    reject = decode_stream_open_reject(frames[0])
    if (
        reject.stream_id != stream_id
        or reject.transmission_id != open_txid
        or reject.error_code != int(ErrorCode.STREAM_STATE_ERROR)
    ):
        raise DecodeError("late STREAM_OPEN rejection mismatch")
    open_rejected = True

    # Repeat the cancellation. The Server processes it idempotently and
    # compacts the lightweight tombstone before sending the response.
    if mode == "reset":
        carrier.send(
            encode_reset_stream(
                stream_id,
                cancellation_txid,
                0,
                error_code,
            )
        )
        frames = carrier.recv()
        if len(frames) != 1:
            raise DecodeError("duplicate RESET expected one ACK")
        _require_ack(frames[0], stream_id, cancellation_txid)
    else:
        carrier.send(
            encode_stop_sending(
                stream_id,
                cancellation_txid,
                error_code,
            )
        )
        frames = carrier.recv()
        if len(frames) != 1:
            raise DecodeError("settled duplicate STOP expected one ACK")
        _require_ack(frames[0], stream_id, cancellation_txid)

    # This stale DATA is intentionally invalid as new application traffic but
    # is safe to ignore after tombstone compaction. A following PING proves
    # that the Carrier remains usable and the stale Stream did not revive.
    carrier.send(
        encode_stream_data(
            stream_id,
            0,
            stale_data_txid,
            b"stale",
        )
    )
    carrier.send(encode_ping(9001))
    frames = carrier.recv()
    if (
        len(frames) != 1
        or frames[0].type != int(FrameType.PONG)
        or decode_token_frame(frames[0]) != 9001
    ):
        raise DecodeError("terminal scenario completion PONG mismatch")

    return TerminalScenarioResult(
        session_id=session.session_id,
        mode=mode,
        stream_id=stream_id,
        cancellation_acked=cancellation_acked,
        reset_sent=reset_sent,
        open_rejected=open_rejected,
        application_created=False,
        tombstone_recorded=True,
        retired_identity=True,
        stale_data_ignored=True,
        session_committed_bytes=0,
    )


def server_terminal_scenario(
    sock: socket.socket,
    transport_key: bytes,
    *,
    expected_mode: str,
    error_code: int = 77,
) -> TerminalScenarioResult:
    if expected_mode not in {"reset", "stop"}:
        raise ValueError("expected_mode must be reset or stop")

    carrier, session = server_create_carrier(
        sock,
        transport_key,
        server_nonce=os.urandom(32),
    )
    machine = ServerTerminalStateMachine(
        max_streams=session.server_limits.max_streams
    )

    stream_id = 1
    frames = carrier.recv()
    if len(frames) != 1:
        raise DecodeError("expected one pre-open cancellation Frame")

    if expected_mode == "reset":
        if frames[0].type != int(FrameType.RESET_STREAM):
            raise DecodeError("expected pre-open RESET_STREAM")
        cancellation = decode_reset_stream(frames[0])
        if cancellation.error_code != error_code:
            raise DecodeError("pre-open RESET error mismatch")
        result = machine.handle_reset(cancellation)
        reset_sent = False
    else:
        if frames[0].type != int(FrameType.STOP_SENDING):
            raise DecodeError("expected pre-open STOP_SENDING")
        cancellation = decode_stop_sending(frames[0])
        if cancellation.error_code != error_code:
            raise DecodeError("pre-open STOP error mismatch")
        result = machine.handle_stop(cancellation)
        reset_sent = len(result.responses) == 2

    carrier.send(*result.responses)
    cancellation_acked = True

    if expected_mode == "stop":
        frames = carrier.recv()
        if len(frames) != 1 or frames[0].type != int(FrameType.TRANSMISSION_ACK):
            raise DecodeError("expected ACK for generated RESET_STREAM")
        machine.handle_ack(decode_transmission_ack(frames[0]))

    frames = carrier.recv()
    if len(frames) != 1 or frames[0].type != int(FrameType.STREAM_OPEN):
        raise DecodeError("expected late STREAM_OPEN")
    open_stream_id, open_txid = decode_stream_open(frames[0])
    open_result = machine.handle_open(open_stream_id, open_txid)
    if open_result.application_created:
        raise DecodeError("pre-open cancellation created application Stream")
    carrier.send(*open_result.responses)

    tombstone_recorded = stream_id in machine.tombstones

    frames = carrier.recv()
    if len(frames) != 1:
        raise DecodeError("expected duplicate cancellation")
    if expected_mode == "reset":
        duplicate_result = machine.handle_reset(
            decode_reset_stream(frames[0])
        )
    else:
        duplicate_result = machine.handle_stop(
            decode_stop_sending(frames[0])
        )

    # STOP's generated RESET has already been acknowledged, so its duplicate
    # response is now only the STOP ACK.
    if expected_mode == "stop" and len(duplicate_result.responses) != 1:
        raise DecodeError("settled STOP duplicate repeated RESET unexpectedly")

    machine.compact(stream_id)
    carrier.send(*duplicate_result.responses)

    frames = carrier.recv()
    if len(frames) != 1 or frames[0].type != int(FrameType.STREAM_DATA):
        raise DecodeError("expected stale retired STREAM_DATA")
    stale = decode_stream_data(frames[0])
    stale_result = machine.handle_data(stale)
    if stale_result.delivered or stale_result.responses:
        raise DecodeError("retired Stream DATA had an observable effect")

    frames = carrier.recv()
    if len(frames) != 1 or frames[0].type != int(FrameType.PING):
        raise DecodeError("expected terminal scenario PING")
    token = decode_token_frame(frames[0])
    carrier.send(encode_pong(token))

    return TerminalScenarioResult(
        session_id=session.session_id,
        mode=expected_mode,
        stream_id=stream_id,
        cancellation_acked=cancellation_acked,
        reset_sent=reset_sent,
        open_rejected=True,
        application_created=stream_id in machine.active,
        tombstone_recorded=tombstone_recorded,
        retired_identity=stream_id in machine.retired,
        stale_data_ignored=True,
        session_committed_bytes=machine.receive_flow.session_committed,
    )


def run_terminal_server(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    mode: str,
) -> TerminalScenarioResult:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, port))
        listener.listen(1)
        conn, _ = listener.accept()
        with conn:
            return server_terminal_scenario(
                conn,
                transport_key,
                expected_mode=mode,
            )


def run_terminal_client(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    mode: str,
) -> TerminalScenarioResult:
    with socket.create_connection((host, port), timeout=10) as sock:
        return client_terminal_scenario(
            sock,
            transport_key,
            mode=mode,
        )
