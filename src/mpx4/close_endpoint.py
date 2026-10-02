from __future__ import annotations

from dataclasses import dataclass
import os
import socket

from .carrier import (
    SecureCarrier,
    client_create_carrier,
    client_join_carrier,
    server_create_carrier,
    server_join_carrier,
)
from .close import SessionCloseController
from .codec import decode_token_frame, encode_ping, encode_pong
from .constants import FrameType
from .errors import CarrierLostError, DecodeError, SessionConflictError


CLOSE_MODES = {"carrier", "session", "bare-eof", "half-close"}


@dataclass(frozen=True)
class CloseScenarioResult:
    session_id: bytes
    mode: str
    session_lifecycle: str
    carrier1_active: bool
    carrier2_active: bool
    carrier2_graceful: bool
    carrier2_close_kind: str | None
    surviving_ping: bool
    transport_loss_detected: bool
    new_stream_blocked: bool
    new_join_blocked: bool
    close_records_sent: int


def _expect_one(frames, frame_type: FrameType):
    if len(frames) != 1 or frames[0].type != int(frame_type):
        raise DecodeError(f"expected one {frame_type.name}")
    return frames[0]


def _serve_ping(carrier: SecureCarrier) -> None:
    frame = _expect_one(carrier.recv(), FrameType.PING)
    carrier.send(encode_pong(decode_token_frame(frame)))


def _client_ping(carrier: SecureCarrier, token: int = 4242) -> bool:
    carrier.send(encode_ping(token))
    frame = _expect_one(carrier.recv(), FrameType.PONG)
    return decode_token_frame(frame) == token


def _blocked_checks(session, controller: SessionCloseController) -> tuple[bool, bool]:
    try:
        controller.ensure_new_stream_allowed()
    except SessionConflictError:
        stream_blocked = True
    else:
        stream_blocked = False

    try:
        session.validate_local_join(3, 0)
    except SessionConflictError:
        join_blocked = True
    else:
        join_blocked = False
    return stream_blocked, join_blocked


def _server_with_carriers(
    carrier1: SecureCarrier,
    carrier2: SecureCarrier,
    session,
    join_sock: socket.socket,
    *,
    mode: str,
) -> CloseScenarioResult:
    controller = SessionCloseController(session)
    controller.add_carrier(carrier1)
    controller.add_carrier(carrier2)

    surviving_ping = False
    transport_loss_detected = False
    close_records_sent = 0

    if mode == "carrier":
        controller.send_carrier_close(
            carrier2,
            reason="graceful path retirement",
        )
        close_records_sent = 1
        _serve_ping(carrier1)
        surviving_ping = True
    elif mode == "session":
        _, close_records_sent = controller.send_session_close(
            reason="graceful session shutdown",
        )
    elif mode == "bare-eof":
        # No authenticated MPX close Frame is sent.
        join_sock.shutdown(socket.SHUT_RDWR)
        carrier2.mark_lost()
        transport_loss_detected = True
        _serve_ping(carrier1)
        surviving_ping = True
    elif mode == "half-close":
        # A TCP half-close is transport state, not an MPX terminal Frame.
        join_sock.shutdown(socket.SHUT_WR)
        carrier2.mark_lost()
        transport_loss_detected = True
        _serve_ping(carrier1)
        surviving_ping = True
    else:
        raise ValueError("invalid close mode")

    if mode == "session":
        stream_blocked = session.lifecycle != "active"
        join_blocked = session.lifecycle != "active"
    else:
        stream_blocked = False
        join_blocked = False

    return CloseScenarioResult(
        session_id=session.session_id,
        mode=mode,
        session_lifecycle=session.lifecycle,
        carrier1_active=carrier1.active,
        carrier2_active=carrier2.active,
        carrier2_graceful=carrier2.gracefully_closed,
        carrier2_close_kind=carrier2.close_kind,
        surviving_ping=surviving_ping,
        transport_loss_detected=transport_loss_detected,
        new_stream_blocked=stream_blocked,
        new_join_blocked=join_blocked,
        close_records_sent=close_records_sent,
    )


def _client_with_carriers(
    carrier1: SecureCarrier,
    carrier2: SecureCarrier,
    session,
    *,
    mode: str,
) -> CloseScenarioResult:
    controller = SessionCloseController(session)
    controller.add_carrier(carrier1)
    controller.add_carrier(carrier2)

    surviving_ping = False
    transport_loss_detected = False
    close_records_sent = 0

    if mode == "carrier":
        frame = _expect_one(carrier2.recv(), FrameType.CARRIER_CLOSE)
        controller.receive_carrier_close(carrier2, frame)
        surviving_ping = _client_ping(carrier1)
    elif mode == "session":
        frame = _expect_one(carrier1.recv(), FrameType.SESSION_CLOSE)
        controller.receive_session_close(carrier1, frame)
    elif mode in {"bare-eof", "half-close"}:
        try:
            carrier2.recv()
        except CarrierLostError:
            transport_loss_detected = True
        else:
            raise DecodeError("raw TCP termination looked like graceful MPX close")
        if carrier2.gracefully_closed:
            raise DecodeError("transport loss was misclassified as graceful close")
        surviving_ping = _client_ping(carrier1)
    else:
        raise ValueError("invalid close mode")

    if mode == "session":
        stream_blocked, join_blocked = _blocked_checks(session, controller)
    else:
        stream_blocked = False
        join_blocked = False

    return CloseScenarioResult(
        session_id=session.session_id,
        mode=mode,
        session_lifecycle=session.lifecycle,
        carrier1_active=carrier1.active,
        carrier2_active=carrier2.active,
        carrier2_graceful=carrier2.gracefully_closed,
        carrier2_close_kind=carrier2.close_kind,
        surviving_ping=surviving_ping,
        transport_loss_detected=transport_loss_detected,
        new_stream_blocked=stream_blocked,
        new_join_blocked=join_blocked,
        close_records_sent=close_records_sent,
    )


def run_close_server(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    mode: str,
) -> CloseScenarioResult:
    if mode not in CLOSE_MODES:
        raise ValueError("invalid close mode")

    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, port))
        listener.listen(2)

        create_sock, _ = listener.accept()
        try:
            carrier1, session = server_create_carrier(
                create_sock,
                transport_key,
                server_nonce=os.urandom(32),
            )
            join_sock, _ = listener.accept()
            try:
                carrier2 = server_join_carrier(
                    join_sock,
                    transport_key,
                    session,
                    server_nonce=os.urandom(32),
                )
                return _server_with_carriers(
                    carrier1,
                    carrier2,
                    session,
                    join_sock,
                    mode=mode,
                )
            finally:
                join_sock.close()
        finally:
            create_sock.close()


def run_close_client(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    mode: str,
) -> CloseScenarioResult:
    if mode not in CLOSE_MODES:
        raise ValueError("invalid close mode")

    create_sock = socket.create_connection((host, port), timeout=10)
    try:
        carrier1, session = client_create_carrier(
            create_sock,
            transport_key,
            session_id=os.urandom(16),
            client_nonce=os.urandom(32),
            carrier_id=1,
            generation=0,
        )
        join_sock = socket.create_connection((host, port), timeout=10)
        try:
            carrier2 = client_join_carrier(
                join_sock,
                transport_key,
                session,
                carrier_id=2,
                generation=0,
                client_nonce=os.urandom(32),
            )
            return _client_with_carriers(
                carrier1,
                carrier2,
                session,
                mode=mode,
            )
        finally:
            join_sock.close()
    finally:
        create_sock.close()
