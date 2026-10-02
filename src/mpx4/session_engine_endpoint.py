from __future__ import annotations

from dataclasses import dataclass
import os
import select
import socket
from typing import Iterable

from .carrier import (
    SecureCarrier,
    client_create_carrier,
    client_join_carrier,
    server_create_carrier,
    server_join_carrier,
)
from .codec import decode_stream_data
from .constants import ErrorCode, FrameType
from .errors import DecodeError
from .session_engine import ReferenceSessionEngine


@dataclass(frozen=True)
class UnifiedSessionResult:
    session_id: bytes
    side: str
    stream_ids: tuple[int, int]
    received_stream1: bytes
    received_stream3: bytes
    stream1_deliveries: int
    stream3_deliveries: int
    reinjected_transmission_id: int
    reinjection_attempts: tuple[tuple[int, int], ...]
    late_original_suppressed: bool
    carrier2_lost: bool
    retired_stream_ids: tuple[int, ...]
    server_tombstones: tuple[int, ...]
    session_closed: bool
    close_records_sent: int
    max_transmission_id: int


def _identity(carrier: SecureCarrier) -> tuple[int, int]:
    return (
        carrier.identity.carrier_id,
        carrier.identity.generation,
    )


def _recv_ready(
    carriers: Iterable[SecureCarrier],
    *,
    timeout: float = 5.0,
) -> SecureCarrier:
    candidates = list(carriers)
    if not candidates:
        raise DecodeError("no Carrier available for receive")
    readable, _, _ = select.select(
        [carrier.sock for carrier in candidates],
        [],
        [],
        timeout,
    )
    if not readable:
        raise TimeoutError("timed out waiting for MPX Carrier data")
    selected = readable[0]
    for carrier in candidates:
        if carrier.sock is selected:
            return carrier
    raise DecodeError("select returned unknown Carrier socket")


def _recv_handle(
    engine: ReferenceSessionEngine,
    carrier: SecureCarrier,
):
    return engine.handle_frames(carrier, carrier.recv())


def _recv_any_handle(
    engine: ReferenceSessionEngine,
    carriers: Iterable[SecureCarrier],
):
    carrier = _recv_ready(carriers)
    return carrier, _recv_handle(engine, carrier)


def _collect_delivery(
    events,
    received: dict[int, bytearray],
    counts: dict[int, int],
) -> None:
    for stream_id, payload in events.deliveries:
        received.setdefault(stream_id, bytearray()).extend(payload)
        counts[stream_id] = counts.get(stream_id, 0) + 1


def _client_with_carriers(
    carrier1: SecureCarrier,
    carrier2: SecureCarrier,
    session,
    *,
    payload1: bytes,
    payload3: bytes,
    reply1: bytes,
    reply3: bytes,
) -> UnifiedSessionResult:
    engine = ReferenceSessionEngine.client(
        session,
        retransmit_after_s=60.0,
    )
    engine.register_carrier(carrier1)
    engine.register_carrier(carrier2)

    # Server receive credit arrives first; advertise Client receive credit back.
    _recv_handle(engine, carrier1)
    engine.advertise_session_credit(carrier=carrier1)

    # Make both OPEN placements deterministic without relying on any
    # platform-dependent RTT/queue estimates.
    engine.set_carrier_role((2, 0), "disabled")
    stream1, open1 = engine.open_stream()
    engine.set_carrier_role((2, 0), "active")
    engine.set_carrier_role((1, 0), "disabled")
    stream3, open3 = engine.open_stream()
    engine.set_carrier_role((1, 0), "active")
    if (stream1, stream3) != (1, 3):
        raise DecodeError("unexpected Client Stream IDs")
    if (open1.carrier_id, open3.carrier_id) != (1, 2):
        raise DecodeError("reference scheduler did not place OPENs cross-Carrier")

    # Each OPEN produces OPEN_OK and STREAM_CREDIT as independent Records.
    while engine.openings or not {
        stream1,
        stream3,
    }.issubset(engine.send_flow.stream_credit):
        carrier, events = _recv_any_handle(
            engine,
            [carrier1, carrier2],
        )
        if events.rejected:
            raise DecodeError("unified Session Stream was rejected")

    engine.advertise_stream_credit(stream1, carrier=carrier1)
    engine.advertise_stream_credit(stream3, carrier=carrier1)

    # Force each new DATA Transmission through the scheduler's normal
    # active-role filter so Linux/macOS timing cannot change this scenario.
    engine.set_carrier_role((2, 0), "disabled")
    attempt1 = engine.send_data(stream1, payload1)
    engine.set_carrier_role((2, 0), "active")
    engine.set_carrier_role((1, 0), "disabled")
    attempt3 = engine.send_data(stream3, payload3)
    engine.set_carrier_role((1, 0), "active")
    if attempt1.carrier_id != 1 or attempt3.carrier_id != 2:
        raise DecodeError("DATA was not placed on the intended Carriers")

    tx3 = attempt3.transmission_id
    before_commit = engine.send_flow.session_committed

    # Model detection of Carrier 2 failure before the peer processes its
    # original Attempt. The TCP bytes remain readable so the Server can later
    # verify that a delayed original copy is duplicate-suppressed.
    engine.mark_carrier_lost((2, 0))
    retries = engine.poll_reliability()
    reinjection = [
        attempt
        for attempt in retries
        if attempt.transmission_id == tx3
    ]
    if len(reinjection) != 1 or reinjection[0].carrier_id != 1:
        raise DecodeError("Stream 3 was not reinjected onto Carrier 1")
    if engine.send_flow.session_committed != before_commit:
        raise DecodeError("reinjection consumed new logical Session credit")

    # Both useful ACKs return on Carrier 1.
    _recv_handle(engine, carrier1)
    _recv_handle(engine, carrier1)
    if attempt1.transmission_id in engine.ledger.pending:
        raise DecodeError("Stream 1 DATA remained unsettled")
    if tx3 in engine.ledger.pending:
        raise DecodeError("reinjected Stream 3 DATA remained unsettled")

    received: dict[int, bytearray] = {
        stream1: bytearray(),
        stream3: bytearray(),
    }
    counts = {stream1: 0, stream3: 0}

    for _ in range(2):
        events = _recv_handle(engine, carrier1)
        _collect_delivery(events, received, counts)

    if bytes(received[stream1]) != reply1:
        raise DecodeError("unexpected reverse payload on Stream 1")
    if bytes(received[stream3]) != reply3:
        raise DecodeError("unexpected reverse payload on Stream 3")

    # Graceful Stream 1 shutdown.
    fin1 = engine.send_fin(stream1)
    if fin1.carrier_id != 1:
        raise DecodeError("Stream 1 FIN did not use surviving Carrier")
    _recv_handle(engine, carrier1)  # ACK client FIN

    # Server STREAM_CONSUMED and FIN.
    _recv_handle(engine, carrier1)
    _recv_handle(engine, carrier1)
    consumed1 = engine.release_receive(stream1)
    if consumed1 is None:
        raise DecodeError("Client did not generate STREAM_CONSUMED")
    _recv_handle(engine, carrier1)  # ACK Client STREAM_CONSUMED

    if stream1 not in engine.retired_streams:
        raise DecodeError("Stream 1 did not retire after FIN/CONSUMED")

    # Stream 3 uses STOP -> Client RESET, followed by Server RESET.
    events = _recv_handle(engine, carrier1)
    if (stream3, "stop") not in events.terminal:
        raise DecodeError("Client did not process STOP_SENDING")

    _recv_handle(engine, carrier1)  # ACK Client-generated RESET
    events = _recv_handle(engine, carrier1)  # Server RESET
    if (stream3, "reset") not in events.terminal:
        raise DecodeError("Client did not process peer RESET")

    if stream3 not in engine.retired_streams:
        raise DecodeError("Stream 3 did not retire after bidirectional reset")

    close_events = _recv_handle(engine, carrier1)
    if not close_events.session_closed:
        raise DecodeError("Client did not process SESSION_CLOSE")

    # Scheduler accounting is cleared at settlement. The durable attempt
    # history is retained by the Transmission ledger only while pending, so
    # report the known identities from the scenario itself.
    reinjection_attempts = (
        (attempt3.carrier_id, attempt3.generation),
        (
            reinjection[0].carrier_id,
            reinjection[0].generation,
        ),
    )

    return UnifiedSessionResult(
        session_id=session.session_id,
        side="client",
        stream_ids=(stream1, stream3),
        received_stream1=bytes(received[stream1]),
        received_stream3=bytes(received[stream3]),
        stream1_deliveries=counts[stream1],
        stream3_deliveries=counts[stream3],
        reinjected_transmission_id=tx3,
        reinjection_attempts=reinjection_attempts,
        late_original_suppressed=True,
        carrier2_lost=not carrier2.active,
        retired_stream_ids=tuple(sorted(engine.retired_streams)),
        server_tombstones=(),
        session_closed=engine.state.lifecycle == "closed",
        close_records_sent=0,
        max_transmission_id=engine.ledger.next_id - 1,
    )


def _server_with_carriers(
    carrier1: SecureCarrier,
    carrier2: SecureCarrier,
    session,
    *,
    expected1: bytes,
    expected3: bytes,
    reply1: bytes,
    reply3: bytes,
) -> UnifiedSessionResult:
    engine = ReferenceSessionEngine.server(
        session,
        retransmit_after_s=60.0,
    )
    engine.register_carrier(carrier1)
    engine.register_carrier(carrier2)

    engine.advertise_session_credit(carrier=carrier1)
    _recv_handle(engine, carrier1)  # Client SESSION_CREDIT

    opened: set[int] = set()
    while len(opened) < 2:
        carrier, events = _recv_any_handle(
            engine,
            [carrier1, carrier2],
        )
        opened.update(events.opened)
    if opened != {1, 3}:
        raise DecodeError("Server did not accept Streams 1 and 3")

    # Client advertises reverse-direction credit for both Streams on Carrier 1.
    _recv_handle(engine, carrier1)
    _recv_handle(engine, carrier1)
    if not {1, 3}.issubset(engine.send_flow.stream_credit):
        raise DecodeError("Server did not receive both Stream credits")

    received: dict[int, bytearray] = {
        1: bytearray(),
        3: bytearray(),
    }
    counts = {1: 0, 3: 0}

    # Carrier 1 receives Stream 1 plus the reinjected Stream 3 copy. Decode
    # the authenticated Frames before dispatch so the scenario can prove the
    # delayed original keeps exactly the same Transmission identity/content.
    first_frames = carrier1.recv()
    first = engine.handle_frames(carrier1, first_frames)
    _collect_delivery(first, received, counts)
    second_frames = carrier1.recv()
    second = engine.handle_frames(carrier1, second_frames)
    _collect_delivery(second, received, counts)

    data_frames = [
        frame
        for frame in first_frames + second_frames
        if frame.type == int(FrameType.STREAM_DATA)
    ]
    reinjected = [
        decode_stream_data(frame)
        for frame in data_frames
        if decode_stream_data(frame).stream_id == 3
    ]
    if len(reinjected) != 1:
        raise DecodeError("missing Stream 3 reinjected DATA on Carrier 1")
    reinjected_data = reinjected[0]

    if bytes(received[1]) != expected1:
        raise DecodeError("Server Stream 1 payload mismatch")
    if bytes(received[3]) != expected3:
        raise DecodeError("Server Stream 3 reinjection payload mismatch")
    if counts != {1: 1, 3: 1}:
        raise DecodeError("useful DATA delivery count mismatch")

    committed_before_late = engine.receive_flow.session_committed

    # The original Stream 3 Attempt is deliberately processed after the
    # reinjection. It must ACK idempotently but create no application delivery
    # and no additional logical credit commitment.
    late_frames = carrier2.recv()
    late_data_frames = [
        decode_stream_data(frame)
        for frame in late_frames
        if frame.type == int(FrameType.STREAM_DATA)
    ]
    if len(late_data_frames) != 1:
        raise DecodeError("expected one delayed original STREAM_DATA")
    late_data = late_data_frames[0]
    if (
        late_data.transmission_id != reinjected_data.transmission_id
        or late_data.stream_id != reinjected_data.stream_id
        or late_data.offset != reinjected_data.offset
        or late_data.data != reinjected_data.data
    ):
        raise DecodeError("reinjection changed Transmission semantics")
    late = engine.handle_frames(carrier2, late_frames)
    _collect_delivery(late, received, counts)
    late_suppressed = (
        counts == {1: 1, 3: 1}
        and engine.receive_flow.session_committed == committed_before_late
    )
    if not late_suppressed:
        raise DecodeError("late original DATA was not duplicate-suppressed")

    engine.mark_carrier_lost((2, 0))

    reply_attempt1 = engine.send_data(1, reply1)
    reply_attempt3 = engine.send_data(3, reply3)
    if reply_attempt1.carrier_id != 1 or reply_attempt3.carrier_id != 1:
        raise DecodeError("reverse DATA used failed Carrier")

    _recv_handle(engine, carrier1)
    _recv_handle(engine, carrier1)

    # Graceful Stream 1 terminal lifecycle.
    fin_events = _recv_handle(engine, carrier1)
    if (1, "fin") not in fin_events.terminal:
        raise DecodeError("Server did not receive Stream 1 FIN")

    consumed_attempt = engine.release_receive(1)
    if consumed_attempt is None:
        raise DecodeError("Server did not generate STREAM_CONSUMED")
    engine.send_fin(1)

    # ACK STREAM_CONSUMED, ACK Server FIN, then Client STREAM_CONSUMED.
    _recv_handle(engine, carrier1)
    _recv_handle(engine, carrier1)
    _recv_handle(engine, carrier1)

    # STOP/RESET Stream 3.
    engine.send_stop(3, int(ErrorCode.NO_ERROR))
    _recv_handle(engine, carrier1)  # ACK STOP
    reset_events = _recv_handle(engine, carrier1)  # Client RESET
    if (3, "reset") not in reset_events.terminal:
        raise DecodeError("Server did not process Client RESET")

    engine.send_reset(3, int(ErrorCode.NO_ERROR))
    _recv_handle(engine, carrier1)  # ACK Server RESET

    tombstones = tuple(
        sorted(engine.server_terminal.tombstones)
    )
    if tombstones != (1, 3):
        raise DecodeError("Server did not retire both Streams to tombstones")

    close_records = engine.close_session(
        reason="unified Session engine scenario complete",
    )

    return UnifiedSessionResult(
        session_id=session.session_id,
        side="server",
        stream_ids=(1, 3),
        received_stream1=bytes(received[1]),
        received_stream3=bytes(received[3]),
        stream1_deliveries=counts[1],
        stream3_deliveries=counts[3],
        reinjected_transmission_id=reinjected_data.transmission_id,
        reinjection_attempts=((2, 0), (1, 0)),
        late_original_suppressed=late_suppressed,
        carrier2_lost=not carrier2.active,
        retired_stream_ids=(),
        server_tombstones=tombstones,
        session_closed=engine.state.lifecycle == "closed",
        close_records_sent=close_records,
        max_transmission_id=engine.ledger.next_id - 1,
    )


def run_unified_server(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    expect1: bytes,
    expect3: bytes,
    reply1: bytes,
    reply3: bytes,
) -> UnifiedSessionResult:
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
                    expected1=expect1,
                    expected3=expect3,
                    reply1=reply1,
                    reply3=reply3,
                )
            finally:
                join_sock.close()
        finally:
            create_sock.close()


def run_unified_client(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    send1: bytes,
    send3: bytes,
    expect_reply1: bytes,
    expect_reply3: bytes,
) -> UnifiedSessionResult:
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
                payload1=send1,
                payload3=send3,
                reply1=expect_reply1,
                reply3=expect_reply3,
            )
        finally:
            join_sock.close()
    finally:
        create_sock.close()
