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
from .codec import (
    ResetStream,
    StopSending,
    StreamCredit,
    StreamTerminal,
    decode_reset_stream,
    decode_stream_open,
    decode_transmission_ack,
    encode_reset_stream,
    encode_stop_sending,
    encode_stream_credit,
    encode_stream_fin,
    encode_stream_open,
    encode_stream_open_ok,
    encode_transmission_ack,
)
from .constants import ErrorCode, FrameType
from .errors import DecodeError
from .opening import ClientOpeningState
from .stream import ReceiveFlow, ServerStreamRegistry, SendFlow, TransmissionLedger


EVIDENCE_MODES = {"credit", "fin", "reset", "stop"}


@dataclass(frozen=True)
class OpeningEvidenceResult:
    session_id: bytes
    mode: str
    stream_id: int
    phase_before_open_ok: str
    final_phase: str
    acceptance_evidence: bool
    open_settled: bool
    evidence_acked: bool
    client_reset_sent: bool
    client_reset_acked: bool
    peer_terminal_kind: str | None
    stream_credit_maximum: int


def _single(frames, expected_type: FrameType):
    if len(frames) != 1 or frames[0].type != int(expected_type):
        raise DecodeError(f"expected one {expected_type.name}")
    return frames[0]


def client_opening_evidence_exchange(
    create_sock: socket.socket,
    join_sock: socket.socket,
    transport_key: bytes,
    *,
    mode: str,
    session_id: bytes | None = None,
) -> OpeningEvidenceResult:
    if mode not in EVIDENCE_MODES:
        raise ValueError("invalid acceptance-evidence mode")

    carrier1, session = client_create_carrier(
        create_sock,
        transport_key,
        session_id=session_id or os.urandom(16),
        client_nonce=os.urandom(32),
        carrier_id=1,
        generation=0,
    )
    carrier2 = client_join_carrier(
        join_sock,
        transport_key,
        session,
        carrier_id=2,
        generation=0,
        client_nonce=os.urandom(32),
    )

    stream_id = 1
    ledger = TransmissionLedger()
    send_flow = SendFlow()
    open_txid = ledger.allocate(stream_id, "open")
    open_wire = encode_stream_open(stream_id, open_txid)
    ledger.bind_frame(open_txid, open_wire)
    opening = ClientOpeningState(
        stream_id=stream_id,
        open_transmission_id=open_txid,
        ledger=ledger,
        send_flow=send_flow,
    )
    carrier1.send(open_wire)

    evidence_frames = carrier2.recv()
    evidence_acked = False
    client_reset_sent = False
    client_reset_acked = False

    if mode == "credit":
        frame = _single(evidence_frames, FrameType.STREAM_CREDIT)
        from .codec import decode_stream_credit

        opening.observe_credit(decode_stream_credit(frame))
    elif mode == "fin":
        frame = _single(evidence_frames, FrameType.STREAM_FIN)
        from .codec import decode_stream_fin

        result = opening.observe_fin(decode_stream_fin(frame))
        carrier2.send(*result.responses)
        evidence_acked = True
    elif mode == "reset":
        frame = _single(evidence_frames, FrameType.RESET_STREAM)
        result = opening.observe_reset(decode_reset_stream(frame))
        carrier2.send(*result.responses)
        evidence_acked = True
    else:
        frame = _single(evidence_frames, FrameType.STOP_SENDING)
        from .codec import decode_stop_sending

        result = opening.observe_stop(decode_stop_sending(frame))
        if len(result.responses) != 2:
            raise DecodeError("STOP acceptance evidence must produce ACK + RESET")
        carrier2.send(*result.responses)
        evidence_acked = True
        client_reset_sent = True

        ack = decode_transmission_ack(
            _single(carrier2.recv(), FrameType.TRANSMISSION_ACK)
        )
        opening.observe_ack(ack)
        client_reset_acked = opening.local_reset_acked

    phase_before = opening.phase
    if phase_before != "OPENING_WITH_ACCEPTANCE_EVIDENCE":
        raise DecodeError("acceptance evidence did not change opening phase")

    from .codec import decode_stream_open_ok

    sid, txid = decode_stream_open_ok(
        _single(carrier1.recv(), FrameType.STREAM_OPEN_OK)
    )
    opening.observe_open_ok(sid, txid)

    return OpeningEvidenceResult(
        session_id=session.session_id,
        mode=mode,
        stream_id=stream_id,
        phase_before_open_ok=phase_before,
        final_phase=opening.phase,
        acceptance_evidence=opening.acceptance_evidence,
        open_settled=open_txid in ledger.settled,
        evidence_acked=evidence_acked,
        client_reset_sent=client_reset_sent,
        client_reset_acked=client_reset_acked,
        peer_terminal_kind=opening.receive.terminal_kind,
        stream_credit_maximum=(
            send_flow.stream_credit.get(stream_id).maximum
            if stream_id in send_flow.stream_credit
            else 0
        ),
    )


def server_opening_evidence_exchange(
    create_sock: socket.socket,
    join_sock: socket.socket,
    transport_key: bytes,
    *,
    mode: str,
) -> OpeningEvidenceResult:
    if mode not in EVIDENCE_MODES:
        raise ValueError("invalid acceptance-evidence mode")

    carrier1, session = server_create_carrier(
        create_sock,
        transport_key,
        server_nonce=os.urandom(32),
    )
    carrier2 = server_join_carrier(
        join_sock,
        transport_key,
        session,
        server_nonce=os.urandom(32),
    )

    frame = _single(carrier1.recv(), FrameType.STREAM_OPEN)
    stream_id, open_txid = decode_stream_open(frame)

    registry = ServerStreamRegistry(
        max_active=session.server_limits.max_streams
    )
    recv_flow = ReceiveFlow()
    registry.accept_open(stream_id)
    recv_flow.open_stream(stream_id)

    ledger = TransmissionLedger()
    evidence_acked = False
    client_reset_sent = False
    client_reset_acked = False
    peer_terminal_kind = None
    stream_credit_maximum = 0

    if mode == "credit":
        stream_credit_maximum = recv_flow.stream_maximum[stream_id]
        carrier2.send(
            encode_stream_credit(
                stream_id,
                0,
                stream_credit_maximum,
            )
        )
    elif mode == "fin":
        txid = ledger.allocate(stream_id, "fin")
        wire = encode_stream_fin(stream_id, txid, 0)
        ledger.bind_frame(txid, wire)
        carrier2.send(wire)
        ack = decode_transmission_ack(
            _single(carrier2.recv(), FrameType.TRANSMISSION_ACK)
        )
        ledger.settle(ack.stream_id, ack.transmission_id)
        evidence_acked = txid in ledger.settled
        peer_terminal_kind = "fin"
    elif mode == "reset":
        txid = ledger.allocate(stream_id, "reset")
        wire = encode_reset_stream(
            stream_id,
            txid,
            0,
            int(ErrorCode.NO_ERROR),
        )
        ledger.bind_frame(txid, wire)
        carrier2.send(wire)
        ack = decode_transmission_ack(
            _single(carrier2.recv(), FrameType.TRANSMISSION_ACK)
        )
        ledger.settle(ack.stream_id, ack.transmission_id)
        evidence_acked = txid in ledger.settled
        peer_terminal_kind = "reset"
    else:
        txid = ledger.allocate(stream_id, "stop")
        wire = encode_stop_sending(
            stream_id,
            txid,
            int(ErrorCode.NO_ERROR),
        )
        ledger.bind_frame(txid, wire)
        carrier2.send(wire)

        frames = carrier2.recv()
        if len(frames) != 2:
            raise DecodeError("expected STOP ACK plus Client RESET_STREAM")

        ack = None
        reset = None
        for response in frames:
            if response.type == int(FrameType.TRANSMISSION_ACK):
                ack = decode_transmission_ack(response)
            elif response.type == int(FrameType.RESET_STREAM):
                reset = decode_reset_stream(response)
            else:
                raise DecodeError("unexpected STOP evidence response")

        if ack is None or reset is None:
            raise DecodeError("incomplete STOP evidence response")
        ledger.settle(ack.stream_id, ack.transmission_id)
        evidence_acked = txid in ledger.settled
        if reset.final_offset != 0:
            raise DecodeError("Client STOP response RESET final size is non-zero")
        client_reset_sent = True
        carrier2.send(
            encode_transmission_ack(
                reset.stream_id,
                reset.transmission_id,
                0,
            )
        )
        client_reset_acked = True

    carrier1.send(encode_stream_open_ok(stream_id, open_txid))

    return OpeningEvidenceResult(
        session_id=session.session_id,
        mode=mode,
        stream_id=stream_id,
        phase_before_open_ok="OPENING_WITH_ACCEPTANCE_EVIDENCE",
        final_phase="OPEN",
        acceptance_evidence=True,
        open_settled=True,
        evidence_acked=evidence_acked,
        client_reset_sent=client_reset_sent,
        client_reset_acked=client_reset_acked,
        peer_terminal_kind=peer_terminal_kind,
        stream_credit_maximum=stream_credit_maximum,
    )


def run_opening_server(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    mode: str,
) -> OpeningEvidenceResult:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, port))
        listener.listen(2)

        create_sock, _ = listener.accept()
        try:
            # The first authenticated Carrier creates the Session before JOIN.
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
                    mode,
                )
            finally:
                join_sock.close()
        finally:
            create_sock.close()


def _server_with_carriers(
    carrier1: SecureCarrier,
    carrier2: SecureCarrier,
    session,
    mode: str,
) -> OpeningEvidenceResult:
    frame = _single(carrier1.recv(), FrameType.STREAM_OPEN)
    stream_id, open_txid = decode_stream_open(frame)

    registry = ServerStreamRegistry(
        max_active=session.server_limits.max_streams
    )
    recv_flow = ReceiveFlow()
    registry.accept_open(stream_id)
    recv_flow.open_stream(stream_id)

    ledger = TransmissionLedger()
    evidence_acked = False
    client_reset_sent = False
    client_reset_acked = False
    peer_terminal_kind = None
    stream_credit_maximum = 0

    if mode == "credit":
        stream_credit_maximum = recv_flow.stream_maximum[stream_id]
        carrier2.send(
            encode_stream_credit(stream_id, 0, stream_credit_maximum)
        )
    elif mode == "fin":
        txid = ledger.allocate(stream_id, "fin")
        wire = encode_stream_fin(stream_id, txid, 0)
        ledger.bind_frame(txid, wire)
        carrier2.send(wire)
        ack = decode_transmission_ack(
            _single(carrier2.recv(), FrameType.TRANSMISSION_ACK)
        )
        ledger.settle(ack.stream_id, ack.transmission_id)
        evidence_acked = txid in ledger.settled
        peer_terminal_kind = "fin"
    elif mode == "reset":
        txid = ledger.allocate(stream_id, "reset")
        wire = encode_reset_stream(stream_id, txid, 0, 0)
        ledger.bind_frame(txid, wire)
        carrier2.send(wire)
        ack = decode_transmission_ack(
            _single(carrier2.recv(), FrameType.TRANSMISSION_ACK)
        )
        ledger.settle(ack.stream_id, ack.transmission_id)
        evidence_acked = txid in ledger.settled
        peer_terminal_kind = "reset"
    elif mode == "stop":
        txid = ledger.allocate(stream_id, "stop")
        wire = encode_stop_sending(stream_id, txid, 0)
        ledger.bind_frame(txid, wire)
        carrier2.send(wire)
        frames = carrier2.recv()
        ack = None
        reset = None
        for response in frames:
            if response.type == int(FrameType.TRANSMISSION_ACK):
                ack = decode_transmission_ack(response)
            elif response.type == int(FrameType.RESET_STREAM):
                reset = decode_reset_stream(response)
        if ack is None or reset is None:
            raise DecodeError("missing STOP acceptance-evidence response")
        ledger.settle(ack.stream_id, ack.transmission_id)
        evidence_acked = txid in ledger.settled
        if reset.final_offset != 0:
            raise DecodeError("Client RESET final size must be zero")
        client_reset_sent = True
        carrier2.send(
            encode_transmission_ack(
                reset.stream_id,
                reset.transmission_id,
                0,
            )
        )
        client_reset_acked = True
    else:
        raise ValueError("invalid acceptance-evidence mode")

    carrier1.send(encode_stream_open_ok(stream_id, open_txid))
    return OpeningEvidenceResult(
        session_id=session.session_id,
        mode=mode,
        stream_id=stream_id,
        phase_before_open_ok="OPENING_WITH_ACCEPTANCE_EVIDENCE",
        final_phase="OPEN",
        acceptance_evidence=True,
        open_settled=True,
        evidence_acked=evidence_acked,
        client_reset_sent=client_reset_sent,
        client_reset_acked=client_reset_acked,
        peer_terminal_kind=peer_terminal_kind,
        stream_credit_maximum=stream_credit_maximum,
    )


def run_opening_client(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    mode: str,
) -> OpeningEvidenceResult:
    session_id = os.urandom(16)
    create_sock = socket.create_connection((host, port), timeout=10)
    try:
        carrier1, session = client_create_carrier(
            create_sock,
            transport_key,
            session_id=session_id,
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
                mode,
            )
        finally:
            join_sock.close()
    finally:
        create_sock.close()


def _client_with_carriers(
    carrier1: SecureCarrier,
    carrier2: SecureCarrier,
    session,
    mode: str,
) -> OpeningEvidenceResult:
    stream_id = 1
    ledger = TransmissionLedger()
    send_flow = SendFlow()
    open_txid = ledger.allocate(stream_id, "open")
    open_wire = encode_stream_open(stream_id, open_txid)
    ledger.bind_frame(open_txid, open_wire)
    opening = ClientOpeningState(
        stream_id=stream_id,
        open_transmission_id=open_txid,
        ledger=ledger,
        send_flow=send_flow,
    )
    carrier1.send(open_wire)

    frames = carrier2.recv()
    evidence_acked = False
    client_reset_sent = False
    client_reset_acked = False

    if mode == "credit":
        from .codec import decode_stream_credit

        opening.observe_credit(
            decode_stream_credit(_single(frames, FrameType.STREAM_CREDIT))
        )
    elif mode == "fin":
        from .codec import decode_stream_fin

        result = opening.observe_fin(
            decode_stream_fin(_single(frames, FrameType.STREAM_FIN))
        )
        carrier2.send(*result.responses)
        evidence_acked = True
    elif mode == "reset":
        result = opening.observe_reset(
            decode_reset_stream(_single(frames, FrameType.RESET_STREAM))
        )
        carrier2.send(*result.responses)
        evidence_acked = True
    elif mode == "stop":
        from .codec import decode_stop_sending

        result = opening.observe_stop(
            decode_stop_sending(_single(frames, FrameType.STOP_SENDING))
        )
        carrier2.send(*result.responses)
        evidence_acked = True
        client_reset_sent = True
        ack = decode_transmission_ack(
            _single(carrier2.recv(), FrameType.TRANSMISSION_ACK)
        )
        opening.observe_ack(ack)
        client_reset_acked = opening.local_reset_acked
    else:
        raise ValueError("invalid acceptance-evidence mode")

    phase_before = opening.phase
    from .codec import decode_stream_open_ok

    sid, txid = decode_stream_open_ok(
        _single(carrier1.recv(), FrameType.STREAM_OPEN_OK)
    )
    opening.observe_open_ok(sid, txid)

    return OpeningEvidenceResult(
        session_id=session.session_id,
        mode=mode,
        stream_id=stream_id,
        phase_before_open_ok=phase_before,
        final_phase=opening.phase,
        acceptance_evidence=opening.acceptance_evidence,
        open_settled=open_txid in ledger.settled,
        evidence_acked=evidence_acked,
        client_reset_sent=client_reset_sent,
        client_reset_acked=client_reset_acked,
        peer_terminal_kind=opening.receive.terminal_kind,
        stream_credit_maximum=(
            send_flow.stream_credit[stream_id].maximum
            if stream_id in send_flow.stream_credit
            else 0
        ),
    )
