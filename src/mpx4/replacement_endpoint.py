from __future__ import annotations

from dataclasses import dataclass
import os
import socket

from .carrier import (
    ClientSessionState,
    SecureCarrier,
    ServerSessionState,
    client_create_carrier,
    client_join_carrier,
    server_create_carrier,
    server_join_carrier,
)
from .codec import (
    decode_session_credit,
    decode_stream_consumed,
    decode_stream_credit,
    decode_stream_data,
    decode_stream_fin,
    decode_stream_open,
    decode_stream_open_ok,
    decode_token_frame,
    decode_transmission_ack,
    encode_ping,
    encode_pong,
    encode_session_credit,
    encode_stream_consumed,
    encode_stream_credit,
    encode_stream_data,
    encode_stream_fin,
    encode_stream_open,
    encode_stream_open_ok,
    encode_transmission_ack,
)
from .constants import FrameType
from .endpoint import EndpointLimits
from .errors import CarrierLostError, DecodeError, FinalSizeError
from .reliability import ReliabilityLoop, send_tracked_attempt
from .scheduler import AggregateScheduler
from .stream import ReceiveFlow, ReceiveStream, SendFlow, TransmissionLedger


LOSS_SYNC_TOKEN = 0x404


@dataclass(frozen=True)
class ReplacementExchangeResult:
    session_id: bytes
    payload: bytes
    stream_id: int
    transmission_id: int
    attempts: tuple[tuple[int, int], ...]
    scheduler_initial_carrier: tuple[int, int]
    recovery_reason: str
    failed_carrier_inactive: bool
    replacement_generation: int
    replacement_first_record_sequence: int
    fresh_replacement_keys: bool
    carrier_generations: tuple[tuple[int, int], ...]
    application_deliveries: int
    session_committed_bytes: int
    surviving_carrier_active: bool


def _handle_open_response(
    carrier: SecureCarrier,
    *,
    stream_id: int,
    open_tx: int,
    send_flow: SendFlow,
    ledger: TransmissionLedger,
) -> None:
    for frame in carrier.recv():
        if frame.type == int(FrameType.SESSION_CREDIT):
            credit = decode_session_credit(frame)
            send_flow.update_session_credit(
                credit.consumed_bytes,
                credit.maximum_bytes,
            )
        elif frame.type == int(FrameType.STREAM_OPEN_OK):
            sid, txid = decode_stream_open_ok(frame)
            if sid != stream_id:
                raise DecodeError("STREAM_OPEN_OK Stream mismatch")
            ledger.settle_open(sid, txid)
        elif frame.type == int(FrameType.STREAM_CREDIT):
            credit = decode_stream_credit(frame)
            send_flow.update_stream_credit(
                credit.stream_id,
                credit.consumed_offset,
                credit.maximum_offset,
            )
        else:
            raise DecodeError("unexpected Frame during replacement Stream opening")

    if open_tx not in ledger.settled:
        raise DecodeError("Stream was not accepted")


def _open_stream_server(
    carrier1: SecureCarrier,
    *,
    send_flow: SendFlow,
    recv_flow: ReceiveFlow,
    stream_id: int,
) -> int:
    open_tx = None
    for frame in carrier1.recv():
        if frame.type == int(FrameType.SESSION_CREDIT):
            credit = decode_session_credit(frame)
            send_flow.update_session_credit(
                credit.consumed_bytes,
                credit.maximum_bytes,
            )
        elif frame.type == int(FrameType.STREAM_OPEN):
            sid, open_tx = decode_stream_open(frame)
            if sid != stream_id or sid % 2 != 1:
                raise DecodeError("invalid Client Stream ID")
        else:
            raise DecodeError("unexpected initial Stream Frame")

    if open_tx is None:
        raise DecodeError("missing STREAM_OPEN")

    recv_flow.open_stream(stream_id)
    carrier1.send(
        encode_session_credit(0, recv_flow.session_maximum),
        encode_stream_open_ok(stream_id, open_tx),
        encode_stream_credit(
            stream_id,
            recv_flow.stream_consumed[stream_id],
            recv_flow.stream_maximum[stream_id],
        ),
    )

    frames = carrier1.recv()
    if len(frames) != 1 or frames[0].type != int(FrameType.STREAM_CREDIT):
        raise DecodeError("expected client STREAM_CREDIT")
    credit = decode_stream_credit(frames[0])
    send_flow.update_stream_credit(
        credit.stream_id,
        credit.consumed_offset,
        credit.maximum_offset,
    )
    return open_tx


def _finish_client_direction(
    carrier1: SecureCarrier,
    *,
    stream_id: int,
    payload_length: int,
    ledger: TransmissionLedger,
    recv_stream: ReceiveStream,
) -> None:
    fin_tx = ledger.allocate(stream_id, "fin")
    fin_frame = encode_stream_fin(stream_id, fin_tx, payload_length)
    ledger.bind_frame(fin_tx, fin_frame)
    send_tracked_attempt(carrier1, ledger, fin_tx)

    final_acks: list[bytes] = []
    peer_fin = None
    for frame in carrier1.recv():
        if frame.type == int(FrameType.TRANSMISSION_ACK):
            ack = decode_transmission_ack(frame)
            ledger.settle(ack.stream_id, ack.transmission_id)
        elif frame.type == int(FrameType.STREAM_CONSUMED):
            terminal = decode_stream_consumed(frame)
            if terminal.final_offset != payload_length:
                raise FinalSizeError("peer consumed wrong final size")
            final_acks.append(
                encode_transmission_ack(
                    terminal.stream_id,
                    terminal.transmission_id,
                    0,
                )
            )
        elif frame.type == int(FrameType.STREAM_FIN):
            terminal = decode_stream_fin(frame)
            recv_stream.set_final(terminal.final_offset)
            peer_fin = terminal
            final_acks.append(
                encode_transmission_ack(
                    terminal.stream_id,
                    terminal.transmission_id,
                    0,
                )
            )
        else:
            raise DecodeError("unexpected terminal Frame")

    if fin_tx not in ledger.settled:
        raise DecodeError("client FIN was not acknowledged")
    if peer_fin is None or not recv_stream.complete:
        raise DecodeError("server send direction did not finish")

    consumed_tx = ledger.allocate(stream_id, "consumed")
    consumed_frame = encode_stream_consumed(
        stream_id,
        consumed_tx,
        peer_fin.final_offset,
    )
    ledger.bind_frame(consumed_tx, consumed_frame)
    ledger.note_attempt(
        consumed_tx,
        carrier1.identity.carrier_id,
        carrier1.identity.generation,
    )
    carrier1.send(*final_acks, consumed_frame)

    frames = carrier1.recv()
    if len(frames) != 1 or frames[0].type != int(FrameType.TRANSMISSION_ACK):
        raise DecodeError("expected STREAM_CONSUMED acknowledgement")
    ack = decode_transmission_ack(frames[0])
    ledger.settle(ack.stream_id, ack.transmission_id)


def _finish_server_direction(
    carrier1: SecureCarrier,
    *,
    stream_id: int,
    client_fin,
    recv_stream: ReceiveStream,
    recv_flow: ReceiveFlow,
    ledger: TransmissionLedger,
) -> None:
    recv_stream.set_final(client_fin.final_offset)
    if not recv_stream.complete:
        raise DecodeError("client FIN arrived before complete receive data")

    recv_flow.consume(stream_id, recv_stream.delivered_offset)

    consumed_tx = ledger.allocate(stream_id, "consumed")
    consumed_frame = encode_stream_consumed(
        stream_id,
        consumed_tx,
        client_fin.final_offset,
    )
    ledger.bind_frame(consumed_tx, consumed_frame)

    fin_tx = ledger.allocate(stream_id, "fin")
    fin_frame = encode_stream_fin(stream_id, fin_tx, 0)
    ledger.bind_frame(fin_tx, fin_frame)

    ledger.note_attempt(
        consumed_tx,
        carrier1.identity.carrier_id,
        carrier1.identity.generation,
    )
    ledger.note_attempt(
        fin_tx,
        carrier1.identity.carrier_id,
        carrier1.identity.generation,
    )
    carrier1.send(
        encode_transmission_ack(
            stream_id,
            client_fin.transmission_id,
            0,
        ),
        consumed_frame,
        fin_frame,
    )

    peer_consumed = None
    for frame in carrier1.recv():
        if frame.type == int(FrameType.TRANSMISSION_ACK):
            ack = decode_transmission_ack(frame)
            ledger.settle(ack.stream_id, ack.transmission_id)
        elif frame.type == int(FrameType.STREAM_CONSUMED):
            peer_consumed = decode_stream_consumed(frame)
            if peer_consumed.final_offset != 0:
                raise FinalSizeError("peer consumed wrong server final size")
        else:
            raise DecodeError("unexpected final client Frame")

    if consumed_tx not in ledger.settled or fin_tx not in ledger.settled:
        raise DecodeError("server terminal Transmissions were not acknowledged")
    if peer_consumed is None:
        raise DecodeError("missing client STREAM_CONSUMED")

    carrier1.send(
        encode_transmission_ack(
            peer_consumed.stream_id,
            peer_consumed.transmission_id,
            0,
        )
    )


def client_replacement_exchange(
    host: str,
    port: int,
    transport_key: bytes,
    payload: bytes,
    *,
    session_id: bytes | None = None,
    limits: EndpointLimits = EndpointLimits(),
) -> ReplacementExchangeResult:
    if not payload:
        raise ValueError("payload must not be empty")
    session_id = session_id or os.urandom(16)

    create_sock = socket.create_connection((host, port), timeout=10)
    try:
        carrier1, session = client_create_carrier(
            create_sock,
            transport_key,
            session_id=session_id,
            client_nonce=os.urandom(32),
            client_limits=limits,
            carrier_id=1,
            generation=0,
        )

        failed_sock = socket.create_connection((host, port), timeout=10)
        try:
            failed_carrier = client_join_carrier(
                failed_sock,
                transport_key,
                session,
                carrier_id=2,
                generation=0,
                client_nonce=os.urandom(32),
            )

            stream_id = 1
            send_flow = SendFlow()
            recv_flow = ReceiveFlow()
            recv_stream = ReceiveStream(stream_id)
            ledger = TransmissionLedger()
            scheduler = AggregateScheduler()
            scheduler.register(
                carrier1,
                latest_rtt_s=0.080,
                delivery_rate_bps=1_000_000.0,
            )
            scheduler.register(
                failed_carrier,
                latest_rtt_s=0.010,
                delivery_rate_bps=1_000_000.0,
            )
            reliability = ReliabilityLoop(
                ledger,
                scheduler,
                retransmit_after_s=0.250,
            )

            open_tx = ledger.allocate(stream_id, "open")
            open_frame = encode_stream_open(stream_id, open_tx)
            ledger.bind_frame(open_tx, open_frame)
            ledger.note_attempt(
                open_tx,
                carrier1.identity.carrier_id,
                carrier1.identity.generation,
            )
            carrier1.send(
                encode_session_credit(0, recv_flow.session_maximum),
                open_frame,
            )
            _handle_open_response(
                carrier1,
                stream_id=stream_id,
                open_tx=open_tx,
                send_flow=send_flow,
                ledger=ledger,
            )

            recv_flow.open_stream(stream_id)
            carrier1.send(
                encode_stream_credit(
                    stream_id,
                    0,
                    recv_flow.stream_maximum[stream_id],
                )
            )

            data_tx = ledger.allocate(stream_id, "data")
            data_frame = encode_stream_data(
                stream_id,
                0,
                data_tx,
                payload,
            )
            ledger.bind_frame(data_tx, data_frame)
            send_flow.commit(stream_id, len(payload))
            initial_attempt = reliability.transmit_new(data_tx)
            initial_identity = (
                initial_attempt.carrier_id,
                initial_attempt.generation,
            )
            if initial_identity != (2, 0):
                raise DecodeError("AGGREGATE scheduler did not choose the lower-delay Carrier")

            # PING on the surviving Carrier is only a deterministic test
            # synchronization point. The Server closes Carrier 2 after seeing it
            # without parsing the queued DATA Record on Carrier 2.
            carrier1.send(encode_ping(LOSS_SYNC_TOKEN))
            frames = carrier1.recv()
            if (
                len(frames) != 1
                or frames[0].type != int(FrameType.PONG)
                or decode_token_frame(frames[0]) != LOSS_SYNC_TOKEN
            ):
                raise DecodeError("replacement loss synchronization failed")

            try:
                failed_carrier.recv()
            except CarrierLostError:
                scheduler.mark_failure((2, 0))
            else:
                raise DecodeError("failed Carrier did not report transport loss")

            if data_tx not in ledger.pending:
                raise DecodeError("Carrier loss incorrectly settled Transmission")

            old_client_key = failed_carrier.sender.key
            old_server_key = failed_carrier.receiver.key

            replacement_sock = socket.create_connection((host, port), timeout=10)
            try:
                replacement = client_join_carrier(
                    replacement_sock,
                    transport_key,
                    session,
                    carrier_id=2,
                    generation=1,
                    client_nonce=os.urandom(32),
                )

                fresh_keys = (
                    replacement.sender.key != old_client_key
                    and replacement.receiver.key != old_server_key
                )
                if not fresh_keys:
                    raise DecodeError("replacement reused failed Carrier traffic keys")

                first_sequence = replacement.sender.sequence_number
                scheduler.register(
                    replacement,
                    latest_rtt_s=0.010,
                    delivery_rate_bps=1_000_000.0,
                )
                scheduled = reliability.poll()
                if len(scheduled) != 1 or scheduled[0].transmission_id != data_tx:
                    raise DecodeError("reliability loop did not reinject outstanding DATA")
                if scheduled[0].reason != "carrier-loss":
                    raise DecodeError("replacement reinjection used wrong trigger")
                if (scheduled[0].carrier_id, scheduled[0].generation) != (2, 1):
                    raise DecodeError("scheduler did not choose the replacement Carrier")
                data_attempts = tuple(ledger.pending[data_tx].attempts)

                frames = replacement.recv()
                if (
                    len(frames) != 1
                    or frames[0].type != int(FrameType.TRANSMISSION_ACK)
                ):
                    raise DecodeError("replacement DATA was not acknowledged")
                ack = decode_transmission_ack(frames[0])
                reliability.acknowledge(
                    ack.stream_id,
                    ack.transmission_id,
                    ack_carrier=(2, 1),
                )

                _finish_client_direction(
                    carrier1,
                    stream_id=stream_id,
                    payload_length=len(payload),
                    ledger=ledger,
                    recv_stream=recv_stream,
                )

                return ReplacementExchangeResult(
                    session_id=session.session_id,
                    payload=payload,
                    stream_id=stream_id,
                    transmission_id=data_tx,
                    attempts=data_attempts,
                    scheduler_initial_carrier=initial_identity,
                    recovery_reason=scheduled[0].reason,
                    failed_carrier_inactive=not failed_carrier.active,
                    replacement_generation=replacement.identity.generation,
                    replacement_first_record_sequence=first_sequence,
                    fresh_replacement_keys=fresh_keys,
                    carrier_generations=tuple(sorted(session.generations.items())),
                    application_deliveries=1,
                    session_committed_bytes=send_flow.session_committed,
                    surviving_carrier_active=carrier1.active,
                )
            finally:
                replacement_sock.close()
        finally:
            failed_sock.close()
    finally:
        create_sock.close()


def server_replacement_exchange(
    listener: socket.socket,
    transport_key: bytes,
    expected_payload: bytes,
    *,
    limits: EndpointLimits = EndpointLimits(),
) -> ReplacementExchangeResult:
    create_sock, _ = listener.accept()
    try:
        carrier1, session = server_create_carrier(
            create_sock,
            transport_key,
            server_nonce=os.urandom(32),
            server_limits=limits,
        )

        failed_sock, _ = listener.accept()
        failed_carrier = None
        try:
            failed_carrier = server_join_carrier(
                failed_sock,
                transport_key,
                session,
                server_nonce=os.urandom(32),
            )

            stream_id = 1
            send_flow = SendFlow()
            recv_flow = ReceiveFlow()
            recv_stream = ReceiveStream(stream_id)
            ledger = TransmissionLedger()

            _open_stream_server(
                carrier1,
                send_flow=send_flow,
                recv_flow=recv_flow,
                stream_id=stream_id,
            )

            # The Client writes its DATA on Carrier 2, then sends this PING on
            # Carrier 1. We intentionally discard Carrier 2 without reading it.
            frames = carrier1.recv()
            if (
                len(frames) != 1
                or frames[0].type != int(FrameType.PING)
                or decode_token_frame(frames[0]) != LOSS_SYNC_TOKEN
            ):
                raise DecodeError("replacement loss synchronization failed")

            old_client_key = failed_carrier.receiver.key
            old_server_key = failed_carrier.sender.key
            failed_carrier.mark_lost()
            try:
                failed_sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            failed_sock.close()
            carrier1.send(encode_pong(LOSS_SYNC_TOKEN))

            replacement_sock, _ = listener.accept()
            try:
                replacement = server_join_carrier(
                    replacement_sock,
                    transport_key,
                    session,
                    server_nonce=os.urandom(32),
                )
                if replacement.identity.carrier_id != 2:
                    raise DecodeError("replacement changed Carrier ID")
                if replacement.identity.generation != 1:
                    raise DecodeError("replacement did not increment Generation")

                fresh_keys = (
                    replacement.receiver.key != old_client_key
                    and replacement.sender.key != old_server_key
                )
                if not fresh_keys:
                    raise DecodeError("replacement reused failed Carrier traffic keys")
                first_sequence = replacement.receiver.sequence_number

                frames = replacement.recv()
                if (
                    len(frames) != 1
                    or frames[0].type != int(FrameType.STREAM_DATA)
                ):
                    raise DecodeError("expected reinjected DATA on replacement Carrier")
                data = decode_stream_data(frames[0])
                recv_flow.accept_commit(
                    stream_id,
                    data.offset + len(data.data),
                )
                delivered = recv_stream.insert(data.offset, data.data)
                if delivered != expected_payload:
                    raise DecodeError("replacement payload mismatch")
                replacement.send(
                    encode_transmission_ack(
                        stream_id,
                        data.transmission_id,
                        0,
                    )
                )

                frames = carrier1.recv()
                if (
                    len(frames) != 1
                    or frames[0].type != int(FrameType.STREAM_FIN)
                ):
                    raise DecodeError("expected client STREAM_FIN")
                client_fin = decode_stream_fin(frames[0])

                _finish_server_direction(
                    carrier1,
                    stream_id=stream_id,
                    client_fin=client_fin,
                    recv_stream=recv_stream,
                    recv_flow=recv_flow,
                    ledger=ledger,
                )

                return ReplacementExchangeResult(
                    session_id=session.session_id,
                    payload=delivered,
                    stream_id=stream_id,
                    transmission_id=data.transmission_id,
                    attempts=((2, 0), (2, 1)),
                    scheduler_initial_carrier=(2, 0),
                    recovery_reason="carrier-loss",
                    failed_carrier_inactive=not failed_carrier.active,
                    replacement_generation=replacement.identity.generation,
                    replacement_first_record_sequence=first_sequence,
                    fresh_replacement_keys=fresh_keys,
                    carrier_generations=tuple(sorted(session.generations.items())),
                    application_deliveries=1,
                    session_committed_bytes=recv_flow.session_committed,
                    surviving_carrier_active=carrier1.active,
                )
            finally:
                replacement_sock.close()
        finally:
            if failed_carrier is not None:
                failed_carrier.mark_lost()
            try:
                failed_sock.close()
            except OSError:
                pass
    finally:
        create_sock.close()


def run_replacement_server(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    expected_payload: bytes,
) -> ReplacementExchangeResult:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, port))
        listener.listen(3)
        return server_replacement_exchange(
            listener,
            transport_key,
            expected_payload,
        )


def run_replacement_client(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    payload: bytes,
) -> ReplacementExchangeResult:
    return client_replacement_exchange(
        host,
        port,
        transport_key,
        payload,
    )
