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
    decode_session_credit,
    decode_stream_consumed,
    decode_stream_credit,
    decode_stream_data,
    decode_stream_fin,
    decode_stream_open,
    decode_stream_open_ok,
    decode_transmission_ack,
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
from .errors import DecodeError, FinalSizeError
from .stream import ReceiveFlow, ReceiveStream, SendFlow, TransmissionLedger


@dataclass(frozen=True)
class MultipathExchangeResult:
    session_id: bytes
    payload: bytes
    stream_id: int
    transmission_id: int
    application_deliveries: int
    session_committed_bytes: int
    carrier_generations: tuple[tuple[int, int], ...]
    fresh_carrier_keys: bool
    carrier2_first_record_sequence: int


def _process_open_response(
    carrier: SecureCarrier,
    *,
    stream_id: int,
    open_tx: int,
    send_flow: SendFlow,
    ledger: TransmissionLedger,
) -> None:
    frames = carrier.recv()
    for frame in frames:
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
            raise DecodeError("unexpected Frame during multipath Stream opening")
    if open_tx not in ledger.settled:
        raise DecodeError("Stream was not accepted")


def client_reinjection_exchange(
    create_sock: socket.socket,
    join_sock: socket.socket,
    transport_key: bytes,
    payload: bytes,
    *,
    session_id: bytes | None = None,
    create_nonce: bytes | None = None,
    join_nonce: bytes | None = None,
    limits: EndpointLimits = EndpointLimits(),
) -> MultipathExchangeResult:
    if not payload:
        raise ValueError("payload must not be empty")
    session_id = session_id or os.urandom(16)

    carrier1, session = client_create_carrier(
        create_sock,
        transport_key,
        session_id=session_id,
        client_nonce=create_nonce or os.urandom(32),
        client_limits=limits,
        carrier_id=1,
        generation=0,
    )
    carrier2 = client_join_carrier(
        join_sock,
        transport_key,
        session,
        carrier_id=2,
        generation=0,
        client_nonce=join_nonce or os.urandom(32),
    )

    fresh_keys = (
        carrier1.sender.key != carrier2.sender.key
        and carrier1.sender.iv != carrier2.sender.iv
        and carrier1.receiver.key != carrier2.receiver.key
        and carrier1.receiver.iv != carrier2.receiver.iv
    )
    if not fresh_keys:
        raise DecodeError("JOIN did not derive independent Carrier traffic keys")
    if carrier2.sender.sequence_number != 0 or carrier2.receiver.sequence_number != 0:
        raise DecodeError("JOIN Carrier record sequence did not begin at zero")

    stream_id = 1
    send_flow = SendFlow()
    recv_flow = ReceiveFlow()
    recv_stream = ReceiveStream(stream_id)
    ledger = TransmissionLedger()

    open_tx = ledger.allocate(stream_id, "open")
    carrier1.send(
        encode_session_credit(0, recv_flow.session_maximum),
        encode_stream_open(stream_id, open_tx),
    )
    _process_open_response(
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
            recv_flow.stream_consumed[stream_id],
            recv_flow.stream_maximum[stream_id],
        )
    )

    data_tx = ledger.allocate(stream_id, "data")
    send_flow.commit(stream_id, len(payload))
    data_frame = encode_stream_data(stream_id, 0, data_tx, payload)

    carrier2_first_sequence = carrier2.sender.sequence_number
    # Attempt 1 is written on Carrier 1. The same Transmission is then
    # reinjected on Carrier 2 with the same Transmission ID and contents.
    carrier1.send(data_frame)
    carrier2.send(data_frame)

    # The receiver intentionally processes Carrier 2 first. This demonstrates
    # that cross-Carrier arrival order is independent of send order.
    frames = carrier2.recv()
    if len(frames) != 1 or frames[0].type != int(FrameType.TRANSMISSION_ACK):
        raise DecodeError("expected reinjection ACK on Carrier 2")
    ack = decode_transmission_ack(frames[0])
    ledger.settle(ack.stream_id, ack.transmission_id)

    frames = carrier1.recv()
    if len(frames) != 1 or frames[0].type != int(FrameType.TRANSMISSION_ACK):
        raise DecodeError("expected duplicate ACK on Carrier 1")
    ack = decode_transmission_ack(frames[0])
    ledger.settle(ack.stream_id, ack.transmission_id)
    if data_tx not in ledger.settled:
        raise DecodeError("reinjected DATA Transmission was not settled")

    fin_tx = ledger.allocate(stream_id, "fin")
    carrier1.send(encode_stream_fin(stream_id, fin_tx, len(payload)))

    final_acks: list[bytes] = []
    peer_fin = None
    frames = carrier1.recv()
    for frame in frames:
        if frame.type == int(FrameType.TRANSMISSION_ACK):
            ack = decode_transmission_ack(frame)
            ledger.settle(ack.stream_id, ack.transmission_id)
        elif frame.type == int(FrameType.STREAM_CONSUMED):
            terminal = decode_stream_consumed(frame)
            if terminal.final_offset != len(payload):
                raise FinalSizeError("server consumed wrong client final size")
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
            raise DecodeError("unexpected multipath terminal Frame")

    if fin_tx not in ledger.settled:
        raise DecodeError("client FIN was not acknowledged")
    if peer_fin is None or not recv_stream.complete:
        raise DecodeError("server zero-length send direction did not finish")

    consumed_tx = ledger.allocate(stream_id, "consumed")
    carrier1.send(
        *final_acks,
        encode_stream_consumed(stream_id, consumed_tx, peer_fin.final_offset),
    )
    frames = carrier1.recv()
    if len(frames) != 1 or frames[0].type != int(FrameType.TRANSMISSION_ACK):
        raise DecodeError("expected final STREAM_CONSUMED ACK")
    ack = decode_transmission_ack(frames[0])
    ledger.settle(ack.stream_id, ack.transmission_id)

    return MultipathExchangeResult(
        session_id=session.session_id,
        payload=payload,
        stream_id=stream_id,
        transmission_id=data_tx,
        application_deliveries=1,
        session_committed_bytes=send_flow.session_committed,
        carrier_generations=tuple(sorted(session.generations.items())),
        fresh_carrier_keys=fresh_keys,
        carrier2_first_record_sequence=carrier2_first_sequence,
    )


def server_reinjection_exchange(
    create_sock: socket.socket,
    join_sock: socket.socket,
    transport_key: bytes,
    *,
    expected_payload: bytes,
    create_nonce: bytes | None = None,
    join_nonce: bytes | None = None,
    limits: EndpointLimits = EndpointLimits(),
) -> MultipathExchangeResult:
    carrier1, session = server_create_carrier(
        create_sock,
        transport_key,
        server_nonce=create_nonce or os.urandom(32),
        server_limits=limits,
    )
    carrier2 = server_join_carrier(
        join_sock,
        transport_key,
        session,
        server_nonce=join_nonce or os.urandom(32),
    )

    fresh_keys = (
        carrier1.sender.key != carrier2.sender.key
        and carrier1.sender.iv != carrier2.sender.iv
        and carrier1.receiver.key != carrier2.receiver.key
        and carrier1.receiver.iv != carrier2.receiver.iv
    )
    if not fresh_keys:
        raise DecodeError("JOIN did not derive independent Carrier traffic keys")
    if carrier2.sender.sequence_number != 0 or carrier2.receiver.sequence_number != 0:
        raise DecodeError("JOIN Carrier record sequence did not begin at zero")

    stream_id = 1
    send_flow = SendFlow()
    recv_flow = ReceiveFlow()
    recv_stream = ReceiveStream(stream_id)
    ledger = TransmissionLedger()

    frames = carrier1.recv()
    open_tx = None
    for frame in frames:
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
            raise DecodeError("unexpected initial multipath Stream Frame")
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

    # The Client advertises reverse-direction Stream credit on Carrier 1.
    frames = carrier1.recv()
    if len(frames) != 1 or frames[0].type != int(FrameType.STREAM_CREDIT):
        raise DecodeError("expected client STREAM_CREDIT")
    credit = decode_stream_credit(frames[0])
    send_flow.update_stream_credit(
        credit.stream_id,
        credit.consumed_offset,
        credit.maximum_offset,
    )

    application = bytearray()
    deliveries = 0
    received_tx = None

    # The reinjected Attempt on Carrier 2 is intentionally processed first.
    frames = carrier2.recv()
    if len(frames) != 1 or frames[0].type != int(FrameType.STREAM_DATA):
        raise DecodeError("expected reinjected DATA on Carrier 2")
    data = decode_stream_data(frames[0])
    received_tx = data.transmission_id
    recv_flow.accept_commit(stream_id, data.offset + len(data.data))
    delivered = recv_stream.insert(data.offset, data.data)
    if delivered:
        application += delivered
        deliveries += 1
    carrier2.send(
        encode_transmission_ack(stream_id, data.transmission_id, 0)
    )

    # The original Attempt then arrives on Carrier 1. It has the same
    # Transmission ID and Stream bytes, so it consumes no new credit and
    # produces no second application delivery.
    frames = carrier1.recv()
    if len(frames) != 1 or frames[0].type != int(FrameType.STREAM_DATA):
        raise DecodeError("expected original DATA on Carrier 1")
    duplicate = decode_stream_data(frames[0])
    if (
        duplicate.transmission_id != received_tx
        or duplicate.offset != data.offset
        or duplicate.data != data.data
    ):
        raise DecodeError("reinjected Attempt changed Transmission semantics")
    delta = recv_flow.accept_commit(
        stream_id,
        duplicate.offset + len(duplicate.data),
    )
    if delta != 0:
        raise DecodeError("duplicate Attempt consumed additional Session credit")
    delivered = recv_stream.insert(duplicate.offset, duplicate.data)
    if delivered:
        application += delivered
        deliveries += 1
    carrier1.send(
        encode_transmission_ack(
            stream_id,
            duplicate.transmission_id,
            0,
        )
    )

    if bytes(application) != expected_payload:
        raise DecodeError("application payload mismatch after reinjection")
    if deliveries != 1:
        raise DecodeError("reinjected DATA reached application more than once")
    if recv_flow.session_committed != len(expected_payload):
        raise DecodeError("reinjection consumed additional Session credit")

    frames = carrier1.recv()
    if len(frames) != 1 or frames[0].type != int(FrameType.STREAM_FIN):
        raise DecodeError("expected client STREAM_FIN")
    client_fin = decode_stream_fin(frames[0])
    recv_stream.set_final(client_fin.final_offset)
    if not recv_stream.complete:
        raise DecodeError("client FIN arrived before complete data")

    consumed, _, _, _ = recv_flow.consume(
        stream_id,
        recv_stream.delivered_offset,
    )
    if consumed != len(expected_payload):
        raise DecodeError("receive accounting did not consume full payload")

    consumed_tx = ledger.allocate(stream_id, "consumed")
    fin_tx = ledger.allocate(stream_id, "fin")
    carrier1.send(
        encode_transmission_ack(
            stream_id,
            client_fin.transmission_id,
            0,
        ),
        encode_stream_consumed(
            stream_id,
            consumed_tx,
            client_fin.final_offset,
        ),
        encode_stream_fin(stream_id, fin_tx, 0),
    )

    peer_consumed = None
    frames = carrier1.recv()
    for frame in frames:
        if frame.type == int(FrameType.TRANSMISSION_ACK):
            ack = decode_transmission_ack(frame)
            ledger.settle(ack.stream_id, ack.transmission_id)
        elif frame.type == int(FrameType.STREAM_CONSUMED):
            peer_consumed = decode_stream_consumed(frame)
            if peer_consumed.final_offset != 0:
                raise FinalSizeError("client consumed wrong server final size")
        else:
            raise DecodeError("unexpected final multipath client Frame")
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

    return MultipathExchangeResult(
        session_id=session.session_id,
        payload=bytes(application),
        stream_id=stream_id,
        transmission_id=received_tx or 0,
        application_deliveries=deliveries,
        session_committed_bytes=recv_flow.session_committed,
        carrier_generations=tuple(sorted(session.generations.items())),
        fresh_carrier_keys=fresh_keys,
        carrier2_first_record_sequence=0,
    )


def run_multipath_server(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    expected_payload: bytes,
) -> MultipathExchangeResult:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, port))
        listener.listen(2)

        create_sock, _ = listener.accept()
        try:
            # The CREATE handshake must complete before the JOIN can refer to
            # the Session. The second TCP connection is accepted afterwards.
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
                return _server_reinjection_with_carriers(
                    carrier1,
                    carrier2,
                    session,
                    expected_payload,
                )
            finally:
                join_sock.close()
        finally:
            create_sock.close()


def _server_reinjection_with_carriers(
    carrier1: SecureCarrier,
    carrier2: SecureCarrier,
    session,
    expected_payload: bytes,
) -> MultipathExchangeResult:
    stream_id = 1
    send_flow = SendFlow()
    recv_flow = ReceiveFlow()
    recv_stream = ReceiveStream(stream_id)
    ledger = TransmissionLedger()

    frames = carrier1.recv()
    open_tx = None
    for frame in frames:
        if frame.type == int(FrameType.SESSION_CREDIT):
            credit = decode_session_credit(frame)
            send_flow.update_session_credit(
                credit.consumed_bytes,
                credit.maximum_bytes,
            )
        elif frame.type == int(FrameType.STREAM_OPEN):
            sid, open_tx = decode_stream_open(frame)
            if sid != stream_id:
                raise DecodeError("invalid Stream ID")
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
            0,
            recv_flow.stream_maximum[stream_id],
        ),
    )
    frames = carrier1.recv()
    credit = decode_stream_credit(frames[0])
    send_flow.update_stream_credit(
        credit.stream_id,
        credit.consumed_offset,
        credit.maximum_offset,
    )

    application = bytearray()
    deliveries = 0

    data2 = decode_stream_data(carrier2.recv()[0])
    recv_flow.accept_commit(stream_id, data2.offset + len(data2.data))
    chunk = recv_stream.insert(data2.offset, data2.data)
    if chunk:
        application += chunk
        deliveries += 1
    carrier2.send(
        encode_transmission_ack(stream_id, data2.transmission_id, 0)
    )

    data1 = decode_stream_data(carrier1.recv()[0])
    delta = recv_flow.accept_commit(stream_id, data1.offset + len(data1.data))
    if (
        data1.transmission_id != data2.transmission_id
        or data1.offset != data2.offset
        or data1.data != data2.data
        or delta != 0
    ):
        raise DecodeError("invalid cross-Carrier reinjection")
    chunk = recv_stream.insert(data1.offset, data1.data)
    if chunk:
        application += chunk
        deliveries += 1
    carrier1.send(
        encode_transmission_ack(stream_id, data1.transmission_id, 0)
    )

    if bytes(application) != expected_payload or deliveries != 1:
        raise DecodeError("reinjection application semantics failed")

    client_fin = decode_stream_fin(carrier1.recv()[0])
    recv_stream.set_final(client_fin.final_offset)
    recv_flow.consume(stream_id, recv_stream.delivered_offset)

    consumed_tx = ledger.allocate(stream_id, "consumed")
    fin_tx = ledger.allocate(stream_id, "fin")
    carrier1.send(
        encode_transmission_ack(stream_id, client_fin.transmission_id, 0),
        encode_stream_consumed(
            stream_id,
            consumed_tx,
            client_fin.final_offset,
        ),
        encode_stream_fin(stream_id, fin_tx, 0),
    )

    peer_consumed = None
    for frame in carrier1.recv():
        if frame.type == int(FrameType.TRANSMISSION_ACK):
            ack = decode_transmission_ack(frame)
            ledger.settle(ack.stream_id, ack.transmission_id)
        elif frame.type == int(FrameType.STREAM_CONSUMED):
            peer_consumed = decode_stream_consumed(frame)
    if peer_consumed is None:
        raise DecodeError("missing client STREAM_CONSUMED")
    carrier1.send(
        encode_transmission_ack(
            peer_consumed.stream_id,
            peer_consumed.transmission_id,
            0,
        )
    )

    return MultipathExchangeResult(
        session_id=session.session_id,
        payload=bytes(application),
        stream_id=stream_id,
        transmission_id=data2.transmission_id,
        application_deliveries=deliveries,
        session_committed_bytes=recv_flow.session_committed,
        carrier_generations=tuple(sorted(session.generations.items())),
        fresh_carrier_keys=(
            carrier1.sender.key != carrier2.sender.key
            and carrier1.receiver.key != carrier2.receiver.key
        ),
        carrier2_first_record_sequence=0,
    )


def run_multipath_client(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    payload: bytes,
) -> MultipathExchangeResult:
    session_id = os.urandom(16)
    create_sock = socket.create_connection((host, port), timeout=10)
    try:
        carrier1, session = client_create_carrier(
            create_sock,
            transport_key,
            session_id=session_id,
            client_nonce=os.urandom(32),
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
            return _client_reinjection_with_carriers(
                carrier1,
                carrier2,
                session,
                payload,
            )
        finally:
            join_sock.close()
    finally:
        create_sock.close()


def _client_reinjection_with_carriers(
    carrier1: SecureCarrier,
    carrier2: SecureCarrier,
    session,
    payload: bytes,
) -> MultipathExchangeResult:
    stream_id = 1
    send_flow = SendFlow()
    recv_flow = ReceiveFlow()
    recv_stream = ReceiveStream(stream_id)
    ledger = TransmissionLedger()

    open_tx = ledger.allocate(stream_id, "open")
    carrier1.send(
        encode_session_credit(0, recv_flow.session_maximum),
        encode_stream_open(stream_id, open_tx),
    )
    _process_open_response(
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
    send_flow.commit(stream_id, len(payload))
    frame = encode_stream_data(stream_id, 0, data_tx, payload)
    c2_first_seq = carrier2.sender.sequence_number
    carrier1.send(frame)
    carrier2.send(frame)

    ack = decode_transmission_ack(carrier2.recv()[0])
    ledger.settle(ack.stream_id, ack.transmission_id)
    ack = decode_transmission_ack(carrier1.recv()[0])
    ledger.settle(ack.stream_id, ack.transmission_id)

    fin_tx = ledger.allocate(stream_id, "fin")
    carrier1.send(encode_stream_fin(stream_id, fin_tx, len(payload)))

    final_acks = []
    peer_fin = None
    for frame_in in carrier1.recv():
        if frame_in.type == int(FrameType.TRANSMISSION_ACK):
            ack = decode_transmission_ack(frame_in)
            ledger.settle(ack.stream_id, ack.transmission_id)
        elif frame_in.type == int(FrameType.STREAM_CONSUMED):
            term = decode_stream_consumed(frame_in)
            final_acks.append(
                encode_transmission_ack(term.stream_id, term.transmission_id, 0)
            )
        elif frame_in.type == int(FrameType.STREAM_FIN):
            peer_fin = decode_stream_fin(frame_in)
            recv_stream.set_final(peer_fin.final_offset)
            final_acks.append(
                encode_transmission_ack(
                    peer_fin.stream_id,
                    peer_fin.transmission_id,
                    0,
                )
            )
    if peer_fin is None:
        raise DecodeError("missing server STREAM_FIN")

    consumed_tx = ledger.allocate(stream_id, "consumed")
    carrier1.send(
        *final_acks,
        encode_stream_consumed(stream_id, consumed_tx, peer_fin.final_offset),
    )
    ack = decode_transmission_ack(carrier1.recv()[0])
    ledger.settle(ack.stream_id, ack.transmission_id)

    return MultipathExchangeResult(
        session_id=session.session_id,
        payload=payload,
        stream_id=stream_id,
        transmission_id=data_tx,
        application_deliveries=1,
        session_committed_bytes=send_flow.session_committed,
        carrier_generations=tuple(sorted(session.generations.items())),
        fresh_carrier_keys=(
            carrier1.sender.key != carrier2.sender.key
            and carrier1.receiver.key != carrier2.receiver.key
        ),
        carrier2_first_record_sequence=c2_first_seq,
    )
