from __future__ import annotations

from dataclasses import dataclass
import hmac
import os
import socket

from .codec import (
    decode_frames,
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
from .constants import FrameType, HandshakeType, MAGIC, SchedulerID, VERSION
from .crypto import RecordCipher, derive_key_schedule
from .endpoint import (
    EndpointLimits,
    _validate_client_init,
    _validate_server_init,
    build_client_init,
    build_server_init,
)
from .errors import AuthenticationError, DecodeError, FinalSizeError
from .stream import (
    ReceiveFlow,
    ReceiveStream,
    SendFlow,
    TransmissionLedger,
)
from .tcp import recv_handshake, recv_preface, recv_wire_record
from .varint import encode_varint


@dataclass(frozen=True)
class StreamExchangeResult:
    session_id: bytes
    sent: bytes
    received: bytes
    stream_id: int = 1


@dataclass
class _Carrier:
    sock: socket.socket
    sender: RecordCipher
    receiver: RecordCipher
    local_limits: EndpointLimits

    def send(self, *frames: bytes) -> None:
        plaintext = b"".join(frames)
        self.sock.sendall(self.sender.seal(plaintext))

    def recv(self):
        record = recv_wire_record(
            self.sock,
            max_record_size=self.local_limits.max_record_size,
        )
        return decode_frames(self.receiver.open(record))


def _client_carrier(
    sock: socket.socket,
    transport_key: bytes,
    *,
    session_id: bytes,
    client_nonce: bytes,
    limits: EndpointLimits,
) -> _Carrier:
    preface = MAGIC + encode_varint(VERSION)
    client_init = build_client_init(
        session_id=session_id,
        client_nonce=client_nonce,
        limits=limits,
        scheduler=int(SchedulerID.AGGREGATE),
    )
    sock.sendall(preface + client_init)

    server_init = recv_handshake(sock)
    if server_init.type != int(HandshakeType.SERVER_INIT):
        raise DecodeError("expected SERVER_INIT")
    _validate_server_init(server_init.body, int(SchedulerID.AGGREGATE))

    schedule = derive_key_schedule(
        transport_key,
        preface,
        client_init,
        server_init.raw,
    )
    sock.sendall(schedule.client_finished)
    finished = recv_handshake(sock)
    if finished.type != int(HandshakeType.SERVER_FINISHED):
        raise DecodeError("expected SERVER_FINISHED")
    if not hmac.compare_digest(finished.body, schedule.server_verify_data):
        raise AuthenticationError("SERVER_FINISHED verification failed")

    return _Carrier(
        sock=sock,
        sender=RecordCipher(schedule.client_traffic_key, schedule.client_traffic_iv),
        receiver=RecordCipher(schedule.server_traffic_key, schedule.server_traffic_iv),
        local_limits=limits,
    )


def _server_carrier(
    sock: socket.socket,
    transport_key: bytes,
    *,
    server_nonce: bytes,
    limits: EndpointLimits,
) -> tuple[_Carrier, bytes]:
    preface = recv_preface(sock)
    client_init = recv_handshake(sock)
    if client_init.type != int(HandshakeType.CLIENT_INIT):
        raise DecodeError("expected CLIENT_INIT")
    session_id, _, scheduler = _validate_client_init(client_init.body)

    server_init = build_server_init(
        server_nonce=server_nonce,
        limits=limits,
        scheduler=scheduler,
    )
    sock.sendall(server_init)
    schedule = derive_key_schedule(
        transport_key,
        preface.raw,
        client_init.raw,
        server_init,
    )

    finished = recv_handshake(sock)
    if finished.type != int(HandshakeType.CLIENT_FINISHED):
        raise DecodeError("expected CLIENT_FINISHED")
    if not hmac.compare_digest(finished.body, schedule.client_verify_data):
        raise AuthenticationError("CLIENT_FINISHED verification failed")
    sock.sendall(schedule.server_finished)

    return (
        _Carrier(
            sock=sock,
            sender=RecordCipher(schedule.server_traffic_key, schedule.server_traffic_iv),
            receiver=RecordCipher(schedule.client_traffic_key, schedule.client_traffic_iv),
            local_limits=limits,
        ),
        session_id,
    )


def client_stream_exchange(
    sock: socket.socket,
    transport_key: bytes,
    payload: bytes,
    *,
    reply_expected: bytes,
    session_id: bytes | None = None,
    client_nonce: bytes | None = None,
    limits: EndpointLimits = EndpointLimits(),
) -> StreamExchangeResult:
    if not payload:
        raise ValueError("payload must not be empty")
    session_id = session_id or os.urandom(16)
    carrier = _client_carrier(
        sock,
        transport_key,
        session_id=session_id,
        client_nonce=client_nonce or os.urandom(32),
        limits=limits,
    )

    stream_id = 1
    send_flow = SendFlow()
    recv_flow = ReceiveFlow()
    recv_stream = ReceiveStream(stream_id)
    ledger = TransmissionLedger()

    open_tx = ledger.allocate(stream_id, "open")
    carrier.send(
        encode_session_credit(0, recv_flow.session_maximum),
        encode_stream_open(stream_id, open_tx),
    )

    frames = carrier.recv()
    for frame in frames:
        if frame.type == int(FrameType.SESSION_CREDIT):
            credit = decode_session_credit(frame)
            send_flow.update_session_credit(credit.consumed_bytes, credit.maximum_bytes)
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
            raise DecodeError("unexpected Frame during Stream opening")

    if open_tx not in ledger.settled:
        raise DecodeError("Stream was not accepted")
    recv_flow.open_stream(stream_id)

    data_tx = ledger.allocate(stream_id, "data")
    send_flow.commit(stream_id, len(payload))
    carrier.send(
        encode_stream_credit(
            stream_id,
            recv_flow.stream_consumed[stream_id],
            recv_flow.stream_maximum[stream_id],
        ),
        encode_stream_data(stream_id, 0, data_tx, payload),
    )

    reverse_tx = None
    reverse_data = b""
    frames = carrier.recv()
    pending_acks: list[bytes] = []
    for frame in frames:
        if frame.type == int(FrameType.TRANSMISSION_ACK):
            ack = decode_transmission_ack(frame)
            ledger.settle(ack.stream_id, ack.transmission_id)
        elif frame.type == int(FrameType.STREAM_CREDIT):
            credit = decode_stream_credit(frame)
            send_flow.update_stream_credit(
                credit.stream_id,
                credit.consumed_offset,
                credit.maximum_offset,
            )
        elif frame.type == int(FrameType.SESSION_CREDIT):
            credit = decode_session_credit(frame)
            send_flow.update_session_credit(credit.consumed_bytes, credit.maximum_bytes)
        elif frame.type == int(FrameType.STREAM_DATA):
            data = decode_stream_data(frame)
            if data.stream_id != stream_id:
                raise DecodeError("unexpected Stream ID")
            recv_flow.accept_commit(stream_id, data.offset + len(data.data))
            reverse_data += recv_stream.insert(data.offset, data.data)
            reverse_tx = data.transmission_id
            pending_acks.append(
                encode_transmission_ack(stream_id, data.transmission_id, 0)
            )
        else:
            raise DecodeError("unexpected Frame during bidirectional DATA")

    if data_tx not in ledger.settled:
        raise DecodeError("client DATA was not acknowledged")
    if reverse_tx is None or reverse_data != reply_expected:
        raise DecodeError("unexpected reverse Stream payload")

    consumed, maximum, sconsumed, smaximum = recv_flow.consume(
        stream_id,
        recv_stream.delivered_offset,
    )
    fin_tx = ledger.allocate(stream_id, "fin")
    carrier.send(
        *pending_acks,
        encode_stream_credit(stream_id, consumed, maximum),
        encode_session_credit(sconsumed, smaximum),
        encode_stream_fin(stream_id, fin_tx, len(payload)),
    )

    frames = carrier.recv()
    final_acks: list[bytes] = []
    peer_fin = None
    for frame in frames:
        if frame.type == int(FrameType.TRANSMISSION_ACK):
            ack = decode_transmission_ack(frame)
            ledger.settle(ack.stream_id, ack.transmission_id)
        elif frame.type == int(FrameType.STREAM_CONSUMED):
            terminal = decode_stream_consumed(frame)
            if terminal.final_offset != len(payload):
                raise FinalSizeError("peer consumed wrong client final size")
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
        raise DecodeError("server FIN did not complete receive direction")

    consumed_tx = ledger.allocate(stream_id, "consumed")
    carrier.send(
        *final_acks,
        encode_stream_consumed(
            stream_id,
            consumed_tx,
            recv_stream.final_offset or 0,
        ),
    )

    frames = carrier.recv()
    if len(frames) != 1 or frames[0].type != int(FrameType.TRANSMISSION_ACK):
        raise DecodeError("expected final TRANSMISSION_ACK")
    ack = decode_transmission_ack(frames[0])
    ledger.settle(ack.stream_id, ack.transmission_id)
    if consumed_tx not in ledger.settled:
        raise DecodeError("STREAM_CONSUMED was not acknowledged")

    return StreamExchangeResult(session_id, payload, reverse_data, stream_id)


def server_stream_exchange(
    sock: socket.socket,
    transport_key: bytes,
    *,
    reply: bytes,
    expected_payload: bytes,
    server_nonce: bytes | None = None,
    limits: EndpointLimits = EndpointLimits(),
) -> StreamExchangeResult:
    carrier, session_id = _server_carrier(
        sock,
        transport_key,
        server_nonce=server_nonce or os.urandom(32),
        limits=limits,
    )

    stream_id = 1
    send_flow = SendFlow()
    recv_flow = ReceiveFlow()
    recv_stream = ReceiveStream(stream_id)
    ledger = TransmissionLedger()

    frames = carrier.recv()
    open_tx = None
    for frame in frames:
        if frame.type == int(FrameType.SESSION_CREDIT):
            credit = decode_session_credit(frame)
            send_flow.update_session_credit(credit.consumed_bytes, credit.maximum_bytes)
        elif frame.type == int(FrameType.STREAM_OPEN):
            sid, open_tx = decode_stream_open(frame)
            if sid != stream_id or sid % 2 != 1:
                raise DecodeError("invalid Client Stream ID")
        else:
            raise DecodeError("unexpected initial Stream Frame")
    if open_tx is None:
        raise DecodeError("missing STREAM_OPEN")

    recv_flow.open_stream(stream_id)
    carrier.send(
        encode_session_credit(0, recv_flow.session_maximum),
        encode_stream_open_ok(stream_id, open_tx),
        encode_stream_credit(
            stream_id,
            recv_flow.stream_consumed[stream_id],
            recv_flow.stream_maximum[stream_id],
        ),
    )

    frames = carrier.recv()
    client_data = b""
    client_data_tx = None
    for frame in frames:
        if frame.type == int(FrameType.STREAM_CREDIT):
            credit = decode_stream_credit(frame)
            send_flow.update_stream_credit(
                credit.stream_id,
                credit.consumed_offset,
                credit.maximum_offset,
            )
        elif frame.type == int(FrameType.STREAM_DATA):
            data = decode_stream_data(frame)
            recv_flow.accept_commit(stream_id, data.offset + len(data.data))
            client_data += recv_stream.insert(data.offset, data.data)
            client_data_tx = data.transmission_id
        else:
            raise DecodeError("unexpected client DATA Frame")
    if client_data != expected_payload or client_data_tx is None:
        raise DecodeError("unexpected client Stream payload")

    consumed, maximum, sconsumed, smaximum = recv_flow.consume(
        stream_id,
        recv_stream.delivered_offset,
    )
    reverse_tx = ledger.allocate(stream_id, "data")
    send_flow.commit(stream_id, len(reply))
    carrier.send(
        encode_transmission_ack(stream_id, client_data_tx, 0),
        encode_stream_credit(stream_id, consumed, maximum),
        encode_session_credit(sconsumed, smaximum),
        encode_stream_data(stream_id, 0, reverse_tx, reply),
    )

    frames = carrier.recv()
    client_fin = None
    for frame in frames:
        if frame.type == int(FrameType.TRANSMISSION_ACK):
            ack = decode_transmission_ack(frame)
            ledger.settle(ack.stream_id, ack.transmission_id)
        elif frame.type == int(FrameType.STREAM_CREDIT):
            credit = decode_stream_credit(frame)
            send_flow.update_stream_credit(
                credit.stream_id,
                credit.consumed_offset,
                credit.maximum_offset,
            )
        elif frame.type == int(FrameType.SESSION_CREDIT):
            credit = decode_session_credit(frame)
            send_flow.update_session_credit(credit.consumed_bytes, credit.maximum_bytes)
        elif frame.type == int(FrameType.STREAM_FIN):
            client_fin = decode_stream_fin(frame)
            recv_stream.set_final(client_fin.final_offset)
        else:
            raise DecodeError("unexpected client terminal prelude")
    if reverse_tx not in ledger.settled:
        raise DecodeError("server DATA was not acknowledged")
    if client_fin is None or not recv_stream.complete:
        raise DecodeError("client FIN did not complete receive direction")

    consumed_tx = ledger.allocate(stream_id, "consumed")
    fin_tx = ledger.allocate(stream_id, "fin")
    carrier.send(
        encode_transmission_ack(stream_id, client_fin.transmission_id, 0),
        encode_stream_consumed(stream_id, consumed_tx, client_fin.final_offset),
        encode_stream_fin(stream_id, fin_tx, len(reply)),
    )

    frames = carrier.recv()
    peer_consumed = None
    for frame in frames:
        if frame.type == int(FrameType.TRANSMISSION_ACK):
            ack = decode_transmission_ack(frame)
            ledger.settle(ack.stream_id, ack.transmission_id)
        elif frame.type == int(FrameType.STREAM_CONSUMED):
            peer_consumed = decode_stream_consumed(frame)
            if peer_consumed.final_offset != len(reply):
                raise FinalSizeError("peer consumed wrong server final size")
        else:
            raise DecodeError("unexpected final client Frame")
    if consumed_tx not in ledger.settled or fin_tx not in ledger.settled:
        raise DecodeError("server terminal Transmissions not acknowledged")
    if peer_consumed is None:
        raise DecodeError("missing client STREAM_CONSUMED")

    carrier.send(
        encode_transmission_ack(
            peer_consumed.stream_id,
            peer_consumed.transmission_id,
            0,
        )
    )
    return StreamExchangeResult(session_id, reply, client_data, stream_id)


def run_stream_server(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    reply: bytes,
    expected_payload: bytes,
) -> StreamExchangeResult:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, port))
        listener.listen(1)
        conn, _ = listener.accept()
        with conn:
            return server_stream_exchange(
                conn,
                transport_key,
                reply=reply,
                expected_payload=expected_payload,
            )


def run_stream_client(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    payload: bytes,
    reply_expected: bytes,
) -> StreamExchangeResult:
    with socket.create_connection((host, port), timeout=10) as sock:
        return client_stream_exchange(
            sock,
            transport_key,
            payload,
            reply_expected=reply_expected,
        )
