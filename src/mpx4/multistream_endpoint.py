from __future__ import annotations

from dataclasses import dataclass
import os
import socket

from .carrier import client_create_carrier, server_create_carrier
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
from .constants import FrameType, SchedulerID
from .endpoint import EndpointLimits
from .errors import DecodeError, FinalSizeError
from .stream import (
    ClientStreamRegistry,
    ReceiveFlow,
    ReceiveStream,
    SendFlow,
    ServerStreamRegistry,
    TransmissionLedger,
)


@dataclass(frozen=True)
class MultiStreamResult:
    session_id: bytes
    stream_ids: tuple[int, int]
    payloads: tuple[bytes, bytes]
    transmission_ids: tuple[int, int]
    session_committed_bytes: int
    retired_stream_ids: tuple[int, int]


def _note_batched_attempts(carrier, ledger, transmission_ids) -> None:
    for transmission_id in transmission_ids:
        ledger.note_attempt(
            transmission_id,
            carrier.identity.carrier_id,
            carrier.identity.generation,
        )


def client_multistream_exchange(
    sock: socket.socket,
    transport_key: bytes,
    payload1: bytes,
    payload2: bytes,
    *,
    session_id: bytes | None = None,
    client_nonce: bytes | None = None,
    limits: EndpointLimits = EndpointLimits(),
) -> MultiStreamResult:
    if not payload1 or not payload2:
        raise ValueError("both Stream payloads must be non-empty")

    carrier, session = client_create_carrier(
        sock,
        transport_key,
        session_id=session_id or os.urandom(16),
        client_nonce=client_nonce or os.urandom(32),
        client_limits=limits,
        scheduler=int(SchedulerID.AGGREGATE),
    )

    registry = ClientStreamRegistry(max_active=session.server_limits.max_streams)
    stream_ids = (registry.allocate(), registry.allocate())
    send_flow = SendFlow()
    recv_flow = ReceiveFlow()
    recv_streams = {stream_id: ReceiveStream(stream_id) for stream_id in stream_ids}
    ledger = TransmissionLedger()

    open_items: list[tuple[int, int, bytes]] = []
    for stream_id in stream_ids:
        txid = ledger.allocate(stream_id, "open")
        frame = encode_stream_open(stream_id, txid)
        ledger.bind_frame(txid, frame)
        open_items.append((stream_id, txid, frame))

    _note_batched_attempts(carrier, ledger, [item[1] for item in open_items])
    carrier.send(
        encode_session_credit(0, recv_flow.session_maximum),
        *(item[2] for item in open_items),
    )

    accepted: set[int] = set()
    for frame in carrier.recv():
        if frame.type == int(FrameType.SESSION_CREDIT):
            credit = decode_session_credit(frame)
            send_flow.update_session_credit(
                credit.consumed_bytes,
                credit.maximum_bytes,
            )
        elif frame.type == int(FrameType.STREAM_OPEN_OK):
            stream_id, txid = decode_stream_open_ok(frame)
            ledger.settle_open(stream_id, txid)
            accepted.add(stream_id)
        elif frame.type == int(FrameType.STREAM_CREDIT):
            credit = decode_stream_credit(frame)
            send_flow.update_stream_credit(
                credit.stream_id,
                credit.consumed_offset,
                credit.maximum_offset,
            )
        else:
            raise DecodeError("unexpected Frame during multi-Stream opening")

    if accepted != set(stream_ids):
        raise DecodeError("not all Streams were accepted")

    for stream_id in stream_ids:
        recv_flow.open_stream(stream_id)

    data_items: list[tuple[int, int, bytes]] = []
    for stream_id, payload in zip(stream_ids, (payload1, payload2)):
        txid = ledger.allocate(stream_id, "data")
        frame = encode_stream_data(stream_id, 0, txid, payload)
        ledger.bind_frame(txid, frame)
        send_flow.commit(stream_id, len(payload))
        data_items.append((stream_id, txid, frame))

    _note_batched_attempts(carrier, ledger, [item[1] for item in data_items])
    carrier.send(
        *(
            encode_stream_credit(
                stream_id,
                recv_flow.stream_consumed[stream_id],
                recv_flow.stream_maximum[stream_id],
            )
            for stream_id in stream_ids
        ),
        *(item[2] for item in data_items),
    )

    data_acks: set[int] = set()
    for frame in carrier.recv():
        if frame.type != int(FrameType.TRANSMISSION_ACK):
            raise DecodeError("expected DATA acknowledgements")
        ack = decode_transmission_ack(frame)
        ledger.settle(ack.stream_id, ack.transmission_id)
        data_acks.add(ack.transmission_id)

    data_txids = tuple(item[1] for item in data_items)
    if data_acks != set(data_txids):
        raise DecodeError("not all multi-Stream DATA was acknowledged")

    fin_items: list[tuple[int, int, bytes]] = []
    for stream_id, payload in zip(stream_ids, (payload1, payload2)):
        txid = ledger.allocate(stream_id, "fin")
        frame = encode_stream_fin(stream_id, txid, len(payload))
        ledger.bind_frame(txid, frame)
        fin_items.append((stream_id, txid, frame))

    _note_batched_attempts(carrier, ledger, [item[1] for item in fin_items])
    carrier.send(*(item[2] for item in fin_items))

    ack_frames: list[bytes] = []
    peer_fin_seen: set[int] = set()
    peer_consumed_seen: set[int] = set()
    for frame in carrier.recv():
        if frame.type == int(FrameType.TRANSMISSION_ACK):
            ack = decode_transmission_ack(frame)
            ledger.settle(ack.stream_id, ack.transmission_id)
        elif frame.type == int(FrameType.STREAM_CONSUMED):
            terminal = decode_stream_consumed(frame)
            expected = len(payload1) if terminal.stream_id == stream_ids[0] else len(payload2)
            if terminal.final_offset != expected:
                raise FinalSizeError("peer consumed wrong multi-Stream final size")
            peer_consumed_seen.add(terminal.stream_id)
            ack_frames.append(
                encode_transmission_ack(
                    terminal.stream_id,
                    terminal.transmission_id,
                    0,
                )
            )
        elif frame.type == int(FrameType.STREAM_FIN):
            terminal = decode_stream_fin(frame)
            if terminal.final_offset != 0:
                raise FinalSizeError("reference Server send direction must be empty")
            recv_streams[terminal.stream_id].set_final(terminal.final_offset)
            peer_fin_seen.add(terminal.stream_id)
            ack_frames.append(
                encode_transmission_ack(
                    terminal.stream_id,
                    terminal.transmission_id,
                    0,
                )
            )
        else:
            raise DecodeError("unexpected multi-Stream terminal Frame")

    if peer_fin_seen != set(stream_ids) or peer_consumed_seen != set(stream_ids):
        raise DecodeError("incomplete multi-Stream terminal response")
    for stream_id in stream_ids:
        if not recv_streams[stream_id].complete:
            raise DecodeError("Server send direction did not finish")

    consumed_items: list[tuple[int, int, bytes]] = []
    for stream_id in stream_ids:
        txid = ledger.allocate(stream_id, "consumed")
        frame = encode_stream_consumed(stream_id, txid, 0)
        ledger.bind_frame(txid, frame)
        consumed_items.append((stream_id, txid, frame))

    _note_batched_attempts(carrier, ledger, [item[1] for item in consumed_items])
    carrier.send(*ack_frames, *(item[2] for item in consumed_items))

    final_acks: set[int] = set()
    for frame in carrier.recv():
        if frame.type != int(FrameType.TRANSMISSION_ACK):
            raise DecodeError("expected final multi-Stream acknowledgements")
        ack = decode_transmission_ack(frame)
        ledger.settle(ack.stream_id, ack.transmission_id)
        final_acks.add(ack.transmission_id)

    if final_acks != {item[1] for item in consumed_items}:
        raise DecodeError("multi-Stream STREAM_CONSUMED was not fully acknowledged")

    for stream_id in stream_ids:
        registry.retire(stream_id)

    return MultiStreamResult(
        session_id=session.session_id,
        stream_ids=stream_ids,
        payloads=(payload1, payload2),
        transmission_ids=data_txids,
        session_committed_bytes=send_flow.session_committed,
        retired_stream_ids=tuple(sorted(registry.used - registry.active)),
    )


def server_multistream_exchange(
    sock: socket.socket,
    transport_key: bytes,
    *,
    expected_payload1: bytes,
    expected_payload2: bytes,
    server_nonce: bytes | None = None,
    limits: EndpointLimits = EndpointLimits(),
) -> MultiStreamResult:
    carrier, session = server_create_carrier(
        sock,
        transport_key,
        server_nonce=server_nonce or os.urandom(32),
        server_limits=limits,
    )

    registry = ServerStreamRegistry(max_active=session.server_limits.max_streams)
    send_flow = SendFlow()
    recv_flow = ReceiveFlow()
    recv_streams: dict[int, ReceiveStream] = {}
    ledger = TransmissionLedger()
    open_txids: dict[int, int] = {}

    for frame in carrier.recv():
        if frame.type == int(FrameType.SESSION_CREDIT):
            credit = decode_session_credit(frame)
            send_flow.update_session_credit(
                credit.consumed_bytes,
                credit.maximum_bytes,
            )
        elif frame.type == int(FrameType.STREAM_OPEN):
            stream_id, txid = decode_stream_open(frame)
            registry.accept_open(stream_id)
            recv_flow.open_stream(stream_id)
            recv_streams[stream_id] = ReceiveStream(stream_id)
            open_txids[stream_id] = txid
        else:
            raise DecodeError("unexpected initial multi-Stream Frame")

    stream_ids = tuple(sorted(open_txids))
    if len(stream_ids) != 2:
        raise DecodeError("reference multi-Stream endpoint expects exactly two Streams")

    response_frames: list[bytes] = [
        encode_session_credit(0, recv_flow.session_maximum)
    ]
    for stream_id in stream_ids:
        response_frames.append(
            encode_stream_open_ok(stream_id, open_txids[stream_id])
        )
        response_frames.append(
            encode_stream_credit(
                stream_id,
                recv_flow.stream_consumed[stream_id],
                recv_flow.stream_maximum[stream_id],
            )
        )
    carrier.send(*response_frames)

    delivered: dict[int, bytes] = {stream_id: b"" for stream_id in stream_ids}
    data_txids: dict[int, int] = {}
    for frame in carrier.recv():
        if frame.type == int(FrameType.STREAM_CREDIT):
            credit = decode_stream_credit(frame)
            send_flow.update_stream_credit(
                credit.stream_id,
                credit.consumed_offset,
                credit.maximum_offset,
            )
        elif frame.type == int(FrameType.STREAM_DATA):
            data = decode_stream_data(frame)
            if data.stream_id not in recv_streams:
                raise DecodeError("DATA for unopened Stream")
            recv_flow.accept_commit(
                data.stream_id,
                data.offset + len(data.data),
            )
            delivered[data.stream_id] += recv_streams[data.stream_id].insert(
                data.offset,
                data.data,
            )
            data_txids[data.stream_id] = data.transmission_id
        else:
            raise DecodeError("unexpected multi-Stream DATA Frame")

    expected = {
        stream_ids[0]: expected_payload1,
        stream_ids[1]: expected_payload2,
    }
    if delivered != expected:
        raise DecodeError("multi-Stream application payload mismatch")

    carrier.send(
        *(
            encode_transmission_ack(
                stream_id,
                data_txids[stream_id],
                0,
            )
            for stream_id in stream_ids
        )
    )

    client_fins = {}
    for frame in carrier.recv():
        if frame.type != int(FrameType.STREAM_FIN):
            raise DecodeError("expected multi-Stream FIN Frames")
        terminal = decode_stream_fin(frame)
        recv_streams[terminal.stream_id].set_final(terminal.final_offset)
        if not recv_streams[terminal.stream_id].complete:
            raise DecodeError("FIN arrived before complete Stream data")
        client_fins[terminal.stream_id] = terminal

    if set(client_fins) != set(stream_ids):
        raise DecodeError("missing multi-Stream FIN")

    terminal_frames: list[bytes] = []
    server_terminal_txids: set[int] = set()
    for stream_id in stream_ids:
        recv_flow.consume(
            stream_id,
            recv_streams[stream_id].delivered_offset,
        )
        terminal_frames.append(
            encode_transmission_ack(
                stream_id,
                client_fins[stream_id].transmission_id,
                0,
            )
        )

        consumed_tx = ledger.allocate(stream_id, "consumed")
        consumed_frame = encode_stream_consumed(
            stream_id,
            consumed_tx,
            client_fins[stream_id].final_offset,
        )
        ledger.bind_frame(consumed_tx, consumed_frame)
        ledger.note_attempt(
            consumed_tx,
            carrier.identity.carrier_id,
            carrier.identity.generation,
        )
        terminal_frames.append(consumed_frame)
        server_terminal_txids.add(consumed_tx)

        fin_tx = ledger.allocate(stream_id, "fin")
        fin_frame = encode_stream_fin(stream_id, fin_tx, 0)
        ledger.bind_frame(fin_tx, fin_frame)
        ledger.note_attempt(
            fin_tx,
            carrier.identity.carrier_id,
            carrier.identity.generation,
        )
        terminal_frames.append(fin_frame)
        server_terminal_txids.add(fin_tx)

    carrier.send(*terminal_frames)

    peer_consumed = {}
    for frame in carrier.recv():
        if frame.type == int(FrameType.TRANSMISSION_ACK):
            ack = decode_transmission_ack(frame)
            ledger.settle(ack.stream_id, ack.transmission_id)
        elif frame.type == int(FrameType.STREAM_CONSUMED):
            terminal = decode_stream_consumed(frame)
            if terminal.final_offset != 0:
                raise FinalSizeError("client consumed wrong empty server direction")
            peer_consumed[terminal.stream_id] = terminal
        else:
            raise DecodeError("unexpected final multi-Stream client Frame")

    if not server_terminal_txids.issubset(ledger.settled):
        raise DecodeError("server multi-Stream terminal Transmissions not settled")
    if set(peer_consumed) != set(stream_ids):
        raise DecodeError("missing client STREAM_CONSUMED")

    carrier.send(
        *(
            encode_transmission_ack(
                stream_id,
                peer_consumed[stream_id].transmission_id,
                0,
            )
            for stream_id in stream_ids
        )
    )

    for stream_id in stream_ids:
        registry.retire(stream_id)

    return MultiStreamResult(
        session_id=session.session_id,
        stream_ids=(stream_ids[0], stream_ids[1]),
        payloads=(delivered[stream_ids[0]], delivered[stream_ids[1]]),
        transmission_ids=(
            data_txids[stream_ids[0]],
            data_txids[stream_ids[1]],
        ),
        session_committed_bytes=recv_flow.session_committed,
        retired_stream_ids=tuple(sorted(registry.used - registry.active)),
    )


def run_multistream_server(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    expected_payload1: bytes,
    expected_payload2: bytes,
) -> MultiStreamResult:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, port))
        listener.listen(1)
        conn, _ = listener.accept()
        with conn:
            return server_multistream_exchange(
                conn,
                transport_key,
                expected_payload1=expected_payload1,
                expected_payload2=expected_payload2,
            )


def run_multistream_client(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    payload1: bytes,
    payload2: bytes,
) -> MultiStreamResult:
    with socket.create_connection((host, port), timeout=10) as sock:
        return client_multistream_exchange(
            sock,
            transport_key,
            payload1,
            payload2,
        )
