from __future__ import annotations

from dataclasses import dataclass
import hmac
import os
import socket

from .codec import (
    decode_frames,
    decode_parameters,
    encode_handshake_message,
    encode_parameters,
    encode_ping,
    encode_pong,
    parameter_bytes,
    parameter_map,
    parameter_varint,
    parameter_varint_value,
    decode_token_frame,
)
from .constants import (
    FrameType,
    HandshakeType,
    MAGIC,
    ParameterType,
    SchedulerID,
    SessionAction,
    VERSION,
)
from .crypto import RecordCipher, derive_key_schedule
from .errors import AuthenticationError, DecodeError
from .tcp import recv_handshake, recv_preface, recv_wire_record
from .varint import decode_varint, encode_varint


@dataclass(frozen=True)
class EndpointLimits:
    max_frame_payload: int = 32768
    max_record_size: int = 65536
    max_streams: int = 32


@dataclass(frozen=True)
class ExchangeResult:
    session_id: bytes
    carrier_id: int
    scheduler: int
    ping_token: int


@dataclass(frozen=True)
class ClientInitParameters:
    session_id: bytes
    action: int
    carrier_id: int
    generation: int
    client_nonce: bytes
    limits: EndpointLimits
    scheduler: int
    path_capacity: tuple[int, int] | None = None


def _preface() -> bytes:
    return MAGIC + encode_varint(VERSION)


def build_client_init(
    *,
    session_id: bytes,
    client_nonce: bytes,
    limits: EndpointLimits,
    carrier_id: int = 1,
    generation: int = 0,
    scheduler: int = int(SchedulerID.AGGREGATE),
    action: int = int(SessionAction.CREATE),
    path_capacity: tuple[int, int] | None = None,
) -> bytes:
    if len(session_id) != 16 or session_id == b"\x00" * 16:
        raise ValueError("SESSION_ID must be 16 non-zero random octets")
    if len(client_nonce) != 32:
        raise ValueError("CLIENT_NONCE must be 32 octets")
    if scheduler not in {int(value) for value in SchedulerID}:
        raise ValueError("unsupported Scheduler ID")
    if scheduler == int(SchedulerID.WEIGHTED):
        if path_capacity is None:
            raise ValueError("WEIGHTED requires PATH_CAPACITY")
        downlink, uplink = path_capacity
        if not 1 <= downlink <= 65535 or not 0 <= uplink <= 65535:
            raise ValueError("invalid PATH_CAPACITY units")
    elif path_capacity is not None:
        raise ValueError("PATH_CAPACITY is valid only for WEIGHTED")

    parameters = [
        parameter_bytes(ParameterType.SESSION_ID, session_id),
        parameter_varint(ParameterType.SESSION_ACTION, action),
        parameter_varint(ParameterType.CARRIER_ID, carrier_id),
        parameter_varint(ParameterType.CARRIER_GENERATION, generation),
        parameter_bytes(ParameterType.CLIENT_NONCE, client_nonce),
        parameter_varint(ParameterType.MAX_FRAME_PAYLOAD, limits.max_frame_payload),
        parameter_varint(ParameterType.MAX_RECORD_SIZE, limits.max_record_size),
        parameter_varint(ParameterType.MAX_STREAMS, limits.max_streams),
        parameter_varint(ParameterType.SCHEDULER, scheduler),
    ]
    if path_capacity is not None:
        downlink, uplink = path_capacity
        parameters.append(
            parameter_bytes(
                ParameterType.PATH_CAPACITY,
                encode_varint(downlink) + encode_varint(uplink),
            )
        )
    return encode_handshake_message(
        HandshakeType.CLIENT_INIT,
        encode_parameters(parameters),
    )


def build_server_init(
    *,
    server_nonce: bytes,
    limits: EndpointLimits,
    scheduler: int,
) -> bytes:
    if len(server_nonce) != 32:
        raise ValueError("SERVER_NONCE must be 32 octets")
    parameters = [
        parameter_bytes(ParameterType.SERVER_NONCE, server_nonce),
        parameter_varint(ParameterType.MAX_FRAME_PAYLOAD, limits.max_frame_payload),
        parameter_varint(ParameterType.MAX_RECORD_SIZE, limits.max_record_size),
        parameter_varint(ParameterType.MAX_STREAMS, limits.max_streams),
        parameter_varint(ParameterType.SCHEDULER, scheduler),
    ]
    return encode_handshake_message(
        HandshakeType.SERVER_INIT,
        encode_parameters(parameters),
    )


def parse_client_init(raw_message_body: bytes) -> ClientInitParameters:
    params = parameter_map(decode_parameters(raw_message_body))
    required = {
        int(ParameterType.SESSION_ID),
        int(ParameterType.SESSION_ACTION),
        int(ParameterType.CARRIER_ID),
        int(ParameterType.CARRIER_GENERATION),
        int(ParameterType.CLIENT_NONCE),
        int(ParameterType.MAX_FRAME_PAYLOAD),
        int(ParameterType.MAX_RECORD_SIZE),
        int(ParameterType.MAX_STREAMS),
        int(ParameterType.SCHEDULER),
    }
    allowed = required | {int(ParameterType.PATH_CAPACITY)}
    if not required.issubset(params) or not set(params).issubset(allowed):
        raise DecodeError("reference endpoint requires the Draft 03 Core parameter set")

    session_id = params[int(ParameterType.SESSION_ID)].value
    if len(session_id) != 16 or session_id == b"\x00" * 16:
        raise DecodeError("invalid SESSION_ID")

    action = parameter_varint_value(params[int(ParameterType.SESSION_ACTION)])
    if action not in (int(SessionAction.CREATE), int(SessionAction.JOIN)):
        raise DecodeError("invalid SESSION_ACTION")

    carrier_id = parameter_varint_value(params[int(ParameterType.CARRIER_ID)])
    if not 1 <= carrier_id <= 8:
        raise DecodeError("invalid CARRIER_ID")
    generation = parameter_varint_value(params[int(ParameterType.CARRIER_GENERATION)])
    if action == int(SessionAction.CREATE) and generation != 0:
        raise DecodeError("CREATE Carrier must use Generation 0")

    client_nonce = params[int(ParameterType.CLIENT_NONCE)].value
    if len(client_nonce) != 32:
        raise DecodeError("invalid CLIENT_NONCE")

    limits = EndpointLimits(
        max_frame_payload=parameter_varint_value(params[int(ParameterType.MAX_FRAME_PAYLOAD)]),
        max_record_size=parameter_varint_value(params[int(ParameterType.MAX_RECORD_SIZE)]),
        max_streams=parameter_varint_value(params[int(ParameterType.MAX_STREAMS)]),
    )
    if not 1 <= limits.max_frame_payload <= 32768:
        raise DecodeError("invalid MAX_FRAME_PAYLOAD")
    if not 1024 <= limits.max_record_size <= 65536:
        raise DecodeError("invalid MAX_RECORD_SIZE")
    if not 1 <= limits.max_streams <= 2048:
        raise DecodeError("invalid MAX_STREAMS")

    scheduler = parameter_varint_value(params[int(ParameterType.SCHEDULER)])
    if scheduler not in {int(value) for value in SchedulerID}:
        raise DecodeError("unsupported Scheduler ID")

    capacity_parameter = params.get(int(ParameterType.PATH_CAPACITY))
    path_capacity = None
    if scheduler == int(SchedulerID.WEIGHTED):
        if capacity_parameter is None:
            raise DecodeError("WEIGHTED requires PATH_CAPACITY")
        offset = 0
        downlink, offset = decode_varint(capacity_parameter.value, offset)
        uplink, offset = decode_varint(capacity_parameter.value, offset)
        if offset != len(capacity_parameter.value):
            raise DecodeError("invalid PATH_CAPACITY encoding")
        if not 1 <= downlink <= 65535 or not 0 <= uplink <= 65535:
            raise DecodeError("invalid PATH_CAPACITY units")
        path_capacity = (downlink, uplink)
    elif capacity_parameter is not None:
        raise DecodeError("PATH_CAPACITY is valid only for WEIGHTED")

    return ClientInitParameters(
        session_id=session_id,
        action=action,
        carrier_id=carrier_id,
        generation=generation,
        client_nonce=client_nonce,
        limits=limits,
        scheduler=scheduler,
        path_capacity=path_capacity,
    )


def _validate_client_init(raw_message_body: bytes) -> tuple[bytes, int, int]:
    parsed = parse_client_init(raw_message_body)
    if parsed.action != int(SessionAction.CREATE):
        raise DecodeError("reference endpoint requires CREATE")
    return parsed.session_id, parsed.carrier_id, parsed.scheduler


def _validate_server_init(raw_message_body: bytes, expected_scheduler: int) -> EndpointLimits:
    params = parameter_map(decode_parameters(raw_message_body))
    required = {
        int(ParameterType.SERVER_NONCE),
        int(ParameterType.MAX_FRAME_PAYLOAD),
        int(ParameterType.MAX_RECORD_SIZE),
        int(ParameterType.MAX_STREAMS),
        int(ParameterType.SCHEDULER),
    }
    if set(params) != required:
        raise DecodeError("unexpected SERVER_INIT parameter set")
    if len(params[int(ParameterType.SERVER_NONCE)].value) != 32:
        raise DecodeError("invalid SERVER_NONCE")

    scheduler = parameter_varint_value(params[int(ParameterType.SCHEDULER)])
    if scheduler != expected_scheduler:
        raise DecodeError("SCHEDULER_MISMATCH")

    limits = EndpointLimits(
        max_frame_payload=parameter_varint_value(params[int(ParameterType.MAX_FRAME_PAYLOAD)]),
        max_record_size=parameter_varint_value(params[int(ParameterType.MAX_RECORD_SIZE)]),
        max_streams=parameter_varint_value(params[int(ParameterType.MAX_STREAMS)]),
    )
    if not 1 <= limits.max_frame_payload <= 32768:
        raise DecodeError("invalid server MAX_FRAME_PAYLOAD")
    if not 1024 <= limits.max_record_size <= 65536:
        raise DecodeError("invalid server MAX_RECORD_SIZE")
    if not 1 <= limits.max_streams <= 2048:
        raise DecodeError("invalid server MAX_STREAMS")
    return limits


def client_exchange(
    sock: socket.socket,
    transport_key: bytes,
    *,
    token: int = 1,
    session_id: bytes | None = None,
    client_nonce: bytes | None = None,
    limits: EndpointLimits = EndpointLimits(),
) -> ExchangeResult:
    session_id = session_id or os.urandom(16)
    client_nonce = client_nonce or os.urandom(32)
    scheduler = int(SchedulerID.AGGREGATE)

    preface = _preface()
    client_init = build_client_init(
        session_id=session_id,
        client_nonce=client_nonce,
        limits=limits,
        scheduler=scheduler,
    )
    sock.sendall(preface + client_init)

    server_init_msg = recv_handshake(sock)
    if server_init_msg.type != int(HandshakeType.SERVER_INIT):
        raise DecodeError("expected SERVER_INIT")
    server_limits = _validate_server_init(server_init_msg.body, scheduler)

    schedule = derive_key_schedule(
        transport_key,
        preface,
        client_init,
        server_init_msg.raw,
    )
    sock.sendall(schedule.client_finished)

    server_finished = recv_handshake(sock)
    if server_finished.type != int(HandshakeType.SERVER_FINISHED):
        raise DecodeError("expected SERVER_FINISHED")
    if not hmac.compare_digest(server_finished.body, schedule.server_verify_data):
        raise AuthenticationError("SERVER_FINISHED verification failed")

    sender = RecordCipher(schedule.client_traffic_key, schedule.client_traffic_iv)
    receiver = RecordCipher(schedule.server_traffic_key, schedule.server_traffic_iv)

    sock.sendall(sender.seal(encode_ping(token)))
    record = recv_wire_record(sock, max_record_size=limits.max_record_size)
    plaintext = receiver.open(record)
    frames = decode_frames(plaintext)
    if len(frames) != 1 or frames[0].type != int(FrameType.PONG):
        raise DecodeError("expected one PONG Frame")
    if decode_token_frame(frames[0]) != token:
        raise DecodeError("PONG token mismatch")

    return ExchangeResult(
        session_id=session_id,
        carrier_id=1,
        scheduler=scheduler,
        ping_token=token,
    )


def server_exchange(
    sock: socket.socket,
    transport_key: bytes,
    *,
    server_nonce: bytes | None = None,
    limits: EndpointLimits = EndpointLimits(),
) -> ExchangeResult:
    preface = recv_preface(sock)
    client_init_msg = recv_handshake(sock)
    if client_init_msg.type != int(HandshakeType.CLIENT_INIT):
        raise DecodeError("expected CLIENT_INIT")
    session_id, carrier_id, scheduler = _validate_client_init(client_init_msg.body)

    server_nonce = server_nonce or os.urandom(32)
    server_init = build_server_init(
        server_nonce=server_nonce,
        limits=limits,
        scheduler=scheduler,
    )
    sock.sendall(server_init)

    schedule = derive_key_schedule(
        transport_key,
        preface.raw,
        client_init_msg.raw,
        server_init,
    )

    client_finished = recv_handshake(sock)
    if client_finished.type != int(HandshakeType.CLIENT_FINISHED):
        raise DecodeError("expected CLIENT_FINISHED")
    if not hmac.compare_digest(client_finished.body, schedule.client_verify_data):
        raise AuthenticationError("CLIENT_FINISHED verification failed")

    sock.sendall(schedule.server_finished)

    receiver = RecordCipher(schedule.client_traffic_key, schedule.client_traffic_iv)
    sender = RecordCipher(schedule.server_traffic_key, schedule.server_traffic_iv)

    record = recv_wire_record(sock, max_record_size=limits.max_record_size)
    plaintext = receiver.open(record)
    frames = decode_frames(plaintext)
    if len(frames) != 1 or frames[0].type != int(FrameType.PING):
        raise DecodeError("expected one PING Frame")
    token = decode_token_frame(frames[0])
    sock.sendall(sender.seal(encode_pong(token)))

    return ExchangeResult(
        session_id=session_id,
        carrier_id=carrier_id,
        scheduler=scheduler,
        ping_token=token,
    )


def run_server(host: str, port: int, transport_key: bytes) -> ExchangeResult:
    with socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, port))
        listener.listen(1)
        conn, _ = listener.accept()
        with conn:
            return server_exchange(conn, transport_key)


def run_client(host: str, port: int, transport_key: bytes, *, token: int = 1) -> ExchangeResult:
    with socket.create_connection((host, port), timeout=10) as sock:
        return client_exchange(sock, transport_key, token=token)
