from __future__ import annotations

from dataclasses import dataclass, field
import hmac
import socket

from .codec import decode_frames
from .constants import HandshakeType, MAGIC, SchedulerID, SessionAction, VERSION
from .crypto import RecordCipher, derive_key_schedule
from .endpoint import (
    ClientInitParameters,
    EndpointLimits,
    _validate_server_init,
    build_client_init,
    build_server_init,
    parse_client_init,
)
from .errors import (
    AuthenticationError,
    CarrierConflictError,
    CarrierLostError,
    DecodeError,
    SchedulerMismatchError,
    SessionConflictError,
)
from .tcp import recv_handshake, recv_preface, recv_wire_record
from .varint import encode_varint


@dataclass(frozen=True)
class CarrierIdentity:
    carrier_id: int
    generation: int


@dataclass
class SecureCarrier:
    sock: socket.socket
    identity: CarrierIdentity
    sender: RecordCipher
    receiver: RecordCipher
    local_limits: EndpointLimits
    peer_limits: EndpointLimits
    active: bool = True

    def mark_lost(self) -> None:
        self.active = False

    def send(self, *frames: bytes) -> None:
        if not self.active:
            raise CarrierLostError("Carrier is inactive")
        plaintext = b"".join(frames)
        if not plaintext:
            raise ValueError("Secure Record must contain at least one Frame")
        if len(plaintext) > self.peer_limits.max_record_size:
            raise ValueError("Secure Record exceeds peer MAX_RECORD_SIZE")
        try:
            self.sock.sendall(self.sender.seal(plaintext))
        except OSError as exc:
            self.mark_lost()
            raise CarrierLostError("Carrier transport write failed") from exc

    def recv(self):
        if not self.active:
            raise CarrierLostError("Carrier is inactive")
        try:
            record = recv_wire_record(
                self.sock,
                max_record_size=self.local_limits.max_record_size,
            )
        except (EOFError, OSError) as exc:
            self.mark_lost()
            raise CarrierLostError("Carrier transport read failed") from exc
        return decode_frames(self.receiver.open(record))


@dataclass
class ClientSessionState:
    session_id: bytes
    client_limits: EndpointLimits
    server_limits: EndpointLimits
    scheduler: int
    generations: dict[int, int] = field(default_factory=dict)

    def validate_local_join(self, carrier_id: int, generation: int) -> None:
        current = self.generations.get(carrier_id)
        if current is None:
            if generation != 0:
                raise CarrierConflictError(
                    "first incarnation of a new Carrier ID must use Generation 0"
                )
            return
        if generation <= current:
            raise CarrierConflictError(
                "replacement Carrier Generation must be strictly greater"
            )

    def commit_carrier(self, carrier_id: int, generation: int) -> None:
        self.generations[carrier_id] = generation


@dataclass
class ServerSessionState:
    session_id: bytes
    client_limits: EndpointLimits
    server_limits: EndpointLimits
    scheduler: int
    generations: dict[int, int] = field(default_factory=dict)

    @classmethod
    def from_create(
        cls,
        init: ClientInitParameters,
        server_limits: EndpointLimits,
    ) -> "ServerSessionState":
        if init.action != int(SessionAction.CREATE):
            raise SessionConflictError("first Carrier must use CREATE")
        if init.generation != 0:
            raise CarrierConflictError("CREATE Carrier must use Generation 0")
        state = cls(
            session_id=init.session_id,
            client_limits=init.limits,
            server_limits=server_limits,
            scheduler=init.scheduler,
        )
        state.generations[init.carrier_id] = init.generation
        return state

    def validate_join(self, init: ClientInitParameters) -> None:
        if init.action != int(SessionAction.JOIN):
            raise SessionConflictError("additional Carrier must use JOIN")
        if init.session_id != self.session_id:
            raise SessionConflictError("JOIN SESSION_ID does not match")
        if init.scheduler != self.scheduler:
            raise SchedulerMismatchError("JOIN Scheduler differs from Session")
        if (
            init.limits.max_frame_payload != self.client_limits.max_frame_payload
            or init.limits.max_streams != self.client_limits.max_streams
        ):
            raise SessionConflictError(
                "JOIN changed Session-scoped client receive limits"
            )

        current = self.generations.get(init.carrier_id)
        if current is None:
            if init.generation != 0:
                raise CarrierConflictError(
                    "first incarnation of a new Carrier ID must use Generation 0"
                )
            return
        if init.generation < current:
            raise CarrierConflictError("stale Carrier Generation")
        if init.generation == current:
            raise CarrierConflictError("equal Carrier Generation conflicts")
        # A higher Generation is a valid replacement and supersedes the lower one.

    def validate_server_limits(self, limits: EndpointLimits) -> None:
        if (
            limits.max_frame_payload != self.server_limits.max_frame_payload
            or limits.max_streams != self.server_limits.max_streams
        ):
            raise SessionConflictError(
                "JOIN changed Session-scoped server receive limits"
            )

    def commit_join(self, init: ClientInitParameters) -> None:
        self.generations[init.carrier_id] = init.generation


def _preface() -> bytes:
    return MAGIC + encode_varint(VERSION)


def _validate_join_server_init(
    state: ClientSessionState,
    server_limits: EndpointLimits,
) -> None:
    if (
        server_limits.max_frame_payload
        != state.server_limits.max_frame_payload
        or server_limits.max_streams != state.server_limits.max_streams
    ):
        raise SessionConflictError(
            "JOIN changed Session-scoped server receive limits"
        )


def client_create_carrier(
    sock: socket.socket,
    transport_key: bytes,
    *,
    session_id: bytes,
    client_nonce: bytes,
    client_limits: EndpointLimits = EndpointLimits(),
    carrier_id: int = 1,
    generation: int = 0,
    scheduler: int = int(SchedulerID.AGGREGATE),
) -> tuple[SecureCarrier, ClientSessionState]:
    preface = _preface()
    client_init = build_client_init(
        session_id=session_id,
        client_nonce=client_nonce,
        limits=client_limits,
        carrier_id=carrier_id,
        generation=generation,
        scheduler=scheduler,
        action=int(SessionAction.CREATE),
    )
    sock.sendall(preface + client_init)

    server_init = recv_handshake(sock)
    if server_init.type != int(HandshakeType.SERVER_INIT):
        raise DecodeError("expected SERVER_INIT")
    server_limits = _validate_server_init(server_init.body, scheduler)

    schedule = derive_key_schedule(
        transport_key,
        preface,
        client_init,
        server_init.raw,
    )
    sock.sendall(schedule.client_finished)
    server_finished = recv_handshake(sock)
    if server_finished.type != int(HandshakeType.SERVER_FINISHED):
        raise DecodeError("expected SERVER_FINISHED")
    if not hmac.compare_digest(
        server_finished.body,
        schedule.server_verify_data,
    ):
        raise AuthenticationError("SERVER_FINISHED verification failed")

    identity = CarrierIdentity(carrier_id, generation)
    state = ClientSessionState(
        session_id=session_id,
        client_limits=client_limits,
        server_limits=server_limits,
        scheduler=scheduler,
        generations={carrier_id: generation},
    )
    carrier = SecureCarrier(
        sock=sock,
        identity=identity,
        sender=RecordCipher(
            schedule.client_traffic_key,
            schedule.client_traffic_iv,
        ),
        receiver=RecordCipher(
            schedule.server_traffic_key,
            schedule.server_traffic_iv,
        ),
        local_limits=client_limits,
        peer_limits=server_limits,
    )
    return carrier, state


def server_create_carrier(
    sock: socket.socket,
    transport_key: bytes,
    *,
    server_nonce: bytes,
    server_limits: EndpointLimits = EndpointLimits(),
) -> tuple[SecureCarrier, ServerSessionState]:
    preface = recv_preface(sock)
    client_init_message = recv_handshake(sock)
    if client_init_message.type != int(HandshakeType.CLIENT_INIT):
        raise DecodeError("expected CLIENT_INIT")
    init = parse_client_init(client_init_message.body)
    if init.action != int(SessionAction.CREATE):
        raise SessionConflictError("first Carrier must CREATE the Session")

    server_init = build_server_init(
        server_nonce=server_nonce,
        limits=server_limits,
        scheduler=init.scheduler,
    )
    sock.sendall(server_init)

    schedule = derive_key_schedule(
        transport_key,
        preface.raw,
        client_init_message.raw,
        server_init,
    )
    client_finished = recv_handshake(sock)
    if client_finished.type != int(HandshakeType.CLIENT_FINISHED):
        raise DecodeError("expected CLIENT_FINISHED")
    if not hmac.compare_digest(
        client_finished.body,
        schedule.client_verify_data,
    ):
        raise AuthenticationError("CLIENT_FINISHED verification failed")

    # The Session is committed only after CLIENT_FINISHED authenticates.
    state = ServerSessionState.from_create(init, server_limits)
    sock.sendall(schedule.server_finished)

    carrier = SecureCarrier(
        sock=sock,
        identity=CarrierIdentity(init.carrier_id, init.generation),
        sender=RecordCipher(
            schedule.server_traffic_key,
            schedule.server_traffic_iv,
        ),
        receiver=RecordCipher(
            schedule.client_traffic_key,
            schedule.client_traffic_iv,
        ),
        local_limits=server_limits,
        peer_limits=init.limits,
    )
    return carrier, state


def client_join_carrier(
    sock: socket.socket,
    transport_key: bytes,
    state: ClientSessionState,
    *,
    carrier_id: int,
    generation: int,
    client_nonce: bytes,
    carrier_receive_limits: EndpointLimits | None = None,
) -> SecureCarrier:
    state.validate_local_join(carrier_id, generation)

    local_limits = carrier_receive_limits or state.client_limits
    if (
        local_limits.max_frame_payload != state.client_limits.max_frame_payload
        or local_limits.max_streams != state.client_limits.max_streams
    ):
        raise SessionConflictError(
            "JOIN changed Session-scoped client receive limits"
        )

    preface = _preface()
    client_init = build_client_init(
        session_id=state.session_id,
        client_nonce=client_nonce,
        limits=local_limits,
        carrier_id=carrier_id,
        generation=generation,
        scheduler=state.scheduler,
        action=int(SessionAction.JOIN),
    )
    sock.sendall(preface + client_init)

    server_init = recv_handshake(sock)
    if server_init.type != int(HandshakeType.SERVER_INIT):
        raise DecodeError("expected SERVER_INIT")
    server_limits = _validate_server_init(server_init.body, state.scheduler)
    _validate_join_server_init(state, server_limits)

    schedule = derive_key_schedule(
        transport_key,
        preface,
        client_init,
        server_init.raw,
    )
    sock.sendall(schedule.client_finished)
    server_finished = recv_handshake(sock)
    if server_finished.type != int(HandshakeType.SERVER_FINISHED):
        raise DecodeError("expected SERVER_FINISHED")
    if not hmac.compare_digest(
        server_finished.body,
        schedule.server_verify_data,
    ):
        raise AuthenticationError("SERVER_FINISHED verification failed")

    state.commit_carrier(carrier_id, generation)
    return SecureCarrier(
        sock=sock,
        identity=CarrierIdentity(carrier_id, generation),
        sender=RecordCipher(
            schedule.client_traffic_key,
            schedule.client_traffic_iv,
        ),
        receiver=RecordCipher(
            schedule.server_traffic_key,
            schedule.server_traffic_iv,
        ),
        local_limits=local_limits,
        peer_limits=server_limits,
    )


def server_join_carrier(
    sock: socket.socket,
    transport_key: bytes,
    state: ServerSessionState,
    *,
    server_nonce: bytes,
    carrier_receive_limits: EndpointLimits | None = None,
) -> SecureCarrier:
    preface = recv_preface(sock)
    client_init_message = recv_handshake(sock)
    if client_init_message.type != int(HandshakeType.CLIENT_INIT):
        raise DecodeError("expected CLIENT_INIT")
    init = parse_client_init(client_init_message.body)
    state.validate_join(init)

    local_limits = carrier_receive_limits or state.server_limits
    state.validate_server_limits(local_limits)

    server_init = build_server_init(
        server_nonce=server_nonce,
        limits=local_limits,
        scheduler=state.scheduler,
    )
    sock.sendall(server_init)

    schedule = derive_key_schedule(
        transport_key,
        preface.raw,
        client_init_message.raw,
        server_init,
    )
    client_finished = recv_handshake(sock)
    if client_finished.type != int(HandshakeType.CLIENT_FINISHED):
        raise DecodeError("expected CLIENT_FINISHED")
    if not hmac.compare_digest(
        client_finished.body,
        schedule.client_verify_data,
    ):
        raise AuthenticationError("CLIENT_FINISHED verification failed")

    # Only now does the new Carrier become authenticated Session state.
    state.commit_join(init)
    sock.sendall(schedule.server_finished)

    return SecureCarrier(
        sock=sock,
        identity=CarrierIdentity(init.carrier_id, init.generation),
        sender=RecordCipher(
            schedule.server_traffic_key,
            schedule.server_traffic_iv,
        ),
        receiver=RecordCipher(
            schedule.client_traffic_key,
            schedule.client_traffic_iv,
        ),
        local_limits=local_limits,
        peer_limits=init.limits,
    )
