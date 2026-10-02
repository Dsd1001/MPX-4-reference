from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from .carrier import (
    ClientSessionState,
    SecureCarrier,
    ServerSessionState,
)
from .close import SessionCloseController
from .codec import (
    Frame,
    ResetStream,
    StopSending,
    StreamData,
    StreamOpenReject,
    StreamTerminal,
    TransmissionAck,
    decode_frames,
    decode_reset_stream,
    decode_session_credit,
    decode_stop_sending,
    decode_stream_consumed,
    decode_stream_credit,
    decode_stream_data,
    decode_stream_fin,
    decode_stream_open,
    decode_stream_open_ok,
    decode_stream_open_reject,
    decode_token_frame,
    decode_transmission_ack,
    encode_pong,
    encode_reset_stream,
    encode_session_credit,
    encode_stop_sending,
    encode_stream_consumed,
    encode_stream_credit,
    encode_stream_data,
    encode_stream_fin,
    encode_stream_open,
    encode_transmission_ack,
)
from .constants import ErrorCode, FrameType
from .errors import (
    DecodeError,
    FinalSizeError,
    SessionConflictError,
    StreamStateError,
    TransmissionIDError,
)
from .opening import ClientOpeningState
from .reliability import ReliabilityLoop, ScheduledAttempt
from .scheduler import scheduler_for_id
from .stream import (
    ClientStreamRegistry,
    ReceiveFlow,
    ReceiveStream,
    SendFlow,
    TransmissionLedger,
)
from .terminal import ServerTerminalStateMachine, TerminalResult


Role = Literal["client", "server"]
SessionState = ClientSessionState | ServerSessionState


@dataclass
class ClientActiveStream:
    stream_id: int
    receive: ReceiveStream
    send_offset: int = 0

    local_terminal_kind: str | None = None
    local_terminal_transmission_id: int | None = None
    local_final_offset: int | None = None
    local_error_code: int | None = None
    local_terminal_acked: bool = False

    peer_terminal_transmission_id: int | None = None
    peer_error_code: int | None = None
    peer_accounting_released: bool = False

    local_consumed_transmission_id: int | None = None
    local_consumed_acked: bool = False

    stop_transmission_id: int | None = None
    stop_error_code: int | None = None


@dataclass(frozen=True)
class EngineEvents:
    deliveries: tuple[tuple[int, bytes], ...] = ()
    opened: tuple[int, ...] = ()
    rejected: tuple[int, ...] = ()
    terminal: tuple[tuple[int, str], ...] = ()
    carrier_closed: tuple[tuple[int, int], ...] = ()
    session_closed: bool = False


class ReferenceSessionEngine:
    """Unified Draft 03 Session orchestration layer.

    The engine intentionally reuses the lower-level components rather than
    duplicating their protocol logic. It owns one Session-wide Transmission
    ledger, one scheduler/reliability loop, shared flow-control state, all
    authenticated Carriers, Stream opening/terminal state, and graceful-close
    state.
    """

    def __init__(
        self,
        role: Role,
        state: SessionState,
        *,
        retransmit_after_s: float = 0.250,
    ) -> None:
        if role not in {"client", "server"}:
            raise ValueError("role must be client or server")
        self.role = role
        self.state = state

        self.ledger = TransmissionLedger()
        self.send_flow = SendFlow()
        self.receive_flow = ReceiveFlow()

        self.scheduler = scheduler_for_id(state.scheduler)
        self.reliability = ReliabilityLoop(
            self.ledger,
            self.scheduler,
            retransmit_after_s=retransmit_after_s,
        )
        self.close = SessionCloseController(state)

        self.openings: dict[int, ClientOpeningState] = {}
        self.client_streams: dict[int, ClientActiveStream] = {}
        self.retired_streams: set[int] = set()

        if role == "client":
            self.client_registry = ClientStreamRegistry(
                state.server_limits.max_streams
            )
            self.server_terminal = None
        else:
            self.client_registry = None
            self.server_terminal = ServerTerminalStateMachine(
                max_streams=state.server_limits.max_streams,
                receive_flow=self.receive_flow,
                ledger=self.ledger,
            )

    @classmethod
    def client(
        cls,
        state: ClientSessionState,
        **kwargs,
    ) -> "ReferenceSessionEngine":
        return cls("client", state, **kwargs)

    @classmethod
    def server(
        cls,
        state: ServerSessionState,
        **kwargs,
    ) -> "ReferenceSessionEngine":
        return cls("server", state, **kwargs)

    @property
    def carriers(self) -> dict[tuple[int, int], SecureCarrier]:
        return self.close.carriers

    def register_carrier(
        self,
        carrier: SecureCarrier,
        *,
        latest_rtt_s: float = 0.050,
        delivery_rate_bps: float = 1_000_000.0,
        configured_capacity_bps: float | None = None,
        role: str = "active",
    ) -> None:
        self.close.add_carrier(carrier)
        self.scheduler.register(
            carrier,
            latest_rtt_s=latest_rtt_s,
            delivery_rate_bps=delivery_rate_bps,
            configured_capacity_bps=configured_capacity_bps,
            role=role,
        )

    def set_carrier_role(
        self,
        identity: tuple[int, int],
        role: str,
    ) -> None:
        self.scheduler.set_role(identity, role)

    def mark_carrier_lost(
        self,
        identity: tuple[int, int],
    ) -> None:
        self.scheduler.mark_failure(identity)

    def poll_reliability(self) -> tuple[ScheduledAttempt, ...]:
        return self.reliability.poll()

    def _carrier_identity(
        self,
        carrier: SecureCarrier,
    ) -> tuple[int, int]:
        return (
            carrier.identity.carrier_id,
            carrier.identity.generation,
        )

    def _send_unreliable(
        self,
        wire: bytes,
        *,
        carrier: SecureCarrier | None = None,
    ) -> tuple[int, int]:
        target = carrier
        if target is None:
            target = self.scheduler.choose(len(wire)).carrier
        target.send(wire)
        return self._carrier_identity(target)

    def advertise_session_credit(
        self,
        *,
        carrier: SecureCarrier | None = None,
    ) -> tuple[int, int]:
        return self._send_unreliable(
            encode_session_credit(
                self.receive_flow.session_consumed,
                self.receive_flow.session_maximum,
            ),
            carrier=carrier,
        )

    def advertise_stream_credit(
        self,
        stream_id: int,
        *,
        carrier: SecureCarrier | None = None,
    ) -> tuple[int, int]:
        if stream_id not in self.receive_flow.stream_maximum:
            raise StreamStateError("Stream has no receive-credit state")
        return self._send_unreliable(
            encode_stream_credit(
                stream_id,
                self.receive_flow.stream_consumed[stream_id],
                self.receive_flow.stream_maximum[stream_id],
            ),
            carrier=carrier,
        )

    def _pending_txid_from_wire(self, wire: bytes) -> int | None:
        frames = decode_frames(wire)
        if len(frames) != 1:
            raise DecodeError("generated response must contain one Frame")
        frame = frames[0]
        if frame.type == int(FrameType.RESET_STREAM):
            return decode_reset_stream(frame).transmission_id
        if frame.type == int(FrameType.STREAM_CONSUMED):
            return decode_stream_consumed(frame).transmission_id
        if frame.type == int(FrameType.STREAM_FIN):
            return decode_stream_fin(frame).transmission_id
        if frame.type == int(FrameType.STOP_SENDING):
            return decode_stop_sending(frame).transmission_id
        if frame.type == int(FrameType.STREAM_DATA):
            return decode_stream_data(frame).transmission_id
        if frame.type == int(FrameType.STREAM_OPEN):
            return decode_stream_open(frame)[1]
        return None

    def _transmit_generated_reliable(self, wire: bytes) -> None:
        txid = self._pending_txid_from_wire(wire)
        if txid is None:
            raise TransmissionIDError("generated reliable Frame lacks Transmission ID")
        pending = self.ledger.pending.get(txid)
        if pending is None:
            return
        if not pending.attempts:
            self.reliability.transmit_new(txid)

    def _dispatch_terminal_result(
        self,
        incoming: SecureCarrier,
        result: TerminalResult,
    ) -> None:
        for wire in result.responses:
            frames = decode_frames(wire)
            if len(frames) != 1:
                raise DecodeError("terminal response contains multiple Frames")
            frame = frames[0]
            if frame.type in {
                int(FrameType.TRANSMISSION_ACK),
                int(FrameType.STREAM_OPEN_OK),
                int(FrameType.STREAM_OPEN_REJECT),
                int(FrameType.STREAM_CREDIT),
                int(FrameType.SESSION_CREDIT),
            }:
                incoming.send(wire)
                continue
            if frame.type in {
                int(FrameType.RESET_STREAM),
                int(FrameType.STREAM_CONSUMED),
                int(FrameType.STREAM_FIN),
                int(FrameType.STOP_SENDING),
            }:
                self._transmit_generated_reliable(wire)
                continue
            incoming.send(wire)

    def open_stream(self) -> tuple[int, ScheduledAttempt]:
        if self.role != "client":
            raise StreamStateError("only the Client opens Draft 03 Streams")
        self.state.ensure_stream_creation_allowed()
        assert self.client_registry is not None

        stream_id = self.client_registry.allocate()
        txid = self.ledger.allocate(stream_id, "open")
        wire = encode_stream_open(stream_id, txid)
        self.ledger.bind_frame(txid, wire)
        self.openings[stream_id] = ClientOpeningState(
            stream_id=stream_id,
            open_transmission_id=txid,
            ledger=self.ledger,
            send_flow=self.send_flow,
        )
        return stream_id, self.reliability.transmit_new(txid)

    def _client_active(self, stream_id: int) -> ClientActiveStream:
        state = self.client_streams.get(stream_id)
        if state is None:
            raise StreamStateError("Stream is not OPEN")
        return state

    def _activate_client_stream(
        self,
        opening: ClientOpeningState,
    ) -> ClientActiveStream:
        stream_id = opening.stream_id
        self.receive_flow.open_stream(stream_id)

        peer_released = False
        if opening.receive.final_offset is not None:
            self.receive_flow.accept_commit(
                stream_id,
                opening.receive.final_offset,
            )
            if opening.receive.terminal_kind == "reset":
                self.receive_flow.consume(
                    stream_id,
                    opening.receive.final_offset,
                )
                peer_released = True

        active = ClientActiveStream(
            stream_id=stream_id,
            receive=opening.receive,
            local_terminal_kind=(
                "reset"
                if opening.local_reset_transmission_id is not None
                else None
            ),
            local_terminal_transmission_id=opening.local_reset_transmission_id,
            local_final_offset=(
                0 if opening.local_reset_transmission_id is not None else None
            ),
            local_error_code=opening.local_reset_error_code,
            local_terminal_acked=opening.local_reset_acked,
            peer_terminal_transmission_id=opening.peer_terminal_transmission_id,
            peer_error_code=opening.peer_terminal_error_code,
            peer_accounting_released=peer_released,
            stop_transmission_id=opening.stop_transmission_id,
            stop_error_code=opening.stop_error_code,
        )
        self.client_streams[stream_id] = active
        del self.openings[stream_id]
        return active

    def send_data(
        self,
        stream_id: int,
        data: bytes,
    ) -> ScheduledAttempt:
        if not data:
            raise ValueError("STREAM_DATA payload must not be empty")
        self.state.ensure_stream_creation_allowed()

        if self.role == "client":
            active = self._client_active(stream_id)
            if active.local_terminal_kind is not None:
                raise StreamStateError("cannot send DATA after terminal state")
            offset = active.send_offset
            end = offset + len(data)
            self.send_flow.commit(stream_id, end)
            active.send_offset = end
        else:
            assert self.server_terminal is not None
            state = self.server_terminal.active.get(stream_id)
            if state is None:
                raise StreamStateError("cannot send DATA on unknown Stream")
            if state.local_terminal_kind is not None:
                raise StreamStateError("cannot send DATA after terminal state")
            offset = state.local_send_committed
            end = offset + len(data)
            self.send_flow.commit(stream_id, end)
            self.server_terminal.note_local_send_commit(stream_id, end)

        txid = self.ledger.allocate(stream_id, "data")
        wire = encode_stream_data(stream_id, offset, txid, data)
        self.ledger.bind_frame(txid, wire)
        return self.reliability.transmit_new(txid)

    def send_fin(self, stream_id: int) -> ScheduledAttempt:
        if self.role == "server":
            assert self.server_terminal is not None
            wire = self.server_terminal.start_local_fin(stream_id)
            txid = decode_stream_fin(decode_frames(wire)[0]).transmission_id
            return self.reliability.transmit_new(txid)

        active = self._client_active(stream_id)
        if active.local_terminal_kind is not None:
            raise StreamStateError("local send direction is already terminal")
        txid = self.ledger.allocate(stream_id, "fin")
        wire = encode_stream_fin(stream_id, txid, active.send_offset)
        self.ledger.bind_frame(txid, wire)
        active.local_terminal_kind = "fin"
        active.local_terminal_transmission_id = txid
        active.local_final_offset = active.send_offset
        return self.reliability.transmit_new(txid)

    def send_reset(
        self,
        stream_id: int,
        error_code: int = int(ErrorCode.NO_ERROR),
    ) -> ScheduledAttempt | None:
        if self.role == "server":
            assert self.server_terminal is not None
            wire = self.server_terminal.start_local_reset(
                stream_id,
                error_code,
            )
            reset = decode_reset_stream(decode_frames(wire)[0])
            pending = self.ledger.pending.get(reset.transmission_id)
            if pending is None or pending.attempts:
                return None
            return self.reliability.transmit_new(reset.transmission_id)

        active = self._client_active(stream_id)
        if active.local_terminal_kind == "reset":
            txid = active.local_terminal_transmission_id
            if txid is None:
                raise StreamStateError("RESET state lacks Transmission ID")
            pending = self.ledger.pending.get(txid)
            if pending is None or pending.attempts:
                return None
            return self.reliability.transmit_new(txid)

        final_offset = (
            active.local_final_offset
            if active.local_final_offset is not None
            else active.send_offset
        )
        txid = self.ledger.allocate(stream_id, "reset")
        wire = encode_reset_stream(
            stream_id,
            txid,
            final_offset,
            error_code,
        )
        self.ledger.bind_frame(txid, wire)
        active.local_terminal_kind = "reset"
        active.local_terminal_transmission_id = txid
        active.local_final_offset = final_offset
        active.local_error_code = error_code
        active.local_terminal_acked = False
        return self.reliability.transmit_new(txid)

    def send_stop(
        self,
        stream_id: int,
        error_code: int = int(ErrorCode.NO_ERROR),
    ) -> ScheduledAttempt:
        txid = self.ledger.allocate(stream_id, "stop")
        wire = encode_stop_sending(stream_id, txid, error_code)
        self.ledger.bind_frame(txid, wire)
        return self.reliability.transmit_new(txid)

    def release_receive(self, stream_id: int) -> ScheduledAttempt | None:
        if self.role == "server":
            assert self.server_terminal is not None
            wire = self.server_terminal.release_receive(stream_id)
            if wire is None:
                return None
            txid = decode_stream_consumed(decode_frames(wire)[0]).transmission_id
            pending = self.ledger.pending.get(txid)
            if pending is None or pending.attempts:
                return None
            return self.reliability.transmit_new(txid)

        active = self._client_active(stream_id)
        if active.receive.final_offset is None:
            raise StreamStateError("receive direction has no final size")
        final_offset = active.receive.final_offset

        if active.receive.terminal_kind == "reset":
            if not active.peer_accounting_released:
                self.receive_flow.consume(stream_id, final_offset)
                active.peer_accounting_released = True
            self._client_maybe_retire(stream_id)
            return None

        if not active.receive.complete:
            raise StreamStateError("cannot consume incomplete FIN direction")
        if not active.peer_accounting_released:
            self.receive_flow.consume(stream_id, final_offset)
            active.peer_accounting_released = True

        if active.local_consumed_transmission_id is not None:
            txid = active.local_consumed_transmission_id
            pending = self.ledger.pending.get(txid)
            if pending is None or pending.attempts:
                return None
            return self.reliability.transmit_new(txid)

        txid = self.ledger.allocate(stream_id, "consumed")
        wire = encode_stream_consumed(stream_id, txid, final_offset)
        self.ledger.bind_frame(txid, wire)
        active.local_consumed_transmission_id = txid
        return self.reliability.transmit_new(txid)

    def close_carrier(
        self,
        identity: tuple[int, int],
        *,
        error_code: int = int(ErrorCode.NO_ERROR),
        reason: str = "",
    ) -> None:
        carrier = self.carriers.get(identity)
        if carrier is None:
            raise KeyError(identity)
        self.close.send_carrier_close(
            carrier,
            error_code=error_code,
            reason=reason,
        )

    def close_session(
        self,
        *,
        error_code: int = int(ErrorCode.NO_ERROR),
        reason: str = "",
    ) -> int:
        _, sent = self.close.send_session_close(
            error_code=error_code,
            reason=reason,
        )
        return sent

    def _settle_open_scheduler(
        self,
        opening: ClientOpeningState,
        ack_identity: tuple[int, int],
    ) -> None:
        pending = self.ledger.pending.get(opening.open_transmission_id)
        delivered = len(pending.wire_frame or b"") if pending is not None else 0
        self.scheduler.settle(
            opening.open_transmission_id,
            ack_identity=ack_identity,
            ack_at=self.reliability.clock(),
            delivered_bytes=delivered,
        )

    def _client_handle_open_ok(
        self,
        carrier: SecureCarrier,
        frame: Frame,
    ) -> int:
        stream_id, txid = decode_stream_open_ok(frame)
        opening = self.openings.get(stream_id)
        if opening is None:
            if stream_id in self.client_streams:
                return stream_id
            raise StreamStateError("STREAM_OPEN_OK for unknown Stream")
        self._settle_open_scheduler(
            opening,
            self._carrier_identity(carrier),
        )
        opening.observe_open_ok(stream_id, txid)
        self._activate_client_stream(opening)
        return stream_id

    def _client_handle_open_reject(
        self,
        carrier: SecureCarrier,
        rejection: StreamOpenReject,
    ) -> int:
        opening = self.openings.get(rejection.stream_id)
        if opening is None:
            raise StreamStateError("STREAM_OPEN_REJECT for unknown Stream")
        self._settle_open_scheduler(
            opening,
            self._carrier_identity(carrier),
        )
        opening.observe_open_reject(rejection)
        assert self.client_registry is not None
        self.client_registry.retire(rejection.stream_id)
        self.retired_streams.add(rejection.stream_id)
        del self.openings[rejection.stream_id]
        return rejection.stream_id

    def _client_handle_fin(
        self,
        carrier: SecureCarrier,
        terminal: StreamTerminal,
    ) -> None:
        opening = self.openings.get(terminal.stream_id)
        if opening is not None:
            result = opening.observe_fin(terminal)
            self._dispatch_opening_responses(carrier, result.responses)
            return

        active = self._client_active(terminal.stream_id)
        previous = active.receive.terminal_kind
        if (
            previous == "fin"
            and active.peer_terminal_transmission_id
            != terminal.transmission_id
        ):
            raise TransmissionIDError("STREAM_FIN changed Transmission ID")

        self.receive_flow.accept_commit(
            terminal.stream_id,
            terminal.final_offset,
        )
        active.receive.set_final(terminal.final_offset, kind="fin")
        if previous is None:
            active.peer_terminal_transmission_id = terminal.transmission_id
        carrier.send(
            encode_transmission_ack(
                terminal.stream_id,
                terminal.transmission_id,
                0,
            )
        )

    def _client_handle_reset(
        self,
        carrier: SecureCarrier,
        reset: ResetStream,
    ) -> None:
        opening = self.openings.get(reset.stream_id)
        if opening is not None:
            result = opening.observe_reset(reset)
            self._dispatch_opening_responses(carrier, result.responses)
            return

        active = self._client_active(reset.stream_id)
        if active.receive.terminal_kind == "reset":
            if active.peer_terminal_transmission_id != reset.transmission_id:
                raise TransmissionIDError("RESET_STREAM changed Transmission ID")
            if active.peer_error_code != reset.error_code:
                raise StreamStateError("RESET_STREAM changed Error Code")

        self.receive_flow.accept_commit(reset.stream_id, reset.final_offset)
        active.receive.set_final(reset.final_offset, kind="reset")
        self.receive_flow.consume(reset.stream_id, reset.final_offset)
        active.peer_terminal_transmission_id = reset.transmission_id
        active.peer_error_code = reset.error_code
        active.peer_accounting_released = True
        carrier.send(
            encode_transmission_ack(
                reset.stream_id,
                reset.transmission_id,
                0,
            )
        )
        self._client_maybe_retire(reset.stream_id)

    def _client_handle_stop(
        self,
        carrier: SecureCarrier,
        stop: StopSending,
    ) -> None:
        opening = self.openings.get(stop.stream_id)
        if opening is not None:
            result = opening.observe_stop(stop)
            self._dispatch_opening_responses(carrier, result.responses)
            return

        active = self._client_active(stop.stream_id)
        if active.stop_transmission_id is None:
            active.stop_transmission_id = stop.transmission_id
            active.stop_error_code = stop.error_code
        elif active.stop_transmission_id != stop.transmission_id:
            raise TransmissionIDError("STOP_SENDING changed Transmission ID")
        elif active.stop_error_code != stop.error_code:
            raise StreamStateError("STOP_SENDING changed Error Code")

        carrier.send(
            encode_transmission_ack(
                stop.stream_id,
                stop.transmission_id,
                0,
            )
        )
        if not active.local_terminal_acked:
            self.send_reset(stop.stream_id, stop.error_code)

    def _dispatch_opening_responses(
        self,
        incoming: SecureCarrier,
        responses: tuple[bytes, ...],
    ) -> None:
        for wire in responses:
            frame = decode_frames(wire)[0]
            if frame.type == int(FrameType.TRANSMISSION_ACK):
                incoming.send(wire)
            elif frame.type == int(FrameType.RESET_STREAM):
                self._transmit_generated_reliable(wire)
            else:
                incoming.send(wire)

    def _client_handle_ack(
        self,
        carrier: SecureCarrier,
        ack: TransmissionAck,
    ) -> None:
        self.reliability.acknowledge(
            ack.stream_id,
            ack.transmission_id,
            ack_carrier=self._carrier_identity(carrier),
        )

        opening = self.openings.get(ack.stream_id)
        if opening is not None:
            if ack.transmission_id == opening.local_reset_transmission_id:
                opening.observe_ack(ack)
            return

        active = self.client_streams.get(ack.stream_id)
        if active is None:
            return
        if active.local_terminal_transmission_id == ack.transmission_id:
            active.local_terminal_acked = True
        if active.local_consumed_transmission_id == ack.transmission_id:
            active.local_consumed_acked = True
        self._client_maybe_retire(ack.stream_id)

    def _client_handle_consumed(
        self,
        carrier: SecureCarrier,
        terminal: StreamTerminal,
    ) -> None:
        active = self._client_active(terminal.stream_id)
        if active.local_final_offset is None:
            raise StreamStateError("STREAM_CONSUMED before local terminal")
        if active.local_final_offset != terminal.final_offset:
            raise FinalSizeError("STREAM_CONSUMED contradicts local final size")
        carrier.send(
            encode_transmission_ack(
                terminal.stream_id,
                terminal.transmission_id,
                0,
            )
        )

    def _client_maybe_retire(self, stream_id: int) -> None:
        active = self.client_streams.get(stream_id)
        if active is None:
            return
        if active.local_final_offset is None or not active.local_terminal_acked:
            return
        if active.receive.final_offset is None or not active.peer_accounting_released:
            return
        if (
            active.receive.terminal_kind == "fin"
            and (
                active.local_consumed_transmission_id is None
                or not active.local_consumed_acked
            )
        ):
            return
        if any(
            pending.stream_id == stream_id
            for pending in self.ledger.pending.values()
        ):
            return

        assert self.client_registry is not None
        self.client_registry.retire(stream_id)
        self.retired_streams.add(stream_id)
        del self.client_streams[stream_id]

    def handle_frames(
        self,
        carrier: SecureCarrier,
        frames: list[Frame],
    ) -> EngineEvents:
        deliveries: list[tuple[int, bytes]] = []
        opened: list[int] = []
        rejected: list[int] = []
        terminal: list[tuple[int, str]] = []
        carrier_closed: list[tuple[int, int]] = []
        session_closed = False

        for frame in frames:
            ftype = frame.type

            if ftype == int(FrameType.PADDING):
                continue
            if ftype == int(FrameType.PING):
                carrier.send(encode_pong(decode_token_frame(frame)))
                continue
            if ftype == int(FrameType.PONG):
                continue
            if ftype == int(FrameType.CARRIER_CLOSE):
                self.close.receive_carrier_close(carrier, frame)
                carrier_closed.append(self._carrier_identity(carrier))
                continue
            if ftype == int(FrameType.SESSION_CLOSE):
                self.close.receive_session_close(carrier, frame)
                session_closed = True
                continue
            if ftype == int(FrameType.SESSION_CREDIT):
                credit = decode_session_credit(frame)
                self.send_flow.update_session_credit(
                    credit.consumed_bytes,
                    credit.maximum_bytes,
                )
                continue
            if ftype == int(FrameType.STREAM_CREDIT):
                credit = decode_stream_credit(frame)
                if self.role == "client" and credit.stream_id in self.openings:
                    self.openings[credit.stream_id].observe_credit(credit)
                else:
                    self.send_flow.update_stream_credit(
                        credit.stream_id,
                        credit.consumed_offset,
                        credit.maximum_offset,
                    )
                continue
            if ftype == int(FrameType.TRANSMISSION_ACK):
                ack = decode_transmission_ack(frame)
                if self.role == "client":
                    self._client_handle_ack(carrier, ack)
                else:
                    self.reliability.acknowledge(
                        ack.stream_id,
                        ack.transmission_id,
                        ack_carrier=self._carrier_identity(carrier),
                    )
                    assert self.server_terminal is not None
                    self.server_terminal.handle_ack(ack)
                continue

            if self.role == "client":
                if ftype == int(FrameType.STREAM_OPEN_OK):
                    opened.append(self._client_handle_open_ok(carrier, frame))
                    continue
                if ftype == int(FrameType.STREAM_OPEN_REJECT):
                    rejection = decode_stream_open_reject(frame)
                    rejected.append(
                        self._client_handle_open_reject(
                            carrier,
                            rejection,
                        )
                    )
                    continue
                if ftype == int(FrameType.STREAM_DATA):
                    data = decode_stream_data(frame)
                    opening = self.openings.get(data.stream_id)
                    if opening is not None:
                        opening.observe_data(data)
                        continue
                    active = self._client_active(data.stream_id)
                    end = data.offset + len(data.data)
                    self.receive_flow.accept_commit(data.stream_id, end)
                    chunk = active.receive.insert(data.offset, data.data)
                    carrier.send(
                        encode_transmission_ack(
                            data.stream_id,
                            data.transmission_id,
                            0,
                        )
                    )
                    if chunk:
                        deliveries.append((data.stream_id, chunk))
                    continue
                if ftype == int(FrameType.STREAM_FIN):
                    term = decode_stream_fin(frame)
                    self._client_handle_fin(carrier, term)
                    terminal.append((term.stream_id, "fin"))
                    continue
                if ftype == int(FrameType.RESET_STREAM):
                    reset = decode_reset_stream(frame)
                    self._client_handle_reset(carrier, reset)
                    terminal.append((reset.stream_id, "reset"))
                    continue
                if ftype == int(FrameType.STOP_SENDING):
                    stop = decode_stop_sending(frame)
                    self._client_handle_stop(carrier, stop)
                    terminal.append((stop.stream_id, "stop"))
                    continue
                if ftype == int(FrameType.STREAM_CONSUMED):
                    consumed = decode_stream_consumed(frame)
                    self._client_handle_consumed(carrier, consumed)
                    continue
                if ftype == int(FrameType.STREAM_OPEN):
                    raise StreamStateError(
                        "Server-initiated Streams are reserved in Draft 03"
                    )
            else:
                assert self.server_terminal is not None
                if ftype == int(FrameType.STREAM_OPEN):
                    stream_id, txid = decode_stream_open(frame)
                    result = self.server_terminal.handle_open(
                        stream_id,
                        txid,
                    )
                    self._dispatch_terminal_result(carrier, result)
                    if result.application_created:
                        opened.append(stream_id)
                    continue
                if ftype == int(FrameType.STREAM_DATA):
                    data = decode_stream_data(frame)
                    result = self.server_terminal.handle_data(data)
                    self._dispatch_terminal_result(carrier, result)
                    if result.delivered:
                        deliveries.append((data.stream_id, result.delivered))
                    continue
                if ftype == int(FrameType.STREAM_FIN):
                    term = decode_stream_fin(frame)
                    result = self.server_terminal.handle_fin(term)
                    self._dispatch_terminal_result(carrier, result)
                    terminal.append((term.stream_id, "fin"))
                    continue
                if ftype == int(FrameType.RESET_STREAM):
                    reset = decode_reset_stream(frame)
                    result = self.server_terminal.handle_reset(reset)
                    self._dispatch_terminal_result(carrier, result)
                    terminal.append((reset.stream_id, "reset"))
                    continue
                if ftype == int(FrameType.STOP_SENDING):
                    stop = decode_stop_sending(frame)
                    result = self.server_terminal.handle_stop(stop)
                    self._dispatch_terminal_result(carrier, result)
                    terminal.append((stop.stream_id, "stop"))
                    continue
                if ftype == int(FrameType.STREAM_CONSUMED):
                    consumed = decode_stream_consumed(frame)
                    result = self.server_terminal.handle_stream_consumed(
                        consumed
                    )
                    self._dispatch_terminal_result(carrier, result)
                    continue
                if ftype in {
                    int(FrameType.STREAM_OPEN_OK),
                    int(FrameType.STREAM_OPEN_REJECT),
                }:
                    raise StreamStateError(
                        "Server received Client-only Stream-open response"
                    )

            raise DecodeError(f"unsupported Frame type 0x{ftype:x}")

        return EngineEvents(
            deliveries=tuple(deliveries),
            opened=tuple(opened),
            rejected=tuple(rejected),
            terminal=tuple(terminal),
            carrier_closed=tuple(carrier_closed),
            session_closed=session_closed,
        )
