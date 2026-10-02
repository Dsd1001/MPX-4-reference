from __future__ import annotations

from dataclasses import dataclass, field

from .codec import (
    ResetStream,
    StopSending,
    StreamData,
    StreamTerminal,
    TransmissionAck,
    encode_reset_stream,
    encode_stream_consumed,
    encode_stream_credit,
    encode_stream_open_ok,
    encode_stream_open_reject,
    encode_transmission_ack,
)
from .constants import ErrorCode
from .errors import FinalSizeError, StreamStateError, TransmissionIDError
from .stream import (
    ReceiveFlow,
    ReceiveStream,
    ServerStreamRegistry,
    TransmissionLedger,
)


@dataclass
class ActiveTerminalStream:
    stream_id: int
    open_transmission_id: int
    receive: ReceiveStream
    local_send_committed: int = 0
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


@dataclass
class StreamTombstone:
    stream_id: int
    decision: str
    open_transmission_id: int | None = None
    rejection_error_code: int | None = None

    local_terminal_kind: str | None = None
    local_terminal_transmission_id: int | None = None
    local_final_offset: int | None = None
    local_error_code: int | None = None
    local_terminal_acked: bool = False
    local_terminal_wire: bytes | None = None

    peer_terminal_kind: str | None = None
    peer_terminal_transmission_id: int | None = None
    peer_final_offset: int | None = None
    peer_error_code: int | None = None

    stop_transmission_id: int | None = None
    stop_error_code: int | None = None

    last_consumed_offset: int = 0
    last_maximum_offset: int = 0


@dataclass(frozen=True)
class TerminalResult:
    responses: tuple[bytes, ...] = ()
    delivered: bytes = b""
    application_created: bool = False


@dataclass
class ServerTerminalStateMachine:
    max_streams: int = 32
    registry: ServerStreamRegistry = field(init=False)
    receive_flow: ReceiveFlow = field(default_factory=ReceiveFlow)
    ledger: TransmissionLedger = field(default_factory=TransmissionLedger)
    active: dict[int, ActiveTerminalStream] = field(default_factory=dict)
    tombstones: dict[int, StreamTombstone] = field(default_factory=dict)
    retired: set[int] = field(default_factory=set)

    def __post_init__(self) -> None:
        self.registry = ServerStreamRegistry(self.max_streams)

    def _validate_stream_id(self, stream_id: int) -> None:
        self.registry.validate_client_stream_id(stream_id)

    @staticmethod
    def _ack(stream_id: int, transmission_id: int) -> bytes:
        return encode_transmission_ack(stream_id, transmission_id, 0)

    def _pending_for_stream(self, stream_id: int) -> bool:
        return any(
            pending.stream_id == stream_id
            for pending in self.ledger.pending.values()
        )

    def handle_open(self, stream_id: int, transmission_id: int) -> TerminalResult:
        self._validate_stream_id(stream_id)
        if transmission_id <= 0:
            raise TransmissionIDError("invalid STREAM_OPEN Transmission ID")

        if stream_id in self.retired:
            return TerminalResult()

        tombstone = self.tombstones.get(stream_id)
        if tombstone is not None:
            if tombstone.open_transmission_id is None:
                tombstone.open_transmission_id = transmission_id
            elif tombstone.open_transmission_id != transmission_id:
                raise TransmissionIDError("conflicting STREAM_OPEN Transmission ID")

            if tombstone.decision == "accepted":
                return TerminalResult(
                    responses=(
                        encode_stream_open_ok(stream_id, transmission_id),
                    )
                )

            error_code = (
                tombstone.rejection_error_code
                if tombstone.rejection_error_code is not None
                else int(ErrorCode.STREAM_STATE_ERROR)
            )
            return TerminalResult(
                responses=(
                    encode_stream_open_reject(
                        stream_id,
                        transmission_id,
                        error_code,
                    ),
                )
            )

        existing = self.active.get(stream_id)
        if existing is not None:
            if existing.open_transmission_id != transmission_id:
                raise TransmissionIDError("conflicting STREAM_OPEN Transmission ID")
            return TerminalResult(
                responses=(
                    encode_stream_open_ok(stream_id, transmission_id),
                    encode_stream_credit(
                        stream_id,
                        self.receive_flow.stream_consumed[stream_id],
                        self.receive_flow.stream_maximum[stream_id],
                    ),
                )
            )

        self.registry.accept_open(stream_id)
        self.receive_flow.open_stream(stream_id)
        state = ActiveTerminalStream(
            stream_id=stream_id,
            open_transmission_id=transmission_id,
            receive=ReceiveStream(stream_id),
        )
        self.active[stream_id] = state
        return TerminalResult(
            responses=(
                encode_stream_open_ok(stream_id, transmission_id),
                encode_stream_credit(
                    stream_id,
                    self.receive_flow.stream_consumed[stream_id],
                    self.receive_flow.stream_maximum[stream_id],
                ),
            ),
            application_created=True,
        )

    def _record_preopen_reset(self, reset: ResetStream) -> TerminalResult:
        if reset.final_offset != 0:
            raise StreamStateError(
                "pre-open RESET_STREAM requires Final Offset 0"
            )
        self.registry.remember_used(reset.stream_id)
        self.tombstones[reset.stream_id] = StreamTombstone(
            stream_id=reset.stream_id,
            decision="preopen-reset",
            rejection_error_code=int(ErrorCode.STREAM_STATE_ERROR),
            peer_terminal_kind="reset",
            peer_terminal_transmission_id=reset.transmission_id,
            peer_final_offset=0,
            peer_error_code=reset.error_code,
        )
        return TerminalResult(
            responses=(self._ack(reset.stream_id, reset.transmission_id),)
        )

    def handle_reset(self, reset: ResetStream) -> TerminalResult:
        self._validate_stream_id(reset.stream_id)

        if reset.stream_id in self.retired:
            return TerminalResult()

        tombstone = self.tombstones.get(reset.stream_id)
        if tombstone is not None:
            if (
                tombstone.peer_final_offset is not None
                and tombstone.peer_final_offset != reset.final_offset
            ):
                raise FinalSizeError("RESET_STREAM contradicts tombstone final size")

            if tombstone.peer_terminal_kind == "reset":
                if (
                    tombstone.peer_terminal_transmission_id
                    != reset.transmission_id
                ):
                    raise TransmissionIDError(
                        "RESET_STREAM changed Transmission ID"
                    )
                if tombstone.peer_error_code != reset.error_code:
                    raise StreamStateError(
                        "RESET_STREAM changed terminal Error Code"
                    )
            else:
                tombstone.peer_terminal_kind = "reset"
                tombstone.peer_terminal_transmission_id = reset.transmission_id
                tombstone.peer_final_offset = reset.final_offset
                tombstone.peer_error_code = reset.error_code

            return TerminalResult(
                responses=(self._ack(reset.stream_id, reset.transmission_id),)
            )

        state = self.active.get(reset.stream_id)
        if state is None:
            return self._record_preopen_reset(reset)

        if state.receive.terminal_kind == "reset":
            if state.peer_terminal_transmission_id != reset.transmission_id:
                raise TransmissionIDError("RESET_STREAM changed Transmission ID")
            if state.peer_error_code != reset.error_code:
                raise StreamStateError("RESET_STREAM changed terminal Error Code")

        self.receive_flow.accept_commit(reset.stream_id, reset.final_offset)
        state.receive.set_final(reset.final_offset, kind="reset")
        self.receive_flow.consume(reset.stream_id, reset.final_offset)
        if state.receive.terminal_kind == "reset":
            state.peer_terminal_transmission_id = reset.transmission_id
            state.peer_error_code = reset.error_code
        state.peer_accounting_released = True

        result = TerminalResult(
            responses=(self._ack(reset.stream_id, reset.transmission_id),)
        )
        self._maybe_tombstone(reset.stream_id)
        return result

    def handle_fin(self, terminal: StreamTerminal) -> TerminalResult:
        self._validate_stream_id(terminal.stream_id)

        if terminal.stream_id in self.retired:
            return TerminalResult()

        tombstone = self.tombstones.get(terminal.stream_id)
        if tombstone is not None:
            if tombstone.peer_final_offset is None:
                raise StreamStateError("STREAM_FIN cannot create retired state")
            if tombstone.peer_final_offset != terminal.final_offset:
                raise FinalSizeError("STREAM_FIN contradicts tombstone final size")
            return TerminalResult(
                responses=(
                    self._ack(
                        terminal.stream_id,
                        terminal.transmission_id,
                    ),
                )
            )

        state = self.active.get(terminal.stream_id)
        if state is None:
            raise StreamStateError("STREAM_FIN before STREAM_OPEN")

        previous_kind = state.receive.terminal_kind
        if (
            previous_kind == "fin"
            and state.peer_terminal_transmission_id != terminal.transmission_id
        ):
            raise TransmissionIDError("STREAM_FIN changed Transmission ID")

        self.receive_flow.accept_commit(
            terminal.stream_id,
            terminal.final_offset,
        )
        state.receive.set_final(terminal.final_offset, kind="fin")
        if previous_kind is None:
            state.peer_terminal_transmission_id = terminal.transmission_id

        return TerminalResult(
            responses=(
                self._ack(
                    terminal.stream_id,
                    terminal.transmission_id,
                ),
            )
        )

    def handle_data(self, data: StreamData) -> TerminalResult:
        self._validate_stream_id(data.stream_id)
        end_offset = data.offset + len(data.data)

        if data.stream_id in self.retired:
            return TerminalResult()

        tombstone = self.tombstones.get(data.stream_id)
        if tombstone is not None:
            if tombstone.peer_final_offset is None:
                raise StreamStateError(
                    "STREAM_DATA cannot follow pre-open cancellation"
                )
            if end_offset > tombstone.peer_final_offset:
                raise FinalSizeError("STREAM_DATA extends beyond tombstone final size")
            return TerminalResult(
                responses=(self._ack(data.stream_id, data.transmission_id),)
            )

        state = self.active.get(data.stream_id)
        if state is None:
            raise StreamStateError("STREAM_DATA before STREAM_OPEN")

        if (
            state.receive.final_offset is not None
            and end_offset > state.receive.final_offset
        ):
            raise FinalSizeError("STREAM_DATA extends beyond final size")

        self.receive_flow.accept_commit(data.stream_id, end_offset)
        delivered = state.receive.insert(data.offset, data.data)
        return TerminalResult(
            responses=(self._ack(data.stream_id, data.transmission_id),),
            delivered=delivered,
        )

    def note_local_send_commit(self, stream_id: int, end_offset: int) -> None:
        state = self.active.get(stream_id)
        if state is None:
            raise StreamStateError("cannot send on unknown Stream")
        if state.local_terminal_kind is not None:
            raise StreamStateError("cannot commit DATA after local terminal state")
        if end_offset < state.local_send_committed:
            raise StreamStateError("local send commitment cannot decrease")
        state.local_send_committed = end_offset

    def start_local_fin(self, stream_id: int) -> bytes:
        state = self.active.get(stream_id)
        if state is None:
            raise StreamStateError("cannot FIN unknown Stream")
        if state.local_terminal_kind is not None:
            raise StreamStateError("local send direction is already terminal")

        txid = self.ledger.allocate(stream_id, "fin")
        from .codec import encode_stream_fin

        wire = encode_stream_fin(
            stream_id,
            txid,
            state.local_send_committed,
        )
        self.ledger.bind_frame(txid, wire)
        state.local_terminal_kind = "fin"
        state.local_terminal_transmission_id = txid
        state.local_final_offset = state.local_send_committed
        return wire

    def _active_reset_wire(
        self,
        state: ActiveTerminalStream,
        error_code: int,
    ) -> bytes:
        if state.local_terminal_kind == "reset":
            if state.local_terminal_transmission_id is None:
                raise StreamStateError("RESET state has no Transmission ID")
            return self.ledger.reinjection_frame(
                state.local_terminal_transmission_id
            )

        final_offset = (
            state.local_final_offset
            if state.local_final_offset is not None
            else state.local_send_committed
        )
        txid = self.ledger.allocate(state.stream_id, "reset")
        wire = encode_reset_stream(
            state.stream_id,
            txid,
            final_offset,
            error_code,
        )
        self.ledger.bind_frame(txid, wire)
        state.local_terminal_kind = "reset"
        state.local_terminal_transmission_id = txid
        state.local_final_offset = final_offset
        state.local_error_code = error_code
        state.local_terminal_acked = False
        return wire

    def start_local_reset(self, stream_id: int, error_code: int) -> bytes:
        state = self.active.get(stream_id)
        if state is None:
            raise StreamStateError("cannot RESET unknown Stream")
        return self._active_reset_wire(state, error_code)

    def _tombstone_reset_wire(
        self,
        tombstone: StreamTombstone,
        error_code: int,
    ) -> bytes:
        if tombstone.local_terminal_kind == "reset":
            if tombstone.local_terminal_wire is None:
                raise StreamStateError("tombstone RESET state lacks wire Frame")
            return tombstone.local_terminal_wire

        txid = self.ledger.allocate(tombstone.stream_id, "reset")
        wire = encode_reset_stream(
            tombstone.stream_id,
            txid,
            0,
            error_code,
        )
        self.ledger.bind_frame(txid, wire)
        tombstone.local_terminal_kind = "reset"
        tombstone.local_terminal_transmission_id = txid
        tombstone.local_final_offset = 0
        tombstone.local_error_code = error_code
        tombstone.local_terminal_wire = wire
        return wire

    def handle_stop(self, stop: StopSending) -> TerminalResult:
        self._validate_stream_id(stop.stream_id)

        if stop.stream_id in self.retired:
            return TerminalResult()

        tombstone = self.tombstones.get(stop.stream_id)
        if tombstone is not None:
            if tombstone.stop_transmission_id is None:
                tombstone.stop_transmission_id = stop.transmission_id
                tombstone.stop_error_code = stop.error_code
            elif tombstone.stop_transmission_id != stop.transmission_id:
                raise TransmissionIDError("STOP_SENDING changed Transmission ID")
            elif tombstone.stop_error_code != stop.error_code:
                raise StreamStateError("STOP_SENDING changed Error Code")

            responses = [self._ack(stop.stream_id, stop.transmission_id)]
            if not tombstone.local_terminal_acked:
                responses.append(
                    self._tombstone_reset_wire(
                        tombstone,
                        stop.error_code,
                    )
                )
            return TerminalResult(responses=tuple(responses))

        state = self.active.get(stop.stream_id)
        if state is None:
            self.registry.remember_used(stop.stream_id)
            tombstone = StreamTombstone(
                stream_id=stop.stream_id,
                decision="preopen-stop",
                rejection_error_code=int(ErrorCode.STREAM_STATE_ERROR),
                stop_transmission_id=stop.transmission_id,
                stop_error_code=stop.error_code,
            )
            self.tombstones[stop.stream_id] = tombstone
            reset_wire = self._tombstone_reset_wire(
                tombstone,
                stop.error_code,
            )
            return TerminalResult(
                responses=(
                    self._ack(stop.stream_id, stop.transmission_id),
                    reset_wire,
                )
            )

        responses = [self._ack(stop.stream_id, stop.transmission_id)]
        if not state.local_terminal_acked:
            responses.append(
                self._active_reset_wire(
                    state,
                    stop.error_code,
                )
            )
        return TerminalResult(responses=tuple(responses))

    def handle_stream_consumed(
        self,
        terminal: StreamTerminal,
    ) -> TerminalResult:
        self._validate_stream_id(terminal.stream_id)

        if terminal.stream_id in self.retired:
            return TerminalResult()

        tombstone = self.tombstones.get(terminal.stream_id)
        if tombstone is not None:
            if tombstone.local_final_offset is None:
                raise StreamStateError("STREAM_CONSUMED without local final size")
            if tombstone.local_final_offset != terminal.final_offset:
                raise FinalSizeError("STREAM_CONSUMED contradicts local final size")
            return TerminalResult(
                responses=(
                    self._ack(
                        terminal.stream_id,
                        terminal.transmission_id,
                    ),
                )
            )

        state = self.active.get(terminal.stream_id)
        if state is None or state.local_final_offset is None:
            raise StreamStateError("STREAM_CONSUMED before local terminal state")
        if state.local_final_offset != terminal.final_offset:
            raise FinalSizeError("STREAM_CONSUMED contradicts local final size")

        return TerminalResult(
            responses=(
                self._ack(
                    terminal.stream_id,
                    terminal.transmission_id,
                ),
            )
        )

    def release_receive(self, stream_id: int) -> bytes | None:
        state = self.active.get(stream_id)
        if state is None:
            raise StreamStateError("cannot release unknown Stream")
        if state.receive.final_offset is None:
            raise StreamStateError("cannot release receive side before terminal state")

        final_offset = state.receive.final_offset
        if state.receive.terminal_kind == "reset":
            if not state.peer_accounting_released:
                self.receive_flow.consume(stream_id, final_offset)
                state.peer_accounting_released = True
            self._maybe_tombstone(stream_id)
            return None

        if not state.receive.complete:
            raise StreamStateError("cannot consume incomplete FIN-terminated Stream")

        if not state.peer_accounting_released:
            self.receive_flow.consume(stream_id, final_offset)
            state.peer_accounting_released = True

        if state.local_consumed_transmission_id is not None:
            return self.ledger.reinjection_frame(
                state.local_consumed_transmission_id
            )

        txid = self.ledger.allocate(stream_id, "consumed")
        wire = encode_stream_consumed(stream_id, txid, final_offset)
        self.ledger.bind_frame(txid, wire)
        state.local_consumed_transmission_id = txid
        return wire

    def handle_ack(self, ack: TransmissionAck) -> None:
        self._validate_stream_id(ack.stream_id)
        self.ledger.settle(ack.stream_id, ack.transmission_id)

        state = self.active.get(ack.stream_id)
        if state is not None:
            if state.local_terminal_transmission_id == ack.transmission_id:
                state.local_terminal_acked = True
            if state.local_consumed_transmission_id == ack.transmission_id:
                state.local_consumed_acked = True
            self._maybe_tombstone(ack.stream_id)
            return

        tombstone = self.tombstones.get(ack.stream_id)
        if (
            tombstone is not None
            and tombstone.local_terminal_transmission_id
            == ack.transmission_id
        ):
            tombstone.local_terminal_acked = True

    def _maybe_tombstone(self, stream_id: int) -> None:
        state = self.active.get(stream_id)
        if state is None:
            return
        if state.local_final_offset is None or not state.local_terminal_acked:
            return
        if state.receive.final_offset is None or not state.peer_accounting_released:
            return
        if (
            state.receive.terminal_kind == "fin"
            and (
                state.local_consumed_transmission_id is None
                or not state.local_consumed_acked
            )
        ):
            return
        if self._pending_for_stream(stream_id):
            return

        self.registry.retire(stream_id)
        self.tombstones[stream_id] = StreamTombstone(
            stream_id=stream_id,
            decision="accepted",
            open_transmission_id=state.open_transmission_id,
            local_terminal_kind=state.local_terminal_kind,
            local_terminal_transmission_id=state.local_terminal_transmission_id,
            local_final_offset=state.local_final_offset,
            local_error_code=state.local_error_code,
            local_terminal_acked=True,
            peer_terminal_kind=state.receive.terminal_kind,
            peer_terminal_transmission_id=state.peer_terminal_transmission_id,
            peer_final_offset=state.receive.final_offset,
            peer_error_code=state.peer_error_code,
            last_consumed_offset=self.receive_flow.stream_consumed[stream_id],
            last_maximum_offset=self.receive_flow.stream_maximum[stream_id],
        )
        del self.active[stream_id]

    def compact(self, stream_id: int) -> None:
        tombstone = self.tombstones.get(stream_id)
        if tombstone is None:
            raise StreamStateError("no tombstone to compact")
        if self._pending_for_stream(stream_id):
            raise StreamStateError("cannot compact tombstone with pending Transmission")
        del self.tombstones[stream_id]
        self.retired.add(stream_id)

    @property
    def active_stream_count(self) -> int:
        return len(self.registry.active)
