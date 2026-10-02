from __future__ import annotations

from dataclasses import dataclass, field

from .codec import (
    ResetStream,
    StopSending,
    StreamCredit,
    StreamData,
    StreamOpenReject,
    StreamTerminal,
    TransmissionAck,
    encode_reset_stream,
    encode_transmission_ack,
)
from .errors import FinalSizeError, StreamStateError, TransmissionIDError
from .stream import ReceiveStream, SendFlow, TransmissionLedger


@dataclass(frozen=True)
class OpeningResult:
    responses: tuple[bytes, ...] = ()
    became_open: bool = False
    became_rejected: bool = False


@dataclass
class ClientOpeningState:
    stream_id: int
    open_transmission_id: int
    ledger: TransmissionLedger
    send_flow: SendFlow
    receive: ReceiveStream = field(init=False)
    state: str = "opening"
    acceptance_evidence: bool = False
    rejection_error_code: int | None = None
    peer_terminal_kind: str | None = None
    peer_terminal_transmission_id: int | None = None
    peer_terminal_error_code: int | None = None
    stop_transmission_id: int | None = None
    stop_error_code: int | None = None
    local_reset_transmission_id: int | None = None
    local_reset_error_code: int | None = None
    local_reset_acked: bool = False

    def __post_init__(self) -> None:
        if self.stream_id <= 0 or self.stream_id % 2 == 0:
            raise ValueError("Client Stream ID must be a positive odd integer")
        pending = self.ledger.pending.get(self.open_transmission_id)
        if (
            pending is None
            or pending.stream_id != self.stream_id
            or pending.kind != "open"
        ):
            raise TransmissionIDError(
                "opening state requires the pending STREAM_OPEN Transmission"
            )
        self.receive = ReceiveStream(self.stream_id)

    @property
    def phase(self) -> str:
        if self.state != "opening":
            return self.state.upper()
        if self.acceptance_evidence:
            return "OPENING_WITH_ACCEPTANCE_EVIDENCE"
        return "OPENING"

    def _require_stream(self, stream_id: int) -> None:
        if stream_id != self.stream_id:
            raise StreamStateError("Frame belongs to a different Stream")

    def _mark_acceptance_evidence(self) -> None:
        if self.state == "rejected":
            raise StreamStateError(
                "acceptance evidence arrived after STREAM_OPEN_REJECT"
            )
        if self.state == "opening":
            self.acceptance_evidence = True

    def observe_credit(self, credit: StreamCredit) -> OpeningResult:
        self._require_stream(credit.stream_id)
        self._mark_acceptance_evidence()
        self.send_flow.update_stream_credit(
            credit.stream_id,
            credit.consumed_offset,
            credit.maximum_offset,
        )
        return OpeningResult()

    def observe_fin(self, terminal: StreamTerminal) -> OpeningResult:
        self._require_stream(terminal.stream_id)
        if self.state == "opening" and terminal.final_offset != 0:
            raise StreamStateError(
                "STREAM_FIN acceptance evidence requires Final Offset 0"
            )
        self._mark_acceptance_evidence()

        if self.peer_terminal_kind == "fin":
            if self.peer_terminal_transmission_id != terminal.transmission_id:
                raise TransmissionIDError("STREAM_FIN changed Transmission ID")
        elif self.peer_terminal_kind == "reset":
            if (
                self.receive.final_offset is not None
                and self.receive.final_offset != terminal.final_offset
            ):
                raise FinalSizeError("STREAM_FIN contradicts RESET final size")
        else:
            self.peer_terminal_kind = "fin"
            self.peer_terminal_transmission_id = terminal.transmission_id

        self.receive.set_final(terminal.final_offset, kind="fin")
        return OpeningResult(
            responses=(
                encode_transmission_ack(
                    self.stream_id,
                    terminal.transmission_id,
                    0,
                ),
            )
        )

    def observe_reset(self, reset: ResetStream) -> OpeningResult:
        self._require_stream(reset.stream_id)
        if self.state == "opening" and reset.final_offset != 0:
            raise StreamStateError(
                "RESET_STREAM acceptance evidence requires Final Offset 0"
            )
        self._mark_acceptance_evidence()

        if self.peer_terminal_kind == "reset":
            if self.peer_terminal_transmission_id != reset.transmission_id:
                raise TransmissionIDError("RESET_STREAM changed Transmission ID")
            if self.peer_terminal_error_code != reset.error_code:
                raise StreamStateError("RESET_STREAM changed Error Code")
        elif (
            self.receive.final_offset is not None
            and self.receive.final_offset != reset.final_offset
        ):
            raise FinalSizeError("RESET_STREAM contradicts established final size")

        self.receive.set_final(reset.final_offset, kind="reset")
        self.peer_terminal_kind = "reset"
        self.peer_terminal_transmission_id = reset.transmission_id
        self.peer_terminal_error_code = reset.error_code
        return OpeningResult(
            responses=(
                encode_transmission_ack(
                    self.stream_id,
                    reset.transmission_id,
                    0,
                ),
            )
        )

    def observe_stop(self, stop: StopSending) -> OpeningResult:
        self._require_stream(stop.stream_id)
        self._mark_acceptance_evidence()

        if self.stop_transmission_id is None:
            self.stop_transmission_id = stop.transmission_id
            self.stop_error_code = stop.error_code
        elif self.stop_transmission_id != stop.transmission_id:
            raise TransmissionIDError("STOP_SENDING changed Transmission ID")
        elif self.stop_error_code != stop.error_code:
            raise StreamStateError("STOP_SENDING changed Error Code")

        responses = [
            encode_transmission_ack(
                self.stream_id,
                stop.transmission_id,
                0,
            )
        ]

        if not self.local_reset_acked:
            if self.local_reset_transmission_id is None:
                txid = self.ledger.allocate(self.stream_id, "reset")
                wire = encode_reset_stream(
                    self.stream_id,
                    txid,
                    0,
                    stop.error_code,
                )
                self.ledger.bind_frame(txid, wire)
                self.local_reset_transmission_id = txid
                self.local_reset_error_code = stop.error_code
            else:
                wire = self.ledger.reinjection_frame(
                    self.local_reset_transmission_id
                )
            responses.append(wire)

        return OpeningResult(responses=tuple(responses))

    def observe_data(self, data: StreamData) -> OpeningResult:
        self._require_stream(data.stream_id)
        if self.state == "opening":
            raise StreamStateError(
                "STREAM_DATA cannot arrive before STREAM_OPEN_OK"
            )
        if self.state != "open":
            raise StreamStateError("STREAM_DATA on non-open Stream")
        return OpeningResult()

    def observe_open_ok(
        self,
        stream_id: int,
        transmission_id: int,
    ) -> OpeningResult:
        self._require_stream(stream_id)
        if transmission_id != self.open_transmission_id:
            raise TransmissionIDError("STREAM_OPEN_OK references wrong Transmission")
        if self.state == "rejected":
            raise StreamStateError("STREAM_OPEN_OK arrived after rejection")
        if self.state == "open":
            return OpeningResult()

        self.ledger.settle_open(stream_id, transmission_id)
        self.state = "open"
        return OpeningResult(became_open=True)

    def observe_open_reject(
        self,
        rejection: StreamOpenReject,
    ) -> OpeningResult:
        self._require_stream(rejection.stream_id)
        if rejection.transmission_id != self.open_transmission_id:
            raise TransmissionIDError(
                "STREAM_OPEN_REJECT references wrong Transmission"
            )

        if self.acceptance_evidence or self.state == "open":
            raise StreamStateError(
                "STREAM_OPEN_REJECT contradicts acceptance evidence"
            )

        if self.state == "rejected":
            if self.rejection_error_code != rejection.error_code:
                raise StreamStateError(
                    "duplicate STREAM_OPEN_REJECT changed Error Code"
                )
            return OpeningResult()

        self.ledger.settle_open(
            rejection.stream_id,
            rejection.transmission_id,
        )
        self.state = "rejected"
        self.rejection_error_code = rejection.error_code
        return OpeningResult(became_rejected=True)

    def observe_ack(self, ack: TransmissionAck) -> OpeningResult:
        self._require_stream(ack.stream_id)
        self.ledger.settle(ack.stream_id, ack.transmission_id)
        if ack.transmission_id == self.local_reset_transmission_id:
            self.local_reset_acked = True
        return OpeningResult()
