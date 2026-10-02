from __future__ import annotations

from dataclasses import dataclass, field

from .errors import (
    FinalSizeError,
    FlowControlError,
    StreamStateError,
    TransmissionIDError,
)


STREAM_WINDOW = 64 * 1024
SESSION_WINDOW = 1024 * 1024


@dataclass
class CreditAdvertisement:
    consumed: int = 0
    maximum: int = 0

    def update(self, consumed: int, maximum: int) -> None:
        if consumed < self.consumed or maximum < self.maximum:
            raise FlowControlError("credit values must be monotonically non-decreasing")
        if maximum < consumed:
            raise FlowControlError("maximum credit is below consumed credit")
        self.consumed = consumed
        self.maximum = maximum


@dataclass
class SendFlow:
    session_credit: CreditAdvertisement = field(default_factory=CreditAdvertisement)
    stream_credit: dict[int, CreditAdvertisement] = field(default_factory=dict)
    stream_committed: dict[int, int] = field(default_factory=dict)
    session_committed: int = 0

    def update_session_credit(self, consumed: int, maximum: int) -> None:
        if consumed > self.session_committed:
            raise FlowControlError("peer consumed more Session bytes than were committed")
        self.session_credit.update(consumed, maximum)

    def update_stream_credit(self, stream_id: int, consumed: int, maximum: int) -> None:
        committed = self.stream_committed.get(stream_id, 0)
        if consumed > committed:
            raise FlowControlError("peer consumed beyond committed Stream offset")
        credit = self.stream_credit.setdefault(stream_id, CreditAdvertisement())
        credit.update(consumed, maximum)

    def commit(self, stream_id: int, end_offset: int) -> int:
        credit = self.stream_credit.get(stream_id)
        if credit is None:
            raise FlowControlError("no Stream credit")
        if end_offset > credit.maximum:
            raise FlowControlError("Stream credit exceeded")

        previous = self.stream_committed.get(stream_id, 0)
        delta = max(0, end_offset - previous)
        if self.session_committed + delta > self.session_credit.maximum:
            raise FlowControlError("Session credit exceeded")

        if end_offset > previous:
            self.stream_committed[stream_id] = end_offset
            self.session_committed += delta
        return delta


@dataclass
class ReceiveFlow:
    session_consumed: int = 0
    session_maximum: int = SESSION_WINDOW
    session_committed: int = 0
    session_released: int = 0
    stream_consumed: dict[int, int] = field(default_factory=dict)
    stream_maximum: dict[int, int] = field(default_factory=dict)
    stream_committed: dict[int, int] = field(default_factory=dict)

    def open_stream(self, stream_id: int, *, window: int = STREAM_WINDOW) -> None:
        if stream_id in self.stream_maximum:
            raise StreamStateError("Stream receive credit already exists")
        self.stream_consumed[stream_id] = 0
        self.stream_maximum[stream_id] = window
        self.stream_committed[stream_id] = 0

    def accept_commit(self, stream_id: int, end_offset: int) -> int:
        if stream_id not in self.stream_maximum:
            raise FlowControlError("DATA received before Stream credit exists")
        if end_offset > self.stream_maximum[stream_id]:
            raise FlowControlError("peer exceeded Stream credit")

        previous = self.stream_committed[stream_id]
        delta = max(0, end_offset - previous)
        if self.session_committed + delta > self.session_maximum:
            raise FlowControlError("peer exceeded Session credit")

        if end_offset > previous:
            self.stream_committed[stream_id] = end_offset
            self.session_committed += delta
        return delta

    def consume(self, stream_id: int, consumed_offset: int) -> tuple[int, int, int, int]:
        if stream_id not in self.stream_consumed:
            raise StreamStateError("unknown Stream")
        old = self.stream_consumed[stream_id]
        committed = self.stream_committed[stream_id]
        if consumed_offset < old or consumed_offset > committed:
            raise FlowControlError("invalid consumed Stream offset")

        delta = consumed_offset - old
        self.stream_consumed[stream_id] = consumed_offset
        self.stream_maximum[stream_id] = consumed_offset + STREAM_WINDOW
        self.session_released += delta
        self.session_consumed = self.session_released
        self.session_maximum = self.session_consumed + SESSION_WINDOW
        return (
            self.stream_consumed[stream_id],
            self.stream_maximum[stream_id],
            self.session_consumed,
            self.session_maximum,
        )


@dataclass
class ReceiveStream:
    stream_id: int
    delivered_offset: int = 0
    final_offset: int | None = None
    terminal_kind: str | None = None
    _accepted: dict[int, int] = field(default_factory=dict)

    @property
    def highest_end(self) -> int:
        return max(self._accepted.keys(), default=-1) + 1

    @property
    def complete(self) -> bool:
        return self.final_offset is not None and self.delivered_offset == self.final_offset

    def insert(self, offset: int, data: bytes) -> bytes:
        if not data:
            raise ValueError("empty STREAM_DATA")
        end = offset + len(data)
        if self.final_offset is not None and end > self.final_offset:
            raise FinalSizeError("DATA extends beyond final size")

        for index, value in enumerate(data, start=offset):
            previous = self._accepted.get(index)
            if previous is not None and previous != value:
                raise StreamStateError("conflicting overlapping Stream bytes")
            self._accepted[index] = value

        out = bytearray()
        while self.delivered_offset in self._accepted:
            out.append(self._accepted[self.delivered_offset])
            self.delivered_offset += 1
        return bytes(out)

    def set_final(self, final_offset: int, *, kind: str = "fin") -> None:
        if self.final_offset is not None and self.final_offset != final_offset:
            raise FinalSizeError("contradictory final size")
        if final_offset < self.highest_end:
            raise FinalSizeError("final size is below authenticated data")
        self.final_offset = final_offset
        if self.terminal_kind == "reset":
            return
        if kind == "reset":
            self.terminal_kind = "reset"
        elif self.terminal_kind is None:
            self.terminal_kind = "fin"


@dataclass
class PendingTransmission:
    stream_id: int
    kind: str
    wire_frame: bytes | None = None
    attempts: list[tuple[int, int]] = field(default_factory=list)


@dataclass
class TransmissionLedger:
    next_id: int = 1
    pending: dict[int, PendingTransmission] = field(default_factory=dict)
    settled: set[int] = field(default_factory=set)

    def allocate(self, stream_id: int, kind: str) -> int:
        transmission_id = self.next_id
        self.next_id += 1
        self.pending[transmission_id] = PendingTransmission(stream_id, kind)
        return transmission_id

    def bind_frame(self, transmission_id: int, wire_frame: bytes) -> None:
        pending = self.pending.get(transmission_id)
        if pending is None:
            raise TransmissionIDError("cannot bind a settled or unknown Transmission")
        if pending.wire_frame is not None and pending.wire_frame != wire_frame:
            raise TransmissionIDError("Transmission semantics changed after allocation")
        pending.wire_frame = bytes(wire_frame)

    def note_attempt(
        self,
        transmission_id: int,
        carrier_id: int,
        generation: int,
    ) -> None:
        pending = self.pending.get(transmission_id)
        if pending is None:
            raise TransmissionIDError("cannot attempt a settled or unknown Transmission")
        if pending.wire_frame is None:
            raise TransmissionIDError("Transmission has no bound wire Frame")
        pending.attempts.append((carrier_id, generation))

    def reinjection_frame(self, transmission_id: int) -> bytes:
        pending = self.pending.get(transmission_id)
        if pending is None:
            raise TransmissionIDError("Transmission is no longer outstanding")
        if pending.wire_frame is None:
            raise TransmissionIDError("Transmission has no bound wire Frame")
        return pending.wire_frame

    def outstanding_ids(self) -> tuple[int, ...]:
        return tuple(sorted(self.pending))

    def settle(self, stream_id: int, transmission_id: int) -> None:
        if transmission_id in self.settled:
            return
        pending = self.pending.get(transmission_id)
        if pending is None:
            if transmission_id >= self.next_id:
                raise TransmissionIDError("acknowledgement for never-allocated Transmission ID")
            return
        if pending.stream_id != stream_id:
            raise TransmissionIDError("acknowledgement Stream ID mismatch")
        del self.pending[transmission_id]
        self.settled.add(transmission_id)

    def settle_open(self, stream_id: int, transmission_id: int) -> None:
        pending = self.pending.get(transmission_id)
        if pending is None or pending.kind != "open":
            raise TransmissionIDError("STREAM_OPEN_OK references unexpected Transmission")
        self.settle(stream_id, transmission_id)
