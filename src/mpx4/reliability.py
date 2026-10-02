from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Callable

from .carrier import SecureCarrier
from .errors import CarrierLostError
from .scheduler import AggregateScheduler
from .stream import TransmissionLedger


def send_tracked_attempt(
    carrier: SecureCarrier,
    ledger: TransmissionLedger,
    transmission_id: int,
) -> None:
    frame = ledger.reinjection_frame(transmission_id)
    carrier.send(frame)
    ledger.note_attempt(
        transmission_id,
        carrier.identity.carrier_id,
        carrier.identity.generation,
    )


def reinject_outstanding(
    carrier: SecureCarrier,
    ledger: TransmissionLedger,
    *,
    transmission_ids: tuple[int, ...] | None = None,
) -> tuple[int, ...]:
    ids = transmission_ids or ledger.outstanding_ids()
    sent: list[int] = []
    for transmission_id in ids:
        pending = ledger.pending.get(transmission_id)
        if pending is None or pending.wire_frame is None:
            continue
        send_tracked_attempt(carrier, ledger, transmission_id)
        sent.append(transmission_id)
    return tuple(sent)


@dataclass(frozen=True)
class ScheduledAttempt:
    transmission_id: int
    carrier_id: int
    generation: int
    reason: str
    sent_at: float


class ReliabilityLoop:
    def __init__(
        self,
        ledger: TransmissionLedger,
        scheduler: AggregateScheduler,
        *,
        retransmit_after_s: float = 0.250,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if retransmit_after_s <= 0:
            raise ValueError("retransmit_after_s must be positive")
        self.ledger = ledger
        self.scheduler = scheduler
        self.retransmit_after_s = retransmit_after_s
        self.clock = clock

    def _send(self, transmission_id: int, reason: str) -> ScheduledAttempt:
        pending = self.ledger.pending[transmission_id]
        frame = self.ledger.reinjection_frame(transmission_id)
        previous = tuple(pending.attempts)

        attempts_left = max(1, len(self.scheduler.carriers))
        last_error: CarrierLostError | None = None
        while attempts_left > 0:
            attempts_left -= 1
            metrics = self.scheduler.choose(
                len(frame),
                previous_attempts=previous,
            )
            identity = metrics.identity
            sent_at = self.clock()
            try:
                metrics.carrier.send(frame)
            except CarrierLostError as exc:
                self.scheduler.mark_failure(identity)
                previous = previous + (identity,)
                last_error = exc
                continue

            self.ledger.note_attempt(
                transmission_id,
                identity[0],
                identity[1],
                sent_at=sent_at,
            )
            self.scheduler.record_attempt(
                transmission_id,
                identity,
                len(frame),
                sent_at,
            )
            return ScheduledAttempt(
                transmission_id=transmission_id,
                carrier_id=identity[0],
                generation=identity[1],
                reason=reason,
                sent_at=sent_at,
            )

        if last_error is not None:
            raise last_error
        raise CarrierLostError("no active Carrier accepted the Attempt")

    def transmit_new(self, transmission_id: int) -> ScheduledAttempt:
        pending = self.ledger.pending[transmission_id]
        if pending.attempts:
            raise ValueError("Transmission already has an Attempt")
        return self._send(transmission_id, "new")

    def poll(self) -> tuple[ScheduledAttempt, ...]:
        now = self.clock()
        scheduled: list[ScheduledAttempt] = []

        for transmission_id in self.ledger.outstanding_ids():
            pending = self.ledger.pending.get(transmission_id)
            if pending is None or pending.wire_frame is None:
                continue

            if not pending.attempts:
                scheduled.append(self._send(transmission_id, "new"))
                continue

            last_identity = pending.attempts[-1]
            metrics = self.scheduler.carriers.get(last_identity)
            last_active = metrics is not None and metrics.active
            last_sent = pending.last_attempt_at
            timer_expired = (
                last_sent is not None
                and now - last_sent >= self.retransmit_after_s
            )

            if not last_active:
                scheduled.append(
                    self._send(transmission_id, "carrier-loss")
                )
            elif timer_expired:
                scheduled.append(
                    self._send(transmission_id, "timeout")
                )

        return tuple(scheduled)

    def acknowledge(
        self,
        stream_id: int,
        transmission_id: int,
        *,
        ack_carrier: tuple[int, int] | None,
    ) -> None:
        pending = self.ledger.pending.get(transmission_id)
        if pending is None:
            # Preserve TransmissionLedger duplicate / never-allocated behavior.
            self.ledger.settle(stream_id, transmission_id)
            return

        frame = pending.wire_frame or b""
        self.scheduler.settle(
            transmission_id,
            ack_identity=ack_carrier,
            ack_at=self.clock(),
            delivered_bytes=len(frame),
        )
        self.ledger.settle(stream_id, transmission_id)
