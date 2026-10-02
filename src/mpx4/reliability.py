from __future__ import annotations

from .carrier import SecureCarrier
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
