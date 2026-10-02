from __future__ import annotations

from dataclasses import dataclass, field

from .carrier import SecureCarrier
from .constants import SchedulerID
from .errors import CarrierLostError


@dataclass
class CarrierMetrics:
    carrier: SecureCarrier
    latest_rtt_s: float = 0.050
    min_rtt_s: float = 0.050
    delivery_rate_bps: float = 1_000_000.0
    outstanding_bytes: int = 0
    configured_capacity_bps: float | None = None
    role: str = "active"
    failures: int = 0
    penalty_s: float = 0.0

    @property
    def identity(self) -> tuple[int, int]:
        return (
            self.carrier.identity.carrier_id,
            self.carrier.identity.generation,
        )

    @property
    def active(self) -> bool:
        return self.carrier.active and self.role != "disabled"

    def effective_rate_bps(self) -> float:
        rate = max(self.delivery_rate_bps, 1.0)
        if self.configured_capacity_bps is not None:
            rate = min(rate, max(self.configured_capacity_bps, 1.0))
        return rate

    def predicted_completion_s(self, frame_bytes: int) -> float:
        queue_delay = (self.outstanding_bytes + frame_bytes) / self.effective_rate_bps()
        return (self.latest_rtt_s / 2.0) + queue_delay + self.penalty_s


@dataclass
class AttemptAccounting:
    identity: tuple[int, int]
    wire_bytes: int
    sent_at: float


@dataclass
class AggregateScheduler:
    carriers: dict[tuple[int, int], CarrierMetrics] = field(default_factory=dict)
    attempts: dict[int, list[AttemptAccounting]] = field(default_factory=dict)

    def register(
        self,
        carrier: SecureCarrier,
        *,
        latest_rtt_s: float = 0.050,
        delivery_rate_bps: float = 1_000_000.0,
        configured_capacity_bps: float | None = None,
        role: str = "active",
    ) -> CarrierMetrics:
        identity = (
            carrier.identity.carrier_id,
            carrier.identity.generation,
        )
        metrics = CarrierMetrics(
            carrier=carrier,
            latest_rtt_s=max(latest_rtt_s, 0.000001),
            min_rtt_s=max(latest_rtt_s, 0.000001),
            delivery_rate_bps=max(delivery_rate_bps, 1.0),
            configured_capacity_bps=configured_capacity_bps,
            role=role,
        )
        self.carriers[identity] = metrics
        return metrics

    def metrics_for(self, identity: tuple[int, int]) -> CarrierMetrics:
        return self.carriers[identity]

    def set_role(self, identity: tuple[int, int], role: str) -> None:
        if role not in {"active", "primary", "backup", "degraded", "disabled"}:
            raise ValueError("invalid scheduler role")
        self.carriers[identity].role = role

    def mark_failure(self, identity: tuple[int, int], *, penalty_s: float = 0.250) -> None:
        metrics = self.carriers.get(identity)
        if metrics is None:
            return
        metrics.failures += 1
        metrics.penalty_s = max(metrics.penalty_s, penalty_s)
        metrics.carrier.mark_lost()

    def observe_rtt(self, identity: tuple[int, int], rtt_s: float) -> None:
        metrics = self.carriers[identity]
        rtt_s = max(rtt_s, 0.000001)
        metrics.latest_rtt_s = rtt_s
        metrics.min_rtt_s = min(metrics.min_rtt_s, rtt_s)

    @staticmethod
    def _best(candidates: list[CarrierMetrics], frame_bytes: int) -> CarrierMetrics:
        return min(
            candidates,
            key=lambda metrics: (
                metrics.predicted_completion_s(frame_bytes),
                metrics.carrier.identity.carrier_id,
                metrics.carrier.identity.generation,
            ),
        )

    def _active(self) -> list[CarrierMetrics]:
        active = [metrics for metrics in self.carriers.values() if metrics.active]
        if not active:
            raise CarrierLostError("no active Carrier is available")
        return active

    def choose(
        self,
        frame_bytes: int,
        *,
        previous_attempts: tuple[tuple[int, int], ...] = (),
    ) -> CarrierMetrics:
        active = self._active()
        previous = set(previous_attempts)
        unused = [metrics for metrics in active if metrics.identity not in previous]
        candidates = unused or active
        return self._best(candidates, frame_bytes)

    def record_attempt(
        self,
        transmission_id: int,
        identity: tuple[int, int],
        wire_bytes: int,
        sent_at: float,
    ) -> None:
        metrics = self.carriers[identity]
        metrics.outstanding_bytes += wire_bytes
        self.attempts.setdefault(transmission_id, []).append(
            AttemptAccounting(identity, wire_bytes, sent_at)
        )

    def settle(
        self,
        transmission_id: int,
        *,
        ack_identity: tuple[int, int] | None,
        ack_at: float,
        delivered_bytes: int,
    ) -> None:
        attempts = self.attempts.pop(transmission_id, [])
        for attempt in attempts:
            metrics = self.carriers.get(attempt.identity)
            if metrics is not None:
                metrics.outstanding_bytes = max(
                    0,
                    metrics.outstanding_bytes - attempt.wire_bytes,
                )

        # Draft 03 only permits an unambiguous path-specific delivery sample
        # when there was exactly one Attempt and the acknowledgement returns
        # on that same Carrier.
        if (
            len(attempts) == 1
            and ack_identity is not None
            and attempts[0].identity == ack_identity
        ):
            attempt = attempts[0]
            metrics = self.carriers.get(attempt.identity)
            if metrics is None:
                return
            sample_rtt = max(ack_at - attempt.sent_at, 0.000001)
            self.observe_rtt(attempt.identity, sample_rtt)
            sample_rate = delivered_bytes / sample_rtt if delivered_bytes > 0 else 0.0
            if sample_rate > 0:
                metrics.delivery_rate_bps = (
                    0.75 * metrics.delivery_rate_bps
                    + 0.25 * sample_rate
                )


@dataclass
class ProtectScheduler(AggregateScheduler):
    """Reference PROTECT policy.

    New Transmissions stay on primary/active paths. Later Attempts may move to
    an unused backup/degraded path to preserve reliability.
    """

    def choose(
        self,
        frame_bytes: int,
        *,
        previous_attempts: tuple[tuple[int, int], ...] = (),
    ) -> CarrierMetrics:
        active = self._active()
        previous = set(previous_attempts)

        if not previous:
            primary = [
                metrics
                for metrics in active
                if metrics.role in {"primary", "active"}
            ]
            return self._best(primary or active, frame_bytes)

        unused = [metrics for metrics in active if metrics.identity not in previous]
        candidates = unused or active
        primary_unused = [
            metrics
            for metrics in candidates
            if metrics.role in {"primary", "active"}
        ]
        if primary_unused:
            candidates = primary_unused
        return self._best(candidates, frame_bytes)


@dataclass
class AutoScheduler(AggregateScheduler):
    """Reference AUTO policy switching between aggregate and protect behavior."""

    rtt_ratio_threshold: float = 2.0
    penalty_threshold_s: float = 0.050
    mode: str = "aggregate"

    def refresh_mode(self) -> str:
        active = self._active()
        if len(active) < 2:
            self.mode = "aggregate"
            return self.mode

        minimum = min(metrics.latest_rtt_s for metrics in active)
        maximum = max(metrics.latest_rtt_s for metrics in active)
        diverged = maximum / max(minimum, 0.000001) >= self.rtt_ratio_threshold
        unhealthy = any(
            metrics.failures > 0
            or metrics.penalty_s >= self.penalty_threshold_s
            or metrics.role == "degraded"
            for metrics in active
        )
        self.mode = "protect" if diverged or unhealthy else "aggregate"
        return self.mode

    def choose(
        self,
        frame_bytes: int,
        *,
        previous_attempts: tuple[tuple[int, int], ...] = (),
    ) -> CarrierMetrics:
        mode = self.refresh_mode()
        if mode == "aggregate":
            return super().choose(
                frame_bytes,
                previous_attempts=previous_attempts,
            )

        active = self._active()
        previous = set(previous_attempts)
        if previous:
            unused = [metrics for metrics in active if metrics.identity not in previous]
            if unused:
                return min(
                    unused,
                    key=lambda metrics: (
                        metrics.latest_rtt_s + metrics.penalty_s,
                        metrics.predicted_completion_s(frame_bytes),
                        metrics.identity,
                    ),
                )

        return min(
            active,
            key=lambda metrics: (
                metrics.latest_rtt_s + metrics.penalty_s,
                metrics.predicted_completion_s(frame_bytes),
                metrics.identity,
            ),
        )


@dataclass
class WeightedScheduler(AggregateScheduler):
    """Reference WEIGHTED policy using configured capacity as a hard rate clamp."""

    def register(
        self,
        carrier: SecureCarrier,
        *,
        latest_rtt_s: float = 0.050,
        delivery_rate_bps: float = 1_000_000.0,
        configured_capacity_bps: float | None = None,
        role: str = "active",
    ) -> CarrierMetrics:
        if configured_capacity_bps is None or configured_capacity_bps <= 0:
            raise ValueError("WEIGHTED requires configured capacity")
        return super().register(
            carrier,
            latest_rtt_s=latest_rtt_s,
            delivery_rate_bps=delivery_rate_bps,
            configured_capacity_bps=configured_capacity_bps,
            role=role,
        )


def scheduler_for_id(scheduler_id: int):
    if scheduler_id == int(SchedulerID.AGGREGATE):
        return AggregateScheduler()
    if scheduler_id == int(SchedulerID.PROTECT):
        return ProtectScheduler()
    if scheduler_id == int(SchedulerID.AUTO):
        return AutoScheduler()
    if scheduler_id == int(SchedulerID.WEIGHTED):
        return WeightedScheduler()
    raise ValueError("unsupported Scheduler ID")
