from __future__ import annotations

from dataclasses import dataclass
import os
import socket

from .carrier import (
    client_create_carrier,
    client_join_carrier,
    server_create_carrier,
    server_join_carrier,
)
from .constants import SchedulerID
from .measurement import PathProber, respond_to_probe
from .scheduler import AutoScheduler, ProtectScheduler, scheduler_for_id


SCHEDULER_NAMES = {
    "auto": SchedulerID.AUTO,
    "aggregate": SchedulerID.AGGREGATE,
    "protect": SchedulerID.PROTECT,
}


@dataclass(frozen=True)
class ProbeSessionResult:
    session_id: bytes
    scheduler: int
    scheduler_name: str
    carrier1_rtt_ms: float
    carrier2_rtt_ms: float
    mode: str
    selected_carrier: tuple[int, int]


@dataclass(frozen=True)
class ProbeServerResult:
    session_id: bytes
    scheduler: int
    scheduler_name: str
    carrier1_delay_ms: float
    carrier2_delay_ms: float


def scheduler_name(scheduler_id: int) -> str:
    try:
        return SchedulerID(scheduler_id).name.lower()
    except ValueError:
        return f"unknown-{scheduler_id}"


def run_probe_server(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    carrier1_delay_ms: float = 5.0,
    carrier2_delay_ms: float = 150.0,
) -> ProbeServerResult:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, port))
        listener.listen(2)

        create_sock, _ = listener.accept()
        try:
            carrier1, session = server_create_carrier(
                create_sock,
                transport_key,
                server_nonce=os.urandom(32),
            )

            join_sock, _ = listener.accept()
            try:
                carrier2 = server_join_carrier(
                    join_sock,
                    transport_key,
                    session,
                    server_nonce=os.urandom(32),
                )
                respond_to_probe(
                    carrier1,
                    delay_s=max(carrier1_delay_ms, 0.0) / 1000.0,
                )
                respond_to_probe(
                    carrier2,
                    delay_s=max(carrier2_delay_ms, 0.0) / 1000.0,
                )
                return ProbeServerResult(
                    session_id=session.session_id,
                    scheduler=session.scheduler,
                    scheduler_name=scheduler_name(session.scheduler),
                    carrier1_delay_ms=carrier1_delay_ms,
                    carrier2_delay_ms=carrier2_delay_ms,
                )
            finally:
                join_sock.close()
        finally:
            create_sock.close()


def run_probe_client(
    host: str,
    port: int,
    transport_key: bytes,
    *,
    scheduler_name_value: str = "auto",
    candidate_bytes: int = 1200,
) -> ProbeSessionResult:
    if scheduler_name_value not in SCHEDULER_NAMES:
        raise ValueError("scheduler must be auto, aggregate, or protect")
    scheduler_id = int(SCHEDULER_NAMES[scheduler_name_value])
    session_id = os.urandom(16)

    create_sock = socket.create_connection((host, port), timeout=10)
    try:
        carrier1, session = client_create_carrier(
            create_sock,
            transport_key,
            session_id=session_id,
            client_nonce=os.urandom(32),
            scheduler=scheduler_id,
        )
        join_sock = socket.create_connection((host, port), timeout=10)
        try:
            carrier2 = client_join_carrier(
                join_sock,
                transport_key,
                session,
                carrier_id=2,
                generation=0,
                client_nonce=os.urandom(32),
            )

            scheduler = scheduler_for_id(scheduler_id)
            scheduler.register(carrier1)
            scheduler.register(carrier2)
            prober = PathProber(scheduler)
            probe1 = prober.probe(carrier1)
            probe2 = prober.probe(carrier2)

            mode = scheduler_name_value
            if isinstance(scheduler, AutoScheduler):
                mode = scheduler.refresh_mode()
            elif isinstance(scheduler, ProtectScheduler):
                fastest = min(
                    scheduler.carriers.values(),
                    key=lambda metrics: metrics.latest_rtt_s,
                )
                for metrics in scheduler.carriers.values():
                    scheduler.set_role(
                        metrics.identity,
                        "primary" if metrics.identity == fastest.identity else "backup",
                    )
                mode = "protect"

            selected = scheduler.choose(candidate_bytes).identity
            return ProbeSessionResult(
                session_id=session.session_id,
                scheduler=scheduler_id,
                scheduler_name=scheduler_name_value,
                carrier1_rtt_ms=probe1.rtt_s * 1000.0,
                carrier2_rtt_ms=probe2.rtt_s * 1000.0,
                mode=mode,
                selected_carrier=selected,
            )
        finally:
            join_sock.close()
    finally:
        create_sock.close()
