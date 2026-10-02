from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Callable

from .carrier import SecureCarrier
from .codec import decode_token_frame, encode_ping, encode_pong
from .constants import FrameType
from .errors import DecodeError
from .scheduler import AggregateScheduler


@dataclass(frozen=True)
class ProbeResult:
    carrier_id: int
    generation: int
    token: int
    rtt_s: float


class PathProber:
    def __init__(
        self,
        scheduler: AggregateScheduler,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.scheduler = scheduler
        self.clock = clock
        self._next_token = 1

    def probe(
        self,
        carrier: SecureCarrier,
        *,
        token: int | None = None,
    ) -> ProbeResult:
        if token is None:
            token = self._next_token
            self._next_token += 1

        started = self.clock()
        carrier.send(encode_ping(token))
        frames = carrier.recv()
        finished = self.clock()

        if len(frames) != 1 or frames[0].type != int(FrameType.PONG):
            raise DecodeError("expected one PONG during path probe")
        if decode_token_frame(frames[0]) != token:
            raise DecodeError("path-probe PONG token mismatch")

        rtt_s = max(finished - started, 0.000001)
        identity = (
            carrier.identity.carrier_id,
            carrier.identity.generation,
        )
        self.scheduler.observe_rtt(identity, rtt_s)
        return ProbeResult(
            carrier_id=identity[0],
            generation=identity[1],
            token=token,
            rtt_s=rtt_s,
        )


def respond_to_probe(
    carrier: SecureCarrier,
    *,
    delay_s: float = 0.0,
    sleeper: Callable[[float], None] = time.sleep,
) -> int:
    frames = carrier.recv()
    if len(frames) != 1 or frames[0].type != int(FrameType.PING):
        raise DecodeError("expected one PING during path probe")
    token = decode_token_frame(frames[0])
    if delay_s > 0:
        sleeper(delay_s)
    carrier.send(encode_pong(token))
    return token
