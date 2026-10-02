from __future__ import annotations

from dataclasses import dataclass, field

from .carrier import ClientSessionState, SecureCarrier, ServerSessionState
from .codec import CloseFrame, Frame, decode_close, encode_close
from .constants import ErrorCode, FrameType
from .errors import CarrierLostError, DecodeError, SessionConflictError


SessionState = ClientSessionState | ServerSessionState


@dataclass
class SessionCloseController:
    state: SessionState
    carriers: dict[tuple[int, int], SecureCarrier] = field(default_factory=dict)

    def add_carrier(self, carrier: SecureCarrier) -> None:
        if self.state.lifecycle != "active":
            raise SessionConflictError(
                "cannot add Carrier to a closing or closed Session"
            )
        identity = (
            carrier.identity.carrier_id,
            carrier.identity.generation,
        )
        self.carriers[identity] = carrier

    def ensure_new_stream_allowed(self) -> None:
        self.state.ensure_stream_creation_allowed()

    def _mark(
        self,
        carrier: SecureCarrier,
        kind: str,
        close: CloseFrame,
    ) -> None:
        carrier.mark_graceful_close(
            kind,
            error_code=close.error_code,
            trigger_frame_type=close.trigger_frame_type,
            reason=close.reason,
        )

    def send_carrier_close(
        self,
        carrier: SecureCarrier,
        *,
        error_code: int = int(ErrorCode.NO_ERROR),
        trigger_frame_type: int = 0,
        reason: str = "",
    ) -> CloseFrame:
        if self.state.lifecycle != "active":
            raise SessionConflictError(
                "cannot independently close a Carrier after Session closure begins"
            )
        close = CloseFrame(error_code, trigger_frame_type, reason)
        wire = encode_close(
            FrameType.CARRIER_CLOSE,
            error_code,
            trigger_frame_type,
            reason,
        )
        carrier.send(wire)
        self._mark(carrier, "carrier", close)
        return close

    def receive_carrier_close(
        self,
        carrier: SecureCarrier,
        frame: Frame,
    ) -> CloseFrame:
        if frame.type != int(FrameType.CARRIER_CLOSE):
            raise DecodeError("expected CARRIER_CLOSE")
        close = decode_close(frame)
        self._mark(carrier, "carrier", close)
        return close

    def send_session_close(
        self,
        *,
        error_code: int = int(ErrorCode.NO_ERROR),
        trigger_frame_type: int = 0,
        reason: str = "",
    ) -> tuple[CloseFrame, int]:
        close = CloseFrame(error_code, trigger_frame_type, reason)

        # A repeated local close is idempotent and does not create new Records.
        if self.state.lifecycle == "closed":
            return close, 0

        self.state.begin_close()
        wire = encode_close(
            FrameType.SESSION_CLOSE,
            error_code,
            trigger_frame_type,
            reason,
        )

        sent = 0
        for carrier in tuple(self.carriers.values()):
            if not carrier.active:
                continue
            try:
                carrier.send(wire)
            except CarrierLostError:
                continue
            sent += 1
            self._mark(carrier, "session", close)

        # Any remaining Carrier becomes unusable once Session closure is
        # committed locally, even if the close Record could not be delivered.
        for carrier in self.carriers.values():
            if carrier.active:
                self._mark(carrier, "session", close)

        self.state.finish_close()
        return close, sent

    def receive_session_close(
        self,
        carrier: SecureCarrier,
        frame: Frame,
    ) -> CloseFrame:
        if frame.type != int(FrameType.SESSION_CLOSE):
            raise DecodeError("expected SESSION_CLOSE")
        close = decode_close(frame)

        if self.state.lifecycle == "closed":
            # Repeated SESSION_CLOSE is explicitly idempotent.
            return close

        self.state.begin_close()
        for known in self.carriers.values():
            if known.active or not known.gracefully_closed:
                self._mark(known, "session", close)
        if (
            carrier.identity.carrier_id,
            carrier.identity.generation,
        ) not in self.carriers:
            self._mark(carrier, "session", close)
        self.state.finish_close()
        return close

    def handle_close_frame(
        self,
        carrier: SecureCarrier,
        frame: Frame,
    ) -> CloseFrame:
        if frame.type == int(FrameType.CARRIER_CLOSE):
            return self.receive_carrier_close(carrier, frame)
        if frame.type == int(FrameType.SESSION_CLOSE):
            return self.receive_session_close(carrier, frame)
        raise DecodeError("Frame is not a close Frame")
