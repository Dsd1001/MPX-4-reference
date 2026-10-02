class MPXError(Exception):
    """Base class for MPX/4 reference errors."""


class DecodeError(MPXError):
    """Input is syntactically or semantically invalid."""


class NeedMoreData(MPXError):
    """The byte stream does not yet contain a complete protocol unit."""


class AuthenticationError(MPXError):
    """Handshake or Secure Record authentication failed."""


class FlowControlError(MPXError):
    """Peer exceeded advertised Stream or Session credit."""


class StreamStateError(MPXError):
    """Frame is impossible in the current Stream lifecycle state."""


class FinalSizeError(MPXError):
    """Frame contradicts an established Stream final size."""


class TransmissionIDError(MPXError):
    """Transmission identity is conflicting or impossible."""
