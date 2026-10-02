class MPXError(Exception):
    """Base class for MPX/4 reference errors."""


class DecodeError(MPXError):
    """Input is syntactically or semantically invalid."""


class NeedMoreData(MPXError):
    """The byte stream does not yet contain a complete protocol unit."""


class AuthenticationError(MPXError):
    """Handshake or Secure Record authentication failed."""
