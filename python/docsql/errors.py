"""DB-API 2.0 (PEP 249) exception hierarchy, mapped from server replies.

The mapping is heuristic over the server's error TEXT (the v1 protocol has
no structured error codes); unrecognized texts raise DatabaseError. The
connection is NOT automatically closed on DatabaseError subclasses — only
transport failures (InterfaceError) leave it unusable, matching the
server's per-statement error semantics.
"""


class Error(Exception):
    """DB-API base; carries the raw server message on `.message`."""

    def __init__(self, message=""):
        super().__init__(message)
        self.message = message


class InterfaceError(Error):
    """Transport/protocol failure — the connection must be discarded."""


class DatabaseError(Error):
    """Server-side statement failure (parse, constraint, execution…)."""


class DataError(DatabaseError):
    """Value out of domain (oversized frame payload, bad literal…)."""


class OperationalError(DatabaseError):
    """Connection/credential/timeout trouble (reconnectable)."""


class IntegrityError(DatabaseError):
    """Constraint violation (UNIQUE / FOREIGN KEY / CHECK / NOT NULL)."""


class InternalError(DatabaseError):
    """The driver was used against a closed/invalid object."""


class ProgrammingError(DatabaseError):
    """SQL syntax/parse errors and driver misuse (wrong arity…)."""


class NotSupportedError(DatabaseError):
    """The server or driver does not support the requested feature."""


# Substrings the server's messages use for each category. Ordered: the
# first hit wins; integrity check runs before programming so
# "UNIQUE constraint" is not misread as a parse problem.
_INTEGRITY_MARKERS = ("unique", "foreign key", "check constraint", "not null")
_PROGRAMMING_MARKERS = ("parse", "syntax", "expected", "unknown ", "unrecognized")
_DATA_MARKERS = ("too large", "exceeds", "out of range", "invalid date")


def map_server_error(text):
    lowered = (text or "").lower()
    if any(m in lowered for m in _INTEGRITY_MARKERS):
        return IntegrityError(text)
    if any(m in lowered for m in _DATA_MARKERS):
        return DataError(text)
    if any(m in lowered for m in _PROGRAMMING_MARKERS):
        return ProgrammingError(text)
    return DatabaseError(text)
