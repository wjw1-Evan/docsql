"""DocSQL Python driver — DB-API 2.0 over the DocSQL v1 binary protocol.

Quick start::

    import docsql

    with docsql.connect(host="127.0.0.1", port=7600, token="...") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM users WHERE age > ?", (18,))
            for row in cur.fetchall():
                print(row)

Exact scalars round-trip: ``decimal.Decimal`` ↔ DECIMAL, ``datetime``
(UTC) ↔ TIMESTAMP, ``bytes`` ↔ BLOB. Parameters bind server-side
(``paramstyle="qmark"``) — a bound value can never alter statement text.
"""

from ._convert import param_json, ts_ms  # re-exported for advanced callers
from ._proto import ProtocolError
from .connection import Connection, Cursor, connect
from .errors import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from .subscriber import Subscriber

__version__ = "0.1.0"

__all__ = [
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    "Connection",
    "Cursor",
    "Subscriber",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    "ProtocolError",
    "ts_ms",
    "param_json",
    "__version__",
]

apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"
