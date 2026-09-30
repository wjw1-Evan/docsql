"""Value conversion between the wire's JSON dialect and Python types.

The server serializes result rows through its own JSON writer
(`crates/docsql-core/src/json.rs`): exact scalars ride single-key marker
objects so they survive systems that only have IEEE doubles —

    {"$dec": "<decimal text>"}   DECIMAL  -> decimal.Decimal
    {"$ts": <millis>}            TIMESTAMP -> datetime (UTC)
    {"$bytes": [ints]}           BLOB     -> bytes
    {"$float": "NaN"|"inf"|"-inf"}  non-finite FLOAT -> float

The same markers are accepted in bound parameters (REQ_EXECUTE renders
typed literals server-side, strings are quote-escaped there — a bound
value can never break out of its literal).
"""

import json
import struct
from datetime import datetime, timedelta, timezone
from decimal import Decimal

# The engine's TIMESTAMP domain (year 0001..=9999, see core/value.rs).
# Out-of-domain markers stay plain dicts — the same policy the server's
# decoder applies (there is no canonical text form to round-trip them).
TIMESTAMP_MIN_MS = -62_135_596_800_000
TIMESTAMP_MAX_MS = 253_402_300_799_999

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


def rows_from_payload(payload):
    """Decode a RESP_ROWS payload: {"columns": [str], "rows": [[v]]}.
    Rows come back as tuples (DB-API convention)."""
    body = json.loads(payload.decode("utf-8"), object_hook=_marker_hook)
    columns = body.get("columns") or []
    rows = [tuple(r) for r in (body.get("rows") or [])]
    return columns, rows


def _marker_hook(obj):
    """json object_hook mirroring the server's decode_marker."""
    if len(obj) == 1:
        key, value = next(iter(obj.items()))
        if key == "$dec" and isinstance(value, str):
            try:
                return Decimal(value)
            except ArithmeticError:
                return obj
        if key == "$float" and isinstance(value, str):
            if value == "NaN":
                return float("nan")
            if value == "inf":
                return float("inf")
            if value == "-inf":
                return float("-inf")
            return obj
        if key == "$ts" and isinstance(value, bool):  # bool before int
            return obj
        if key == "$ts" and isinstance(value, int):
            if TIMESTAMP_MIN_MS <= value <= TIMESTAMP_MAX_MS:
                try:
                    return _EPOCH + timedelta(milliseconds=value)
                except (OverflowError, OSError, ValueError):
                    return obj
            return obj
        if key == "$bytes" and isinstance(value, list):
            if all(isinstance(i, int) and not isinstance(i, bool) and 0 <= i <= 255 for i in value):
                return bytes(value)
            return obj
    return obj


def ts_ms(dt):
    """UTC milliseconds for a datetime. Naive values are taken as UTC
    (no silent local-timezone conversion — the same contract as the .NET
    driver's Kind=Unspecified handling)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(round((dt - _EPOCH).total_seconds() * 1000.0))


def param_json(value):
    """Encode one bound parameter as wire JSON text (REQ_EXECUTE params)."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        # Outside int64 a bare JSON number would decay to an IEEE double
        # server-side; route through the exact $dec path instead.
        if _INT64_MIN <= value <= _INT64_MAX:
            return str(value)
        return '{"$dec":"' + str(value) + '"}'
    if isinstance(value, float):
        if value != value:  # NaN
            return '{"$float":"NaN"}'
        if value == float("inf"):
            return '{"$float":"inf"}'
        if value == float("-inf"):
            return '{"$float":"-inf"}'
        return _dump_json(value)
    if isinstance(value, Decimal):
        return '{"$dec":"' + str(value) + '"}'
    if isinstance(value, datetime):
        return '{"$ts":' + str(ts_ms(value)) + "}"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return '{"$bytes":[' + ",".join(str(b) for b in bytes(value)) + "]}"
    if isinstance(value, str):
        return _dump_json(value)
    if isinstance(value, (dict, list, tuple)):
        # JSON documents bind as their JSON text (JSON_EXTRACT can read
        # them back); there is no native object parameter type on the wire.
        return _dump_json(value if isinstance(value, (dict, list)) else list(value))
    raise TypeError(f"unsupported parameter type: {type(value).__name__}")


def _dump_json(value):
    # allow_nan=False: non-finite floats must go through $float markers,
    # never NaN literals (invalid JSON — the server would reject the frame).
    return json.dumps(value, ensure_ascii=False, allow_nan=False)


def execute_payload(handle, params):
    head = '{"handle":' + str(handle) + ',"params":['
    return (head + ",".join(param_json(p) for p in params) + "]}").encode("utf-8")
