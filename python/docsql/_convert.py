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

from .errors import DataError

# The engine's TIMESTAMP domain (year 0001..=9999, see core/value.rs).
# Out-of-domain markers stay plain dicts — the same policy the server's
# decoder applies (there is no canonical text form to round-trip them).
TIMESTAMP_MIN_MS = -62_135_596_800_000
TIMESTAMP_MAX_MS = 253_402_300_799_999

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

# rust_decimal's maximum unscaled value (2^96 - 1): anything beyond it
# cannot parse server-side, where the $dec fallback silently binds the whole
# marker object as a TEXT literal — reject loudly here instead.
_DEC_MAX = 2**96 - 1


def _exact_decimal_text(value):
    """Text for a Decimal-typed bind, rejecting the out-of-domain loudly.

    The server parses $dec with rust_decimal's exact string path: unscaled
    mantissa <= 2^96-1 AND scale <= 28. A magnitude-only check used to pass
    38-digit sub-unity fractions whose mantissa cannot fit — the server's
    marker fallback then bound the whole marker OBJECT as a TEXT literal
    (silent type corruption). NaN/sNaN are rejected first: they raise
    InvalidOperation on comparisons, which used to escape execute() as a
    non-DB-API exception."""
    if isinstance(value, int):
        # The > int64 integer route: whole digits only, no scale concerns.
        if value > _DEC_MAX or value < -_DEC_MAX:
            raise DataError(
                f"{value} exceeds DECIMAL precision (max {_DEC_MAX}); "
                "bind it as a string or scale it down"
            )
        return str(value)
    if not value.is_finite():
        raise DataError(
            f"{value} is not a finite decimal; DECIMAL binds reject NaN/Infinity"
        )
    _sign, digits, exp = value.as_tuple()
    digit_text = "".join(map(str, digits)) or "0"
    if exp >= 0:
        # Reject BEFORE materializing the mantissa: int(digit_text) * 10**exp
        # on something like Decimal("1E+999999999") first builds the whole
        # billion-digit power (client hang / OOM) and only THEN fails the
        # _DEC_MAX check below. The digit-count bound is exact, not a
        # heuristic: _DEC_MAX has 29 digits, so 30+ whole digits always
        # exceed it (and <=29-digit powers are cheap to build).
        if len(digit_text) + exp > 29:
            raise DataError(
                f"{value} exceeds DECIMAL precision (28 significant digits, "
                f"max {_DEC_MAX}); bind it as a string or scale it down"
            )
        mantissa, scale = int(digit_text) * 10**exp, 0
    else:
        mantissa, scale = int(digit_text), -exp
    if scale > 28 or mantissa > _DEC_MAX:
        raise DataError(
            f"{value} exceeds DECIMAL precision (28 significant digits, "
            f"max {_DEC_MAX}); bind it as a string or scale it down"
        )
    # Plain fixed-point text: rust_decimal's FromStr only takes the LOSSY
    # scientific path for e/E spellings (str(Decimal) goes scientific below
    # ~1e-6 / above ~1e16).
    return format(value, "f")


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
        # server-side; route through the exact $dec path instead (rejecting
        # the values even $dec cannot represent — see _exact_decimal_text).
        if _INT64_MIN <= value <= _INT64_MAX:
            return str(value)
        return '{"$dec":"' + _exact_decimal_text(value) + '"}'
    if isinstance(value, float):
        if value != value:  # NaN
            return '{"$float":"NaN"}'
        if value == float("inf"):
            return '{"$float":"inf"}'
        if value == float("-inf"):
            return '{"$float":"-inf"}'
        return _dump_json(value)
    if isinstance(value, Decimal):
        return '{"$dec":"' + _exact_decimal_text(value) + '"}'
    if isinstance(value, datetime):
        ms = ts_ms(value)
        if not (TIMESTAMP_MIN_MS <= ms <= TIMESTAMP_MAX_MS):
            # The server keeps out-of-domain $ts markers as plain objects,
            # which then bind as TEXT — silent type corruption on a legal
            # Python datetime (e.g. 9999-12-31 23:59:59.999999+ rounds one
            # millisecond past the domain edge). Reject at the bind.
            raise DataError(
                f"{value} is outside the engine TIMESTAMP domain "
                "(0001-01-01..9999-12-31 UTC); store it as a string"
            )
        return '{"$ts":' + str(ms) + "}"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return '{"$bytes":[' + ",".join(str(b) for b in bytes(value)) + "]}"
    if isinstance(value, str):
        return _dump_json(value)
    if isinstance(value, (dict, list, tuple)):
        # JSON documents bind as their JSON text (JSON_EXTRACT can read
        # them back); there is no native object parameter type on the wire.
        # A non-finite float NESTED inside the document fails _dump_json
        # with a bare ValueError — the top-level NaN already raises
        # DataError, and the nested shape must not leak a non-DB-API
        # exception out of execute() (it would bypass `except Error`
        # recovery paths in user code).
        seq = value if isinstance(value, (dict, list)) else list(value)
        try:
            return _dump_json(seq)
        except ValueError as exc:
            raise DataError(
                f"cannot bind document with non-finite float: {exc}"
            ) from exc
    raise TypeError(f"unsupported parameter type: {type(value).__name__}")


def _dump_json(value):
    # allow_nan=False: non-finite floats must go through $float markers,
    # never NaN literals (invalid JSON — the server would reject the frame).
    return json.dumps(value, ensure_ascii=False, allow_nan=False)


def execute_payload(handle, params):
    head = '{"handle":' + str(handle) + ',"params":['
    text = head + ",".join(param_json(p) for p in params) + "]}"
    try:
        return text.encode("utf-8")
    except UnicodeEncodeError as exc:
        # Lone surrogates ride str binds through json.dumps(ensure_ascii=
        # False) untouched and only detonate at this final encode — as a
        # bare UnicodeEncodeError it used to escape execute() past every
        # ``except Error`` recovery path in user code.
        raise DataError(
            f"cannot bind parameter text (unpaired surrogate is not UTF-8): {exc}"
        ) from exc
