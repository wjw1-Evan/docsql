package io.docsql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Values {

    private Values() {}

    static final long TIMESTAMP_MIN_MS = -62_135_596_800_000L;
    static final long TIMESTAMP_MAX_MS = 253_402_300_799_999L;
    static final BigInteger DEC_MAX = BigInteger.ONE.shiftLeft(96).subtract(BigInteger.ONE);

    public static Result decodeRows(byte[] payload) {
        Object parsed = Json.parse(payload);
        if (!(parsed instanceof Map)) {
            throw new DocsqlException.Interface("malformed rows payload: not a JSON object");
        }
        Map<?, ?> body = (Map<?, ?>) parsed;
        Object cols = body.get("columns");
        Object rows = body.get("rows");
        if (!(cols instanceof List) || !(rows instanceof List)) {
            throw new DocsqlException.Interface("malformed rows payload: columns/rows missing");
        }
        Result r = new Result();
        for (Object c : (List<?>) cols) {
            r.columns.add(String.valueOf(c));
        }
        for (Object rowObj : (List<?>) rows) {
            if (!(rowObj instanceof List)) {
                throw new DocsqlException.Interface("malformed rows payload: row not an array");
            }
            List<?> rowList = (List<?>) rowObj;
            Object[] row = new Object[rowList.size()];
            for (int i = 0; i < rowList.size(); i++) {
                row[i] = decodeMarker(rowList.get(i));
            }
            r.rows.add(row);
        }
        return r;
    }

    static Object decodeMarker(Object v) {
        if (!(v instanceof Map)) {
            return v;
        }
        Map<?, ?> m = (Map<?, ?>) v;
        if (m.size() != 1) {
            return v;
        }
        Map.Entry<?, ?> e = m.entrySet().iterator().next();
        String key = String.valueOf(e.getKey());
        Object val = e.getValue();
        if ("$dec".equals(key) && val instanceof String) {
            try {
                return new BigDecimal((String) val);
            } catch (NumberFormatException nfe) {
                return v;
            }
        }
        if ("$float".equals(key) && val instanceof String) {
            String t = (String) val;
            if ("NaN".equals(t)) {
                return Double.NaN;
            }
            if ("inf".equals(t)) {
                return Double.POSITIVE_INFINITY;
            }
            if ("-inf".equals(t)) {
                return Double.NEGATIVE_INFINITY;
            }
            return v;
        }
        if ("$ts".equals(key) && val instanceof Long) {
            long ms = (Long) val;
            if (ms >= TIMESTAMP_MIN_MS && ms <= TIMESTAMP_MAX_MS) {
                return new java.sql.Timestamp(ms);
            }
            return v;
        }
        if ("$bytes".equals(key) && val instanceof List) {
            List<?> items = (List<?>) val;
            byte[] out = new byte[items.size()];
            for (int i = 0; i < items.size(); i++) {
                Object o = items.get(i);
                if (!(o instanceof Long)) {
                    return v;
                }
                long b = (Long) o;
                if (b < 0 || b > 255) {
                    return v;
                }
                out[i] = (byte) b;
            }
            return out;
        }
        return v;
    }

    public static String paramJson(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? "true" : "false";
        }
        if (value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long) {
            return value.toString();
        }
        if (value instanceof BigInteger) {
            return "{\"$dec\":\"" + bigIntegerDecimalText((BigInteger) value) + "\"}";
        }
        if (value instanceof BigDecimal) {
            return "{\"$dec\":\"" + decimalText((BigDecimal) value) + "\"}";
        }
        if (value instanceof Float || value instanceof Double) {
            double d = ((Number) value).doubleValue();
            if (Double.isNaN(d)) {
                return "{\"$float\":\"NaN\"}";
            }
            if (d == Double.POSITIVE_INFINITY) {
                return "{\"$float\":\"inf\"}";
            }
            if (d == Double.NEGATIVE_INFINITY) {
                return "{\"$float\":\"-inf\"}";
            }
            return Json.write(d);
        }
        if (value instanceof String) {
            return Json.write(value);
        }
        if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            StringBuilder sb = new StringBuilder("{\"$bytes\":[");
            for (int i = 0; i < bytes.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(bytes[i] & 0xFF);
            }
            return sb.append("]}").toString();
        }
        if (value instanceof java.util.Date) {
            return tsMarkerFromMillis(((java.util.Date) value).getTime(), value);
        }
        if (value instanceof Instant) {
            return tsMarkerFromMillis(((Instant) value).toEpochMilli(), value);
        }
        if (value instanceof LocalDateTime) {
            return tsMarkerFromMillis(
                    ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli(), value);
        }
        if (value instanceof OffsetDateTime) {
            return tsMarkerFromMillis(((OffsetDateTime) value).toInstant().toEpochMilli(), value);
        }
        if (value instanceof ZonedDateTime) {
            return tsMarkerFromMillis(((ZonedDateTime) value).toInstant().toEpochMilli(), value);
        }
        if (value instanceof Map || value instanceof List || value instanceof Object[]) {
            Object doc = value instanceof Object[] ? java.util.Arrays.asList((Object[]) value) : value;
            return Json.write(doc);
        }
        throw new DocsqlException.Programming(
                "unsupported parameter type: " + value.getClass().getName());
    }

    private static String tsMarkerFromMillis(long ms, Object source) {
        if (ms < TIMESTAMP_MIN_MS || ms > TIMESTAMP_MAX_MS) {
            throw new DocsqlException.Data(
                    source + " is outside the engine TIMESTAMP domain (0001-01-01..9999-12-31 UTC)");
        }
        return "{\"$ts\":" + ms + "}";
    }

    private static String bigIntegerDecimalText(BigInteger v) {
        if (v.bitLength() > 96) {
            throw new DocsqlException.Data(
                    v + " exceeds DECIMAL precision; bind it as a string or scale it down");
        }
        return v.toString();
    }

    private static String decimalText(BigDecimal v) {
        int scale = v.scale();
        int digits = v.precision();
        if (scale > 28) {
            throw new DocsqlException.Data(
                    v + " exceeds DECIMAL precision (scale > 28); bind it as a string or scale it down");
        }
        if (scale < 0 && digits - scale > 29) {
            throw new DocsqlException.Data(
                    v + " exceeds DECIMAL precision; bind it as a string or scale it down");
        }
        if (scale < 0) {
            BigInteger scaled = v.unscaledValue();
            for (int i = 0; i < -scale; i++) {
                scaled = scaled.multiply(BigInteger.TEN);
            }
            if (scaled.bitLength() > 96) {
                throw new DocsqlException.Data(
                        v + " exceeds DECIMAL precision; bind it as a string or scale it down");
            }
            return scaled.toString();
        }
        if (v.unscaledValue().bitLength() > 96) {
            throw new DocsqlException.Data(
                    v + " exceeds DECIMAL precision; bind it as a string or scale it down");
        }
        return v.toPlainString();
    }
}