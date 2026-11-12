package io.docsql;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Json {

    private Json() {}

    public static Object parse(byte[] bytes) {
        String s = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        return parse(s);
    }

    public static Object parse(String s) {
        if (s == null) {
            throw new DocsqlException.Interface("JSON text is null");
        }
        Parser p = new Parser(s);
        Object v = p.parseValue();
        p.expectEnd();
        return v;
    }

    private static void writeOther(StringBuilder sb, Object v) {
        if (v instanceof Map<?, ?>) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                writeString(sb, String.valueOf(e.getKey()));
                sb.append(':');
                writeValue(sb, e.getValue());
            }
            sb.append('}');
        } else if (v instanceof List<?>) {
            sb.append('[');
            boolean first = true;
            for (Object item : (List<?>) v) {
                if (first) {
                    first = false;
                } else {
                    sb.append(',');
                }
                writeValue(sb, item);
            }
            sb.append(']');
        } else {
            throw new DocsqlException.Data("cannot write JSON for " + v.getClass().getName());
        }
    }


    /** Write any JSON value: Map/List/String/Number/Boolean/null. Non-finite doubles are rejected. */
    public static String write(Object v) {
        StringBuilder sb = new StringBuilder();
        writeValue(sb, v);
        return sb.toString();
    }

    private static void writeValue(StringBuilder sb, Object v) {
        if (v == null) {
            sb.append("null");
        } else if (v instanceof String) {
            writeString(sb, (String) v);
        } else if (v instanceof Boolean) {
            sb.append(((Boolean) v) ? "true" : "false");
        } else if (v instanceof BigDecimal) {
            sb.append(((BigDecimal) v).toPlainString());
        } else if (v instanceof Number) {
            sb.append(v.toString());
        } else {
            writeOther(sb, v);
        }
    }

    /**
     * Write a JSON string: escape the quote, the backslash and control characters; pass all other
     * characters through (UTF-8 passthrough, ensure_ascii=False style). A lone surrogate is a data
     * error: it cannot encode to UTF-8 and would detonate later at frame encode time, outside the
     * caller's error handling.
     */
    private static void writeString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (i + 1 >= s.length() || !Character.isLowSurrogate(s.charAt(i + 1))) {
                    throw new DocsqlException.Data("cannot bind text with an unpaired surrogate");
                }
            }
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
    }

    private static final class Parser {
        private final String s;
        private int pos;

        Parser(String s) {
            this.s = s;
        }

        Object parseValue() {
            skipWs();
            char c = peek();
            if (c == '{') {
                return parseObject();
            }
            if (c == '[') {
                return parseArray();
            }
            if (c == '"') {
                return parseString();
            }
            if (c == 't') {
                expect("true");
                return Boolean.TRUE;
            }
            if (c == 'f') {
                expect("false");
                return Boolean.FALSE;
            }
            if (c == 'n') {
                expect("null");
                return null;
            }
            return parseNumber();
        }

        private void expectEnd() {
            skipWs();
            if (pos != s.length()) {
                throw new DocsqlException.Interface("trailing garbage after JSON value");
            }
        }

        private void skipWs() {
            while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) {
                pos++;
            }
        }

        private char peek() {
            if (pos >= s.length()) {
                throw new DocsqlException.Interface("truncated JSON");
            }
            return s.charAt(pos);
        }

        private void expect(String lit) {
            if (!s.startsWith(lit, pos)) {
                throw new DocsqlException.Interface("invalid JSON: expected " + lit);
            }
            pos += lit.length();
        }

        private Object parseObject() {
            Map<String, Object> map = new LinkedHashMap<>();
            pos++; // consume '{'
            skipWs();
            if (peek() == '}') {
                pos++;
                return map;
            }
            while (true) {
                skipWs();
                if (peek() != '"') {
                    throw new DocsqlException.Interface("invalid JSON: object keys must be strings");
                }
                String key = parseString();
                skipWs();
                if (peek() != ':') {
                    throw new DocsqlException.Interface("invalid JSON: expected ':' after key");
                }
                pos++;
                Object value = parseValue();
                map.put(key, value);
                skipWs();
                char c = peek();
                if (c == ',') {
                    pos++;
                } else if (c == '}') {
                    pos++;
                    return map;
                } else {
                    throw new DocsqlException.Interface("invalid JSON: expected ',' or '}' in object");
                }
            }
        }

        private Object parseArray() {
            List<Object> list = new ArrayList<>();
            pos++;
            skipWs();
            if (peek() == ']') {
                pos++;
                return list;
            }
            while (true) {
                skipWs();
                list.add(parseValue());
                skipWs();
                char c = peek();
                if (c == ',') {
                    pos++;
                } else if (c == ']') {
                    pos++;
                    return list;
                } else {
                    throw new DocsqlException.Interface("invalid JSON: expected ',' or ']' in array");
                }
            }
        }

        private String parseString() {
            pos++; // consume the opening quote
            StringBuilder sb = new StringBuilder();
            while (true) {
                char c = s.charAt(pos++);
                if (c == '"') {
                    return sb.toString();
                }
                if (c == '\\') {
                    char e = s.charAt(pos++);
                    switch (e) {
                        case '"':
                            sb.append('"');
                            break;
                        case '\\':
                            sb.append('\\');
                            break;
                        case '/':
                            sb.append('/');
                            break;
                        case 'b':
                            sb.append('\b');
                            break;
                        case 't':
                            sb.append('\t');
                            break;
                        case 'n':
                            sb.append('\n');
                            break;
                        case 'f':
                            sb.append('\f');
                            break;
                        case 'r':
                            sb.append('\r');
                            break;
                        case 'u':
                            if (pos + 4 > s.length()) {
                                throw new DocsqlException.Interface("truncated \\u escape");
                            }
                            String hex = s.substring(pos, pos + 4);
                            sb.append((char) Integer.parseInt(hex, 16));
                            pos += 4;
                            break;
                        default:
                            throw new DocsqlException.Interface("invalid JSON escape \\" + e);
                    }
                    continue;
                }
                sb.append(c);
            }
        }

        private Object parseNumber() {
            int start = pos;
            if (pos < s.length() && (s.charAt(pos) == '-')) {
                pos++;
            }
            while (pos < s.length() && Character.isDigit(s.charAt(pos))) {
                pos++;
            }
            boolean isDouble = false;
            if (pos < s.length() && s.charAt(pos) == '.') {
                isDouble = true;
                pos++;
            }
            while (pos < s.length() && Character.isDigit(s.charAt(pos))) {
                pos++;
            }
            if (pos < s.length() && (s.charAt(pos) == 'e' || s.charAt(pos) == 'E')) {
                isDouble = true;
                pos++;
                if (pos < s.length() && (s.charAt(pos) == '+' || s.charAt(pos) == '-')) {
                    pos++;
                }
        while (pos < s.length() && Character.isDigit(s.charAt(pos))) {
                    pos++;
                }
            }
            String text = s.substring(start, pos);
            if (text.isEmpty() || text.equals("-")) {
                throw new DocsqlException.Interface("invalid JSON number at " + start);
            }
            if (isDouble) {
                return Double.parseDouble(text);
            }
            try {
                return Long.parseLong(text);
            } catch (NumberFormatException e) {
                return new BigInteger(text);
          }
        }
    }
}