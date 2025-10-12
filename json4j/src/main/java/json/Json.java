package json;

import java.beans.Introspector;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Minimal, standard-first JSON writer and parser.
 *
 * @author <a href="mailto:llw599502537@gmail.com">Freeman</a>
 */
public final class Json {

    private Json() {
        throw new UnsupportedOperationException();
    }

    // ============================================================
    // Public API
    // ============================================================

    /**
     * Serialize any Java object to a JSON string.
     *
     * <h3>Example</h3>
     * <pre>{@code
     * Point point = new Point(42, 21);
     * String json = Json.stringify(point);
     * // -> {"x":42,"y":21}
     * }</pre>
     *
     * @param o any object, may be {@code null}
     * @return non-null JSON text
     */
    public static String stringify(Object o) {
        StringBuilder sb = new StringBuilder();
        new JsonWriter(sb).writeAny(o);
        return sb.toString();
    }

    /**
     * Parse JSON text into a target type described by a {@link Type} token.
     *
     * <h3>Example</h3>
     * <pre>{@code
     * String json = "{\"x\":42,\"y\":21}";
     * Point point = Json.parse(json, new Json.Type<Point>() {});
     * // -> Point{x=42, y=21}
     * String json = "[{\"x\":42,\"y\":21}, {\"x\":1,\"y\":2}]";
     * List<Point> points = Json.parse(json, new Json.Type<List<Point>>() {});
     * // -> [Point{x=42, y=21}, Point{x=1, y=2}]
     * }</pre>
     *
     * @param json JSON text, not {@code null}
     * @param type target type token, not {@code null}
     * @param <T>  result type
     * @return parsed instance (maybe {@code null} if json is "null")
     */
    public static <T> T parse(String json, Type<T> type) {
        Objects.requireNonNull(json, "json");
        Objects.requireNonNull(type, "type");
        var jv = new Parser(new Lexer(json)).parse();
        return fromJsonValue(jv, canonicalize(type.getType()));
    }

    /**
     * Parse JSON text into a target type.
     *
     * <p> This is a convenience overload of {@link #parse(String, Type)}
     *
     * @param json JSON text, not {@code null}
     * @param clazz target class, not {@code null}
     * @param <T>  result type
     * @return parsed instance (maybe {@code null} if json is "null")
     */
    public static <T> T parse(String json, Class<T> clazz) {
        Objects.requireNonNull(clazz, "clazz");
        return parse(json, Type.of(clazz));
    }

    // ============================================================
    // AST
    // ============================================================

    sealed interface JsonValue permits JsonArray, JsonBoolean, JsonNull, JsonNumber, JsonObject, JsonString {}

    record JsonArray(List<JsonValue> value) implements JsonValue {}

    record JsonBoolean(boolean value) implements JsonValue {}

    record JsonNull() implements JsonValue {}

    record JsonNumber(Number value) implements JsonValue {}

    record JsonObject(Map<String, JsonValue> value) implements JsonValue {}

    record JsonString(String value) implements JsonValue {}

    // ============================================================
    // Type token
    // ============================================================

    public abstract static class Type<T> {
        private final java.lang.reflect.Type type;

        protected Type() {
            Class<?> c = findTypeSubclass(getClass());
            var p = (ParameterizedType) c.getGenericSuperclass();
            this.type = p.getActualTypeArguments()[0];
        }

        private Type(java.lang.reflect.Type t) {
            this.type = t;
        }

        public static <T> Type<T> of(Class<T> clazz) {
            return new Type<>(clazz) {};
        }

        public java.lang.reflect.Type getType() {
            return type;
        }

        private static Class<?> findTypeSubclass(Class<?> child) {
            Class<?> parent = child.getSuperclass();
            if (parent == Type.class) return child;
            if (parent == Object.class) throw new IllegalStateException("Expected Json.Type superclass");
            return findTypeSubclass(parent);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Type<?> t && Objects.equals(type, t.type);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(type);
        }

        @Override
        public String toString() {
            return "Type{" + type + '}';
        }
    }

    // ============================================================
    // Lexer / Parser
    // ============================================================

    enum Token {
        LBRACE,
        RBRACE,
        LBRACKET,
        RBRACKET,
        COLON,
        COMMA,
        STRING,
        NUMBER,
        TRUE,
        FALSE,
        NULL,
        EOF
    }

    static final class Lexer {
        private final String s;
        private int i = 0, line = 1, col = 1;
        private Token current;
        private String stringValue, numberLexeme;

        Lexer(String s) {
            this.s = Objects.requireNonNull(s);
            advance();
        }

        Token current() {
            return current;
        }

        String string() {
            return stringValue;
        }

        String number() {
            return numberLexeme;
        }

        int line() {
            return line;
        }

        int col() {
            return col;
        }

        void advance() {
            skipWs();
            if (eof()) {
                current = Token.EOF;
                return;
            }
            char c = peek();
            switch (c) {
                case '{' -> {
                    consume();
                    current = Token.LBRACE;
                }
                case '}' -> {
                    consume();
                    current = Token.RBRACE;
                }
                case '[' -> {
                    consume();
                    current = Token.LBRACKET;
                }
                case ']' -> {
                    consume();
                    current = Token.RBRACKET;
                }
                case ':' -> {
                    consume();
                    current = Token.COLON;
                }
                case ',' -> {
                    consume();
                    current = Token.COMMA;
                }
                case '"' -> {
                    stringValue = readString();
                    current = Token.STRING;
                }
                case 't' -> {
                    readKeyword("true");
                    current = Token.TRUE;
                }
                case 'f' -> {
                    readKeyword("false");
                    current = Token.FALSE;
                }
                case 'n' -> {
                    readKeyword("null");
                    current = Token.NULL;
                }
                default -> {
                    if (c == '-' || isDigit(c)) {
                        numberLexeme = readNumber();
                        current = Token.NUMBER;
                    } else error("Unexpected character: '" + c + "'");
                }
            }
        }

        private void skipWs() {
            while (!eof()) {
                char c = peek();
                if (c == ' ' || c == '\t' || c == '\r') consume();
                else if (c == '\n') {
                    consume();
                    line++;
                    col = 1;
                } else break;
            }
        }

        private String readString() {
            consume(); // opening "
            StringBuilder sb = new StringBuilder();
            while (!eof()) {
                char c = consume();
                if (c == '"') return sb.toString();
                if (c == '\\') {
                    if (eof()) error("Unterminated escape");
                    char e = consume();
                    switch (e) {
                        case '"' -> sb.append('"');
                        case '\\' -> sb.append('\\');
                        case '/' -> sb.append('/');
                        case 'b' -> sb.append('\b');
                        case 'f' -> sb.append('\f');
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case 'u' -> {
                            int cp = readHex4();
                            if (Character.isHighSurrogate((char) cp)) {
                                if (peek() == '\\' && peekNext() == 'u') {
                                    consume();
                                    consume();
                                    int low = readHex4();
                                    if (!Character.isLowSurrogate((char) low)) error("Invalid low surrogate");
                                    sb.appendCodePoint(Character.toCodePoint((char) cp, (char) low));
                                } else error("High surrogate not followed by low surrogate");
                            } else if (Character.isLowSurrogate((char) cp)) {
                                error("Unexpected low surrogate");
                            } else sb.append((char) cp);
                        }
                        default -> error("Invalid escape: \\" + e);
                    }
                } else {
                    if (c < 0x20) error("Control char in string");
                    sb.append(c);
                }
            }
            error("Unterminated string");
            return null;
        }

        private int readHex4() {
            int cp = 0;
            for (int k = 0; k < 4; k++) {
                if (eof()) error("Unexpected end in \\u escape");
                int v = hexVal(consume());
                if (v < 0) error("Invalid hex in \\u escape");
                cp = (cp << 4) | v;
            }
            return cp;
        }

        private static int hexVal(char c) {
            if (c >= '0' && c <= '9') return c - '0';
            if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
            if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
            return -1;
        }

        private void readKeyword(String kw) {
            for (int k = 0; k < kw.length(); k++) {
                if (eof() || peek() != kw.charAt(k)) error("Invalid literal, expected '" + kw + "'");
                consume();
            }
        }

        private String readNumber() {
            int start = i;
            if (peek() == '-') consume();
            if (eof()) error("Unexpected end in number");
            if (peek() == '0') consume();
            else if (isDigit(peek())) while (!eof() && isDigit(peek())) consume();
            else error("Invalid number (int part)");
            if (!eof() && peek() == '.') {
                consume();
                if (eof() || !isDigit(peek())) error("Invalid number (frac part)");
                while (!eof() && isDigit(peek())) consume();
            }
            if (!eof() && (peek() == 'e' || peek() == 'E')) {
                consume();
                if (!eof() && (peek() == '+' || peek() == '-')) consume();
                if (eof() || !isDigit(peek())) error("Invalid number (exp part)");
                while (!eof() && isDigit(peek())) consume();
            }
            return s.substring(start, i);
        }

        private boolean eof() {
            return i >= s.length();
        }

        private char peek() {
            return s.charAt(i);
        }

        private char peekNext() {
            return (i + 1 < s.length()) ? s.charAt(i + 1) : '\0';
        }

        private char consume() {
            char c = s.charAt(i++);
            col++;
            return c;
        }

        private static boolean isDigit(char c) {
            return c >= '0' && c <= '9';
        }

        private void error(String msg) {
            throw new IllegalStateException(msg + " at line " + line + ", col " + col);
        }
    }

    static final class Parser {
        private final Lexer lexer;

        Parser(Lexer l) {
            this.lexer = l;
        }

        JsonValue parse() {
            JsonValue v = parseValue();
            if (lexer.current() != Token.EOF) error("Trailing characters after top-level value");
            return v;
        }

        private void expect(Token t) {
            if (lexer.current() != t) error("Expected " + t + " but found " + lexer.current());
            lexer.advance();
        }

        private boolean accept(Token t) {
            if (lexer.current() == t) {
                lexer.advance();
                return true;
            }
            return false;
        }

        private JsonValue parseValue() {
            return switch (lexer.current()) {
                case LBRACE -> parseObject();
                case LBRACKET -> parseArray();
                case STRING -> {
                    String s = lexer.string();
                    lexer.advance();
                    yield new JsonString(s);
                }
                case NUMBER -> {
                    String n = lexer.number();
                    lexer.advance();
                    yield new JsonNumber(parseNumber(n));
                }
                case TRUE -> {
                    lexer.advance();
                    yield new JsonBoolean(true);
                }
                case FALSE -> {
                    lexer.advance();
                    yield new JsonBoolean(false);
                }
                case NULL -> {
                    lexer.advance();
                    yield new JsonNull();
                }
                case RBRACE, RBRACKET, COMMA, COLON -> {
                    error("Unexpected token: " + lexer.current());
                    yield null;
                }
                case EOF -> {
                    error("Unexpected end of input while expecting a value");
                    yield null;
                }
            };
        }

        private JsonObject parseObject() {
            expect(Token.LBRACE);
            Map<String, JsonValue> m = new LinkedHashMap<>();
            if (accept(Token.RBRACE)) return new JsonObject(m);
            while (true) {
                if (lexer.current() != Token.STRING) error("Expected string as object key");
                String key = lexer.string();
                lexer.advance();
                expect(Token.COLON);
                m.put(key, parseValue());
                if (accept(Token.COMMA)) continue;
                else if (accept(Token.RBRACE)) break;
                else error("Expected ',' or '}' in object");
            }
            return new JsonObject(m);
        }

        private JsonArray parseArray() {
            expect(Token.LBRACKET);
            List<JsonValue> list = new ArrayList<>();
            if (accept(Token.RBRACKET)) return new JsonArray(list);
            while (true) {
                list.add(parseValue());
                if (accept(Token.COMMA)) continue;
                else if (accept(Token.RBRACKET)) break;
                else error("Expected ',' or ']' in array");
            }
            return new JsonArray(list);
        }

        private void error(String msg) {
            throw new IllegalStateException(
                    msg + " at line " + lexer.line() + ", col " + lexer.col() + " (token " + lexer.current() + ")");
        }

        private static Number parseNumber(String s) {
            BigDecimal b = new BigDecimal(s), n = b.stripTrailingZeros();
            if (n.scale() <= 0) {
                try {
                    return n.intValueExact();
                } catch (ArithmeticException e1) {
                    try {
                        return n.longValueExact();
                    } catch (ArithmeticException e2) {
                        try {
                            return n.toBigIntegerExact();
                        } catch (ArithmeticException e3) {
                            return b;
                        }
                    }
                }
            } else {
                double d = b.doubleValue();
                return Double.isFinite(d) && b.compareTo(BigDecimal.valueOf(d)) == 0 ? d : b;
            }
        }
    }

    // ============================================================
    // JsonWriter (correct escaping, faster stringify)
    // ============================================================

    static final class JsonWriter {
        private final StringBuilder out;

        JsonWriter(StringBuilder sb) {
            this.out = sb;
        }

        void writeAny(Object o) {
            if (o == null) {
                out.append("null");
                return;
            }
            if (o instanceof JsonValue jv) {
                writeJsonValue(jv);
                return;
            }
            if (o instanceof CharSequence s) {
                writeString(s.toString());
                return;
            }
            if (o instanceof Character c) {
                writeString(String.valueOf(c));
                return;
            }
            if (o instanceof Boolean b) {
                out.append(b ? "true" : "false");
                return;
            }
            if (o instanceof Number n) {
                writeNumber(n);
                return;
            }
            if (o.getClass().isArray()) {
                writeArray(o);
                return;
            }
            if (o instanceof Iterable<?> it) {
                writeIterable(it);
                return;
            }
            if (o instanceof Map<?, ?> m) {
                writeMap(m);
                return;
            }
            if (o instanceof Enum<?> e) {
                writeString(e.name());
                return;
            }

            // EXT: temporal types -> string
            if (o instanceof Date d) {
                writeString(d.toInstant().toString());
                return;
            }
            if (o instanceof Instant i) {
                writeString(i.toString());
                return;
            }
            if (o instanceof LocalDate ld) {
                writeString(ld.toString());
                return;
            }
            if (o instanceof LocalTime lt) {
                writeString(lt.toString());
                return;
            }
            if (o instanceof LocalDateTime ldt) {
                writeString(ldt.toString());
                return;
            }
            if (o instanceof ZonedDateTime zdt) {
                writeString(zdt.toString());
                return;
            }
            if (o instanceof OffsetDateTime odt) {
                writeString(odt.toString());
                return;
            }
            if (o instanceof Duration du) {
                writeString(du.toString());
                return;
            }

            // record / bean
            if (o instanceof Record) {
                writeRecord(o);
                return;
            }
            writeBean(o);
        }

        private void writeJsonValue(JsonValue v) {
            if (v instanceof JsonNull) {
                out.append("null");
                return;
            }
            if (v instanceof JsonBoolean b) {
                out.append(b.value() ? "true" : "false");
                return;
            }
            if (v instanceof JsonNumber n) {
                out.append(n.value().toString());
                return;
            }
            if (v instanceof JsonString s) {
                writeString(s.value());
                return;
            }
            if (v instanceof JsonArray a) {
                out.append('[');
                List<JsonValue> vs = a.value();
                for (int i = 0; i < vs.size(); i++) {
                    if (i > 0) out.append(',');
                    writeJsonValue(vs.get(i));
                }
                out.append(']');
                return;
            }
            if (v instanceof JsonObject o) {
                out.append('{');
                boolean first = true;
                for (var en : o.value().entrySet()) {
                    if (!first) out.append(',');
                    first = false;
                    writeString(en.getKey());
                    out.append(':');
                    writeJsonValue(en.getValue());
                }
                out.append('}');
                return;
            }
            throw new IllegalStateException("Unknown JsonValue: " + v.getClass());
        }

        private void writeString(String s) {
            out.append('"');
            escapeTo(out, s);
            out.append('"');
        }

        private void writeNumber(Number n) {
            if (n instanceof BigDecimal || n instanceof BigInteger) {
                out.append(n.toString());
                return;
            }
            // Avoid NaN/Infinity (not valid in JSON)
            double d = n.doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d))
                throw new IllegalArgumentException("Invalid number value: " + n);
            out.append(n.toString());
        }

        private void writeArray(Object arr) {
            out.append('[');
            int len = Array.getLength(arr);
            for (int i = 0; i < len; i++) {
                if (i > 0) out.append(',');
                writeAny(Array.get(arr, i));
            }
            out.append(']');
        }

        private void writeIterable(Iterable<?> it) {
            out.append('[');
            boolean first = true;
            for (Object e : it) {
                if (!first) out.append(',');
                first = false;
                writeAny(e);
            }
            out.append(']');
        }

        private void writeMap(Map<?, ?> map) {
            out.append('{');
            boolean first = true;
            for (var en : map.entrySet()) {
                if (!first) out.append(',');
                first = false;
                writeString(String.valueOf(en.getKey())); // JSON keys must be strings
                out.append(':');
                writeAny(en.getValue());
            }
            out.append('}');
        }

        private void writeRecord(Object r) {
            out.append('{');
            boolean first = true;
            for (var c : r.getClass().getRecordComponents()) {
                if (!first) out.append(',');
                first = false;
                writeString(c.getName());
                out.append(':');
                try {
                    Object v = c.getAccessor().invoke(r);
                    writeAny(v);
                } catch (Exception e) {
                    throw new IllegalStateException("Record accessor failed: " + c.getName(), e);
                }
            }
            out.append('}');
        }

        private void writeBean(Object bean) {
            out.append('{');
            boolean first = true;
            try {
                var bi = Introspector.getBeanInfo(bean.getClass());
                for (var pd : bi.getPropertyDescriptors()) {
                    if ("class".equals(pd.getName())) continue;
                    var read = pd.getReadMethod();
                    if (read == null) continue;
                    Object v = read.invoke(bean);
                    if (!first) out.append(',');
                    first = false;
                    writeString(pd.getName());
                    out.append(':');
                    writeAny(v);
                }
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Bean serialization failed: " + bean.getClass().getName(), e);
            }
            out.append('}');
        }

        static void escapeTo(StringBuilder out, String s) {
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                switch (c) {
                    case '"' -> out.append("\\\"");
                    case '\\' -> out.append("\\\\");
                    case '\b' -> out.append("\\b");
                    case '\f' -> out.append("\\f");
                    case '\n' -> out.append("\\n");
                    case '\r' -> out.append("\\r");
                    case '\t' -> out.append("\\t");
                    default -> {
                        if (c < 0x20) {
                            out.append("\\u");
                            String hex = Integer.toHexString(c);
                            for (int k = hex.length(); k < 4; k++) out.append('0');
                            out.append(hex);
                        } else {
                            out.append(c);
                        }
                    }
                }
            }
        }
    }

    // ============================================================
    // JsonValue -> Object
    // ============================================================

    @SuppressWarnings("unchecked")
    private static <T> T fromJsonValue(JsonValue jv, java.lang.reflect.Type targetType) {
        // Normalize reflective type, obtain raw class
        targetType = canonicalize(targetType);
        Class<?> raw = raw(targetType);

        // 0) Trivial/dynamic targets
        if (raw == Object.class) return (T) fromUntyped(jv); // best-effort untyped
        if (JsonValue.class.isAssignableFrom(raw)) return (T) asJsonValue(jv, raw); // return AST as-is

        // 1) Null handling
        if (jv instanceof JsonNull) return (T) nullValueFor(raw);

        // 2) Scalar targets (LOOSE)

        // 2.1 boolean: accept JsonBoolean, "true"/"false", "1"/"0", numeric 1/0
        if (raw == boolean.class || raw == Boolean.class) {
            return (T) coerceBoolean(jv, raw);
        }

        // 2.2 number: accept JsonNumber, numeric strings, and booleans (true->1, false->0)
        if (Number.class.isAssignableFrom(raw) || (raw.isPrimitive() && raw != boolean.class && raw != char.class)) {
            return (T) coerceNumber(jv, raw);
        }

        // 2.3 String/CharSequence: stringify any JsonValue (numbers/booleans become their textual form)
        if (raw == String.class || raw == CharSequence.class) {
            return (T) coerceString(jv); // json string should have quote, Java String should not have quote
        }

        // 2.4 char/Character: accept 1-char string (or stringify first)
        if (raw == char.class || raw == Character.class) {
            String s = coerceString(jv);
            if (s.length() != 1) throw new IllegalStateException("Cannot coerce to char: " + s);
            return (T) Character.valueOf(s.charAt(0));
        }

        // 2.5 enum: accept name (case-insensitive) or ordinal number (string/number)
        if (raw.isEnum()) {
            return (T) coerceEnum(jv, raw);
        }

        // 2.6 temporal: accept ISO-8601 string or epoch millis (number/string)
        if (isTemporal(raw)) {
            return (T) coerceTemporal(jv, raw);
        }

        // 3) Structured targets (LOOSE)

        // 3.1 arrays: if non-array provided, wrap single element
        if (raw.isArray()) {
            JsonArray ja = (jv instanceof JsonArray) ? (JsonArray) jv : new JsonArray(List.of(jv));
            return (T) arrayFromJson(ja, raw.getComponentType());
        }

        // 3.2 collections: if non-array provided, wrap single element
        if (Iterable.class.isAssignableFrom(raw)) {
            JsonArray ja = (jv instanceof JsonArray) ? (JsonArray) jv : new JsonArray(List.of(jv));
            return (T) collectionFromJson(ja, targetType, raw);
        }

        // 3.3 maps: expect object; (keys are coerced loosely inside mapFromJson via coerceMapKey)
        if (Map.class.isAssignableFrom(raw)) {
            return (T) mapFromJson(expectObject(jv, targetType), targetType, raw);
        }

        // 4) POJOs: record or bean; require object
        JsonObject jo = expectObject(jv, targetType);
        if (raw.isRecord()) return (T) recordFromJson(jo, raw);
        return (T) beanFromJson(jo, raw);
    }

    private static Object mapFromJson(JsonObject jo, java.lang.reflect.Type type, Class<?> raw) {
        var keyType = mapKeyType(type);
        var valueType = mapValueType(type);
        Map<Object, Object> map = createMap(raw);
        for (var en : jo.value().entrySet()) {
            Object key = fromJsonValue(new JsonString(en.getKey()), keyType);
            Object val = fromJsonValue(en.getValue(), valueType);
            map.put(key, val);
        }
        return map;
    }

    private static Map<Object, Object> createMap(Class<?> raw) {
        if (raw == Map.class || raw == LinkedHashMap.class) return new LinkedHashMap<>();
        if (raw == HashMap.class) return new HashMap<>();
        if (raw == ConcurrentMap.class || raw == ConcurrentHashMap.class) return new ConcurrentHashMap<>();
        try {
            @SuppressWarnings("unchecked")
            Map<Object, Object> m =
                    (Map<Object, Object>) raw.getDeclaredConstructor().newInstance();
            return m;
        } catch (Exception e) {
            throw new IllegalStateException("Unsupported Map type: " + raw.getName(), e);
        }
    }

    private static Object recordFromJson(JsonObject jo, Class<?> raw) {
        try {
            var components = raw.getRecordComponents();
            Class<?>[] ctorTypes =
                    Arrays.stream(components).map(RecordComponent::getType).toArray(Class[]::new);
            Object[] args = new Object[components.length];
            for (int i = 0; i < components.length; i++) {
                var c = components[i];
                JsonValue v = findPropertyValue(jo, c.getName());
                args[i] = (v == null) ? defaultValue(c.getType()) : fromJsonValue(v, c.getGenericType());
            }
            var ctor = raw.getDeclaredConstructor(ctorTypes);
            makeAccessible(ctor, raw);
            return ctor.newInstance(args);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Record construct failed: " + raw.getName(), e);
        }
    }

    private static Object beanFromJson(JsonObject jo, Class<?> raw) {
        Object bean = constructBean(raw);
        try {
            var bi = Introspector.getBeanInfo(raw);
            for (var pd : bi.getPropertyDescriptors()) {
                if ("class".equals(pd.getName())) continue;
                var write = pd.getWriteMethod();
                if (write == null) continue;
                JsonValue v = findPropertyValue(jo, pd.getName());
                if (v == null) continue;
                Object tv = fromJsonValue(v, write.getGenericParameterTypes()[0]);
                write.invoke(bean, tv);
            }
            return bean;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Bean populate failed: " + raw.getName(), e);
        }
    }

    private static Object constructBean(Class<?> raw) {
        try {
            Constructor<?> noArg = null, unique = null;
            for (var c : raw.getDeclaredConstructors()) {
                if (c.getParameterCount() == 0) {
                    noArg = c;
                    break;
                } else if (unique == null) unique = c;
                else unique = null; // more than one non-noarg ctor
            }
            if (noArg != null) {
                makeAccessible(noArg, raw);
                return noArg.newInstance();
            }
            if (unique == null) throw new IllegalStateException("No suitable constructor for " + raw.getName());
            makeAccessible(unique, raw);
            var params = unique.getParameters();
            Object[] args = new Object[params.length];
            // fill defaults; will be overwritten via setters if present
            for (int i = 0; i < params.length; i++) args[i] = defaultValue(params[i].getType());
            return unique.newInstance(args);
        } catch (Exception e) {
            throw new IllegalStateException("Bean construct failed: " + raw.getName(), e);
        }
    }

    private static void makeAccessible(Constructor<?> c, Class<?> raw) {
        if (!Modifier.isPublic(c.getModifiers()) || !Modifier.isPublic(raw.getModifiers())) c.setAccessible(true);
    }

    private static JsonValue findPropertyValue(JsonObject jo, String propertyName) {
        var map = jo.value();
        JsonValue direct = map.get(propertyName);
        if (direct != null || map.containsKey(propertyName)) return direct;

        String snake = toSnakeCase(propertyName);
        if (!snake.equals(propertyName)) {
            JsonValue alt = map.get(snake);
            if (alt != null || map.containsKey(snake)) return alt;
        }

        String camel = toCamelCase(propertyName);
        if (!camel.equals(propertyName)) {
            JsonValue alt = map.get(camel);
            if (alt != null || map.containsKey(camel)) return alt;
        }

        return null;
    }

    private static String toSnakeCase(String name) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0) {
                    char prev = name.charAt(i - 1);
                    if (Character.isLowerCase(prev) || Character.isDigit(prev)) {
                        sb.append('_');
                    } else if (Character.isUpperCase(prev)
                            && i + 1 < name.length()
                            && Character.isLowerCase(name.charAt(i + 1))) {
                        sb.append('_');
                    }
                }
                sb.append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String toCamelCase(String name) {
        StringBuilder sb = new StringBuilder();
        boolean upperNext = false;
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c == '_') {
                upperNext = true;
            } else if (upperNext) {
                sb.append(Character.toUpperCase(c));
                upperNext = false;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static Object arrayFromJson(JsonArray ja, Class<?> component) {
        int len = ja.value().size();
        Object arr = Array.newInstance(component, len);
        for (int i = 0; i < len; i++) {
            Array.set(arr, i, fromJsonValue(ja.value().get(i), component));
        }
        return arr;
    }

    private static Object collectionFromJson(JsonArray ja, java.lang.reflect.Type type, Class<?> raw) {
        Collection<Object> coll = createCollection(raw);
        java.lang.reflect.Type elemType = collectionElementType(type);
        for (var v : ja.value()) coll.add(fromJsonValue(v, elemType));
        return coll;
    }

    @SuppressWarnings("unchecked")
    private static Collection<Object> createCollection(Class<?> raw) {
        if (raw == List.class || raw == ArrayList.class || raw == Collection.class || raw == Iterable.class)
            return new ArrayList<>();
        if (raw == LinkedList.class) return new LinkedList<>();
        if (raw == Set.class || raw == LinkedHashSet.class) return new LinkedHashSet<>();
        if (raw == HashSet.class) return new HashSet<>();
        if (raw == Queue.class || raw == Deque.class || raw == ArrayDeque.class) return new ArrayDeque<>();
        try {
            return (Collection<Object>) raw.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("Unsupported Collection type: " + raw.getName(), e);
        }
    }

    private static JsonObject expectObject(JsonValue jv, java.lang.reflect.Type t) {
        if (jv instanceof JsonObject o) return o;
        throw new IllegalStateException("Expected JSON object for type " + t + ", but got "
                + jv.getClass().getSimpleName());
    }

    private static Object asJsonValue(JsonValue jv, Class<?> raw) {
        if (raw.isInstance(jv)) return jv;
        if (raw == JsonValue.class) return jv;
        throw new IllegalStateException("Cannot convert " + jv.getClass() + " to " + raw);
    }

    private static Object fromUntyped(JsonValue jv) {
        if (jv instanceof JsonNull) return null;
        if (jv instanceof JsonBoolean b) return b.value();
        if (jv instanceof JsonNumber n) return n.value();
        if (jv instanceof JsonString s) return s.value();
        if (jv instanceof JsonArray a) {
            List<Object> list = new ArrayList<>(a.value().size());
            for (var e : a.value()) list.add(fromJsonValue(e, Object.class));
            return list;
        }
        if (jv instanceof JsonObject o) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (var en : o.value().entrySet()) map.put(en.getKey(), fromJsonValue(en.getValue(), Object.class));
            return map;
        }
        throw new IllegalStateException("Unknown JsonValue: " + jv.getClass());
    }

    private static Object nullValueFor(Class<?> raw) {
        if (raw.isPrimitive()) throw new IllegalStateException("Cannot assign null to primitive type " + raw.getName());
        return null;
    }

    private static Object defaultValue(Class<?> t) {
        if (t == boolean.class) return false;
        if (t == byte.class) return (byte) 0;
        if (t == short.class) return (short) 0;
        if (t == int.class) return 0;
        if (t == long.class) return 0L;
        if (t == float.class) return 0f;
        if (t == double.class) return 0d;
        if (t == char.class) return '\0';
        return null;
    }

    private static Object coerceBoolean(JsonValue jv, Class<?> raw) {
        if (!(jv instanceof JsonBoolean) && !(jv instanceof JsonString) && !(jv instanceof JsonNumber)) {
            jv = new JsonString(coerceString(jv));
        }
        boolean bool;
        if (jv instanceof JsonBoolean b) bool = b.value();
        else if (jv instanceof JsonString s) {
            String v = s.value();
            if (v.equalsIgnoreCase("true")) bool = true;
            else if (v.equalsIgnoreCase("false")) bool = false;
            else throw new IllegalStateException("Cannot coerce string to boolean: " + v);
        } else if (jv instanceof JsonNumber n) {
            int v = n.value().intValue();
            if (v == 0) bool = false;
            else if (v == 1) bool = true;
            else throw new IllegalStateException("Cannot coerce number to boolean: " + v);
        } else throw new IllegalStateException("Cannot coerce " + jv.getClass().getSimpleName() + " to boolean");

        if (raw == boolean.class || raw == Boolean.class) return bool;
        throw new IllegalStateException("Unsupported boolean target: " + raw.getName());
    }

    private static Object coerceNumber(JsonValue jv, Class<?> raw) {
        if (jv instanceof JsonBoolean b) {
            jv = new JsonNumber(b.value() ? BigDecimal.ONE : BigDecimal.ZERO);
        } else if (!(jv instanceof JsonNumber) && !(jv instanceof JsonString)) {
            // last resort: stringify non-number into string, then parse
            jv = new JsonString(coerceString(jv));
        }
        BigDecimal bd;
        if (jv instanceof JsonNumber n) {
            Number number = n.value();
            if (number instanceof BigDecimal bdNumber) bd = bdNumber;
            else if (number instanceof BigInteger bi) bd = new BigDecimal(bi);
            else bd = new BigDecimal(number.toString());
        } else if (jv instanceof JsonString s) {
            try {
                bd = new BigDecimal(s.value());
            } catch (NumberFormatException e) {
                throw new IllegalStateException("Cannot parse number: " + s.value(), e);
            }
        } else throw new IllegalStateException("Cannot coerce " + jv.getClass().getSimpleName() + " to number");

        if (raw == BigDecimal.class) return bd;
        if (raw == BigInteger.class) return new BigInteger(bd.toPlainString());
        if (raw == Number.class) return bd;
        if (raw == double.class || raw == Double.class) return bd.doubleValue();
        if (raw == float.class || raw == Float.class) return bd.floatValue();
        if (raw == long.class || raw == Long.class) return bd.longValue();
        if (raw == int.class || raw == Integer.class) return bd.intValue();
        if (raw == short.class || raw == Short.class) return bd.shortValue();
        if (raw == byte.class || raw == Byte.class) return bd.byteValue();
        throw new IllegalStateException("Unsupported numeric target: " + raw.getName());
    }

    private static String coerceString(JsonValue jv) {
        if (jv instanceof JsonString s) return s.value();
        return stringify(jv);
    }

    private static Object coerceEnum(JsonValue jv, Class<?> raw) {
        if (!(jv instanceof JsonString) && !(jv instanceof JsonNumber)) {
            jv = new JsonString(coerceString(jv)); // fallback to textual form
        }
        if (jv instanceof JsonString s) {
            for (Object ec : raw.getEnumConstants()) if (((Enum<?>) ec).name().equalsIgnoreCase(s.value())) return ec;
            throw new IllegalStateException("No enum constant " + raw.getName() + "." + s.value());
        }
        if (jv instanceof JsonNumber n) {
            int ord = n.value().intValue();
            Object[] cs = raw.getEnumConstants();
            if (ord < 0 || ord >= cs.length) throw new IllegalStateException("Enum ordinal out of range: " + ord);
            return cs[ord];
        }
        throw new IllegalStateException("Cannot coerce " + jv.getClass().getSimpleName() + " to enum " + raw.getName());
    }

    // -------- temporals
    private static boolean isTemporal(Class<?> raw) {
        return raw == Instant.class
                || raw == Date.class
                || raw == Timestamp.class
                || raw == LocalDate.class
                || raw == LocalTime.class
                || raw == LocalDateTime.class
                || raw == ZonedDateTime.class
                || raw == OffsetDateTime.class
                || raw == Duration.class;
    }

    private static Object coerceTemporal(JsonValue jv, Class<?> raw) {
        if (!(jv instanceof JsonString) && !(jv instanceof JsonNumber)) {
            jv = new JsonString(coerceString(jv));
        }
        if (raw == Instant.class) {
            if (jv instanceof JsonString s) return Instant.parse(s.value());
            if (jv instanceof JsonNumber n)
                return Instant.ofEpochMilli(n.value().longValue());
        } else if (raw == Date.class) {
            Instant i = (Instant) coerceTemporal(jv, Instant.class);
            return Date.from(i);
        } else if (raw == Timestamp.class) {
            Instant i = (Instant) coerceTemporal(jv, Instant.class);
            return Timestamp.from(i);
        } else if (raw == Duration.class) {
            if (jv instanceof JsonString s) return Duration.parse(s.value());
            if (jv instanceof JsonNumber n) return Duration.ofMillis(n.value().longValue());
        } else if (raw == LocalDate.class) {
            if (jv instanceof JsonString s) return LocalDate.parse(s.value());
        } else if (raw == LocalTime.class) {
            if (jv instanceof JsonString s) return LocalTime.parse(s.value());
        } else if (raw == LocalDateTime.class) {
            if (jv instanceof JsonString s) return LocalDateTime.parse(s.value());
            if (jv instanceof JsonNumber n)
                return Instant.ofEpochMilli(n.value().longValue())
                        .atZone(ZoneId.systemDefault())
                        .toLocalDateTime();
        } else if (raw == ZonedDateTime.class) {
            if (jv instanceof JsonString s) return ZonedDateTime.parse(s.value());
            if (jv instanceof JsonNumber n)
                return Instant.ofEpochMilli(n.value().longValue()).atZone(ZoneId.systemDefault());
        } else if (raw == OffsetDateTime.class) {
            if (jv instanceof JsonString s) return OffsetDateTime.parse(s.value());
            if (jv instanceof JsonNumber n)
                return Instant.ofEpochMilli(n.value().longValue())
                        .atZone(ZoneId.systemDefault())
                        .toOffsetDateTime();
        }
        throw new IllegalStateException("Cannot coerce " + jv.getClass().getSimpleName() + " to " + raw.getName());
    }

    // ============================================================
    // Type utils
    // ============================================================

    private static Class<?> raw(java.lang.reflect.Type t) {
        if (t instanceof Class<?> c) return c;
        if (t instanceof ParameterizedType p) return (Class<?>) p.getRawType();
        if (t instanceof GenericArrayType ga) {
            var comp = raw(ga.getGenericComponentType());
            return Array.newInstance(comp, 0).getClass();
        }
        if (t instanceof TypeVariable<?> tv) return raw(erasureOf(tv));
        if (t instanceof WildcardType w) return raw(erasureOf(w));
        throw new IllegalStateException("Unsupported type: " + t);
    }

    private static java.lang.reflect.Type collectionElementType(java.lang.reflect.Type t) {
        if (t instanceof ParameterizedType p) return canonicalize(p.getActualTypeArguments()[0]);
        if (t instanceof GenericArrayType ga) return canonicalize(ga.getGenericComponentType());
        if (t instanceof Class<?> c && c.isArray()) return c.getComponentType();
        return Object.class;
    }

    private static java.lang.reflect.Type mapKeyType(java.lang.reflect.Type t) {
        if (t instanceof ParameterizedType p) return canonicalize(p.getActualTypeArguments()[0]);
        return String.class; // JSON keys are strings
    }

    private static java.lang.reflect.Type mapValueType(java.lang.reflect.Type t) {
        if (t instanceof ParameterizedType p) return canonicalize(p.getActualTypeArguments()[1]);
        return Object.class;
    }

    private static java.lang.reflect.Type canonicalize(java.lang.reflect.Type t) {
        if (t instanceof WildcardType w) return erasureOf(w);
        if (t instanceof TypeVariable<?> tv) return erasureOf(tv);
        return t;
    }

    private static java.lang.reflect.Type erasureOf(WildcardType w) {
        var uppers = w.getUpperBounds();
        return uppers.length == 0 ? Object.class : uppers[0];
    }

    private static java.lang.reflect.Type erasureOf(TypeVariable<?> tv) {
        var uppers = tv.getBounds();
        return uppers.length == 0 ? Object.class : uppers[0];
    }
}
