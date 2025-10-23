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
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Currency;
import java.util.Date;
import java.util.EnumMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.ServiceLoader;
import java.util.SimpleTimeZone;
import java.util.Spliterators;
import java.util.Stack;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.BaseStream;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Singular;

/**
 * Minimal, standard-first JSON writer and parser.
 *
 * @author <a href="mailto:llw599502537@gmail.com">Freeman</a>
 */
public final class Json {

    private static final List<Codec> codecs = loadCodecs();

    private static final Writer defaultWriter = Writer.builder().build();
    private static final Parser defaultParser = Parser.builder().build();

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
        return defaultWriter.write(o);
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
        return defaultParser.parse(json, type);
    }

    /**
     * Parse JSON text into a target type.
     *
     * <p> This is a convenience overload of {@link #parse(String, Type)}
     *
     * @param json  JSON text, not {@code null}
     * @param clazz target class, not {@code null}
     * @param <T>   result type
     * @return parsed instance (maybe {@code null} if json is "null")
     */
    public static <T> T parse(String json, Class<T> clazz) {
        Objects.requireNonNull(clazz, "clazz");
        return parse(json, Type.of(clazz));
    }

    // ============================================================
    // AST
    // ============================================================

    public sealed interface JsonValue permits JsonArray, JsonBoolean, JsonNull, JsonNumber, JsonObject, JsonString {}

    public record JsonArray(List<JsonValue> value) implements JsonValue {}

    public record JsonBoolean(boolean value) implements JsonValue {}

    public record JsonNull() implements JsonValue {}

    public record JsonNumber(Number value) implements JsonValue {}

    public record JsonObject(Map<String, JsonValue> value) implements JsonValue {}

    public record JsonString(String value) implements JsonValue {}

    // ============================================================
    // Extension point
    // ============================================================

    public interface Codec {
        boolean canSerialize(Object o);

        String serialize(Json.Writer writer, Object o);

        boolean canDeserialize(JsonValue jsonValue, java.lang.reflect.Type targetType);

        Object deserialize(Json.Parser parser, JsonValue jsonValue, java.lang.reflect.Type targetType);
    }

    public interface Serializer {
        boolean canSerialize(Object o);

        String serialize(Json.Writer writer, Object o);
    }

    public interface Deserializer {
        boolean canDeserialize(JsonValue jsonValue, java.lang.reflect.Type targetType);

        Object deserialize(Json.Parser parser, JsonValue jsonValue, java.lang.reflect.Type targetType);
    }

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
                    if (eof()) error("Unterminated escape sequence");
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
                                    if (!Character.isLowSurrogate((char) low))
                                        error("Invalid low surrogate in unicode escape");
                                    sb.appendCodePoint(Character.toCodePoint((char) cp, (char) low));
                                } else error("High surrogate not followed by low surrogate in unicode escape");
                            } else if (Character.isLowSurrogate((char) cp)) {
                                error("Unexpected low surrogate in unicode escape");
                            } else sb.append((char) cp);
                        }
                        default -> error("Invalid escape sequence: \\" + e);
                    }
                } else {
                    if (c < 0x20) error("Unescaped control character in string (ASCII " + (int) c + ")");
                    sb.append(c);
                }
            }
            error("Unterminated string literal");
            return null;
        }

        private int readHex4() {
            int cp = 0;
            for (int k = 0; k < 4; k++) {
                if (eof()) error("Unexpected end of input in \\u escape sequence");
                int v = hexVal(consume());
                if (v < 0) error("Invalid hexadecimal digit in \\u escape sequence");
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
            if (eof()) error("Unexpected end of input while parsing number");
            if (peek() == '0') consume();
            else if (isDigit(peek())) while (!eof() && isDigit(peek())) consume();
            else error("Invalid number format (integer part)");
            if (!eof() && peek() == '.') {
                consume();
                if (eof() || !isDigit(peek())) error("Invalid number format (fractional part)");
                while (!eof() && isDigit(peek())) consume();
            }
            if (!eof() && (peek() == 'e' || peek() == 'E')) {
                consume();
                if (!eof() && (peek() == '+' || peek() == '-')) consume();
                if (eof() || !isDigit(peek())) error("Invalid number format (exponent part)");
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
            throw new SyntaxException(msg + " at line " + line + ", column " + col);
        }
    }

    // ============================================================
    // Writer (correct escaping, faster stringify)
    // ============================================================

    @Builder(toBuilder = true)
    public static final class Writer {

        @Singular("serializer")
        private final List<Serializer> serializers;

        public static void main(String[] args) {
            var build = Writer.builder().build();
            System.out.println(build);
        }

        public String write(Object o) {
            var sb = new StringBuilder();
            write(sb, o);
            return sb.toString();
        }

        void write(StringBuilder out, Object o) {
            if (o == null) {
                out.append("null");
                return;
            }
            if (o instanceof JsonValue jv) {
                writeJsonValue(out, jv);
                return;
            }
            for (var codec : codecs) {
                if (codec.canSerialize(o)) {
                    out.append(codec.serialize(this, o));
                    return;
                }
            }
            if (o instanceof CharSequence s) {
                writeString(out, s.toString());
                return;
            }
            if (o instanceof Character c) {
                writeString(out, String.valueOf(c));
                return;
            }
            if (o instanceof Boolean b) {
                out.append(b ? "true" : "false");
                return;
            }
            if (o instanceof Number n) {
                writeNumber(out, n);
                return;
            }
            if (o instanceof Optional<?> optional) {
                if (optional.isEmpty()) out.append("null");
                else write(out, optional.get());
                return;
            }
            // AtomicBoolean: extract the value and serialize
            if (o instanceof AtomicBoolean ab) {
                out.append(ab.get() ? "true" : "false");
                return;
            }
            // AtomicReference: extract the value and serialize
            if (o instanceof AtomicReference<?> ar) {
                write(out, ar.get());
                return;
            }
            if (o.getClass().isArray()) {
                writeArray(out, o);
                return;
            }
            if (o instanceof Collection<?> coll) {
                writeCollection(out, coll);
                return;
            }
            if (o instanceof BaseStream<?, ?> stream) {
                writeStream(out, stream);
                return;
            }
            if (o instanceof Map<?, ?> m) {
                writeMap(out, m);
                return;
            }
            if (o instanceof Enum<?> e) {
                writeString(out, e.name());
                return;
            }

            // temporal types -> string
            if (writeTemporal(out, o)) return;

            // string-based types -> string
            if (writeStringBasedType(out, o)) return;

            // record / bean
            if (o instanceof Record) {
                writeRecord(out, o);
                return;
            }
            writeBean(out, o);
        }

        boolean writeTemporal(StringBuilder out, Object o) {
            if (o instanceof Date d) {
                writeString(out, d.toInstant().toString());
                return true;
            }
            if (o instanceof Instant i) {
                writeString(out, i.toString());
                return true;
            }
            if (o instanceof LocalDate ld) {
                writeString(out, ld.toString());
                return true;
            }
            if (o instanceof LocalTime lt) {
                writeString(out, lt.toString());
                return true;
            }
            if (o instanceof LocalDateTime ldt) {
                writeString(out, ldt.toString());
                return true;
            }
            if (o instanceof ZonedDateTime zdt) {
                writeString(out, zdt.toString());
                return true;
            }
            if (o instanceof OffsetDateTime odt) {
                writeString(out, odt.toString());
                return true;
            }
            if (o instanceof Duration du) {
                writeString(out, du.toString());
                return true;
            }
            if (o instanceof Year y) {
                writeString(out, y.toString());
                return true;
            }
            if (o instanceof YearMonth ym) {
                writeString(out, ym.toString());
                return true;
            }
            if (o instanceof MonthDay md) {
                writeString(out, md.toString());
                return true;
            }
            if (o instanceof Period p) {
                writeString(out, p.toString());
                return true;
            }
            if (o instanceof ZoneOffset zo) {
                writeString(out, zo.toString());
                return true;
            }
            if (o instanceof ZoneId zi) {
                writeString(out, zi.toString());
                return true;
            }
            return false;
        }

        boolean writeStringBasedType(StringBuilder out, Object o) {
            // java.util types -> string
            if (o instanceof UUID uuid) {
                writeString(out, uuid.toString());
                return true;
            }
            if (o instanceof Locale locale) {
                writeString(out, locale.toLanguageTag());
                return true;
            }
            if (o instanceof Currency currency) {
                writeString(out, currency.getCurrencyCode());
                return true;
            }
            if (o instanceof TimeZone tz) {
                writeString(out, tz.getID());
                return true;
            }

            // java.net types -> string
            if (o instanceof URI uri) {
                writeString(out, uri.toString());
                return true;
            }
            if (o instanceof URL url) {
                writeString(out, url.toString());
                return true;
            }

            // java.nio.file.Path -> string
            if (o instanceof Path path) {
                writeString(out, path.toString());
                return true;
            }

            // java.util.regex.Pattern -> string
            if (o instanceof Pattern pattern) {
                writeString(out, pattern.pattern());
                return true;
            }
            return false;
        }

        void writeStream(StringBuilder out, BaseStream<?, ?> stream) {
            out.append('[');
            boolean first = true;
            try (var s = stream) {
                var it = Spliterators.iterator(s.spliterator());
                while (it.hasNext()) {
                    if (!first) out.append(',');
                    first = false;
                    write(out, it.next());
                }
            }
            out.append(']');
        }

        void writeJsonValue(StringBuilder out, JsonValue v) {
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
                writeString(out, s.value());
                return;
            }
            if (v instanceof JsonArray a) {
                out.append('[');
                List<JsonValue> vs = a.value();
                for (int i = 0; i < vs.size(); i++) {
                    if (i > 0) out.append(',');
                    writeJsonValue(out, vs.get(i));
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
                    writeString(out, en.getKey());
                    out.append(':');
                    writeJsonValue(out, en.getValue());
                }
                out.append('}');
                return;
            }
            throw new WriteException("Unknown JsonValue type: " + v.getClass());
        }

        void writeString(StringBuilder out, String s) {
            out.append('"');
            escapeTo(out, s);
            out.append('"');
        }

        void writeNumber(StringBuilder out, Number n) {
            if (n instanceof BigDecimal || n instanceof BigInteger) {
                out.append(n);
                return;
            }
            // Avoid NaN/Infinity (not valid in JSON)
            double d = n.doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d))
                throw new WriteException("Cannot serialize NaN or Infinity as JSON number: " + n);
            out.append(n);
        }

        private void writeArray(StringBuilder out, Object arr) {
            out.append('[');
            int len = Array.getLength(arr);
            for (int i = 0; i < len; i++) {
                if (i > 0) out.append(',');
                write(out, Array.get(arr, i));
            }
            out.append(']');
        }

        private void writeCollection(StringBuilder out, Collection<?> coll) {
            out.append('[');
            boolean first = true;
            for (Object e : coll) {
                if (!first) out.append(',');
                first = false;
                write(out, e);
            }
            out.append(']');
        }

        private void writeMap(StringBuilder out, Map<?, ?> map) {
            out.append('{');
            boolean first = true;
            for (var en : map.entrySet()) {
                if (!first) out.append(',');
                first = false;
                writeString(out, String.valueOf(en.getKey())); // JSON keys must be strings
                out.append(':');
                write(out, en.getValue());
            }
            out.append('}');
        }

        private void writeRecord(StringBuilder out, Object r) {
            out.append('{');
            boolean first = true;
            for (var c : r.getClass().getRecordComponents()) {
                try {
                    var readMethod = c.getAccessor();
                    Object v = readMethod.invoke(r);
                    if (readMethod.getReturnType() == Optional.class) {
                        if (v == null) continue; // treat null Optional as empty
                        var optional = (Optional<?>) v;
                        if (optional.isEmpty()) continue;
                        v = optional.get();
                    }
                    if (!first) out.append(',');
                    first = false;
                    writeString(out, c.getName());
                    out.append(':');
                    write(out, v);
                } catch (java.lang.Exception e) {
                    throw new WriteException(
                            "Failed to access record component '" + c.getName() + "' of type "
                                    + r.getClass().getName(),
                            e);
                }
            }
            out.append('}');
        }

        private void writeBean(StringBuilder out, Object bean) {
            out.append('{');
            boolean first = true;
            try {
                var bi = Introspector.getBeanInfo(bean.getClass());
                for (var pd : bi.getPropertyDescriptors()) {
                    if ("class".equals(pd.getName())) continue;
                    var read = pd.getReadMethod();
                    if (read == null) continue;
                    try {
                        Object v = read.invoke(bean);
                        if (read.getReturnType() == Optional.class) {
                            if (v == null) continue; // treat null Optional as empty
                            var optional = (Optional<?>) v;
                            if (optional.isEmpty()) continue;
                            v = optional.get();
                        }
                        if (!first) out.append(',');
                        first = false;
                        writeString(out, pd.getName());
                        out.append(':');
                        write(out, v);
                    } catch (java.lang.Exception e) {
                        throw new WriteException(
                                "Failed to read bean property '" + pd.getName() + "' of type "
                                        + bean.getClass().getName(),
                                e);
                    }
                }
            } catch (Exception e) {
                throw e;
            } catch (java.lang.Exception e) {
                throw new WriteException(
                        "Failed to introspect bean of type " + bean.getClass().getName(), e);
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
    // Parser
    // ============================================================

    @Builder(toBuilder = true)
    public static final class Parser {

        @Singular("deserializer")
        private final List<Deserializer> deserializers;

        public <T> T parse(String json, Class<T> clazz) {
            return parse(json, Type.of(clazz));
        }

        public <T> T parse(String json, Type<T> type) {
            var jv = parseJsonValue(new Lexer(json));
            return parseJsonValue(jv, canonicalize(type.getType()));
        }

        @SuppressWarnings("unchecked")
        public <T> T parseJsonValue(JsonValue jv, java.lang.reflect.Type targetType) {
            // Normalize reflective type, obtain raw class
            targetType = canonicalize(targetType);
            Class<?> raw = raw(targetType);

            // 0) Trivial/dynamic targets
            if (raw == Object.class) return (T) fromUntyped(jv); // best-effort untyped
            if (JsonValue.class.isAssignableFrom(raw)) return (T) asJsonValue(jv, raw); // return AST as-is

            // Custom codec
            for (var codec : codecs) {
                if (codec.canDeserialize(jv, targetType)) {
                    return (T) codec.deserialize(this, jv, targetType);
                }
            }

            // 1) Null handling
            if (jv instanceof JsonNull) return (T) toNull(raw);

            // 2) Scalar targets (LOOSE)

            // 2.1 boolean: accept JsonBoolean, "true"/"false", "1"/"0", numeric 1/0
            if (raw == boolean.class || raw == Boolean.class || raw == AtomicBoolean.class) {
                return (T) toBoolean(jv, raw);
            }

            // 2.2 number: accept JsonNumber, numeric strings, and booleans (true->1, false->0)
            // AtomicInteger and AtomicLong are Number subclasses, so they're handled here
            if (Number.class.isAssignableFrom(raw) || (raw.isPrimitive() && raw != char.class)) {
                return (T) toNumber(jv, raw);
            }

            // 2.3 String/CharSequence: stringify any JsonValue (numbers/booleans become their textual form)
            if (raw == String.class || raw == CharSequence.class) {
                return (T) Json.toString(jv); // json string should have quote, Java String should not have quote
            }

            // 2.4 char/Character: accept 1-char string (or stringify first)
            if (raw == char.class || raw == Character.class) {
                String s = Json.toString(jv);
                if (s.length() != 1)
                    throw new ConversionException(
                            "Cannot convert string to char: expected length 1, got " + s.length() + " (\"" + s + "\")");
                return (T) Character.valueOf(s.charAt(0));
            }

            // 2.5 enum: accept name (case-insensitive) or ordinal number (string/number)
            if (raw.isEnum()) {
                return (T) toEnum(jv, raw);
            }

            // 2.6 temporal: accept ISO-8601 string or epoch millis (number/string)
            if (isTemporal(raw)) {
                return (T) toTemporal(jv, raw);
            }

            // 2.7 string-based types, like UUID, URI...
            if (isStringBasedType(raw)) {
                return (T) toStringBasedType(jv, raw);
            }

            // 2.8 optional: wrap the inner type
            if (raw == Optional.class) {
                return (T) toOptional(jv, targetType);
            }

            // 2.9 AtomicReference: wrap the inner type
            if (raw == AtomicReference.class) {
                return (T) new AtomicReference<>(
                        parseJsonValue(jv, ((ParameterizedType) targetType).getActualTypeArguments()[0]));
            }

            // 3) Structured targets (LOOSE)

            // 3.1 arrays: if non-array provided, wrap single element
            if (raw.isArray()) {
                JsonArray ja = (jv instanceof JsonArray) ? (JsonArray) jv : new JsonArray(List.of(jv));
                return (T) toArray(ja, raw.getComponentType());
            }

            // 3.2 Collection: if non-array provided, wrap single element
            if (Collection.class.isAssignableFrom(raw)) {
                JsonArray ja = (jv instanceof JsonArray) ? (JsonArray) jv : new JsonArray(List.of(jv));
                return (T) toCollection(ja, targetType);
            }

            // 3.3 Stream
            if (BaseStream.class.isAssignableFrom(raw)) {
                JsonArray ja = (jv instanceof JsonArray) ? (JsonArray) jv : new JsonArray(List.of(jv));
                return (T) toStream(ja, targetType);
            }

            // Must JsonObject here!
            JsonObject jo = expectObject(jv);

            // 3.4 Map
            if (Map.class.isAssignableFrom(raw)) {
                return (T) toMap(jo, targetType);
            }

            // 3.5 Record
            if (raw.isRecord()) return (T) toRecord(jo, raw);

            // 3.6 Java bean
            return (T) toBean(jo, raw);
        }

        static JsonValue parseJsonValue(Lexer lexer) {
            JsonValue v = parseValue(lexer);
            if (lexer.current() != Token.EOF) error(lexer, "Trailing characters after top-level value");
            return v;
        }

        static void expect(Lexer lexer, Token t) {
            if (lexer.current() != t) error(lexer, "Expected " + t + " but found " + lexer.current());
            lexer.advance();
        }

        static boolean accept(Lexer lexer, Token t) {
            if (lexer.current() == t) {
                lexer.advance();
                return true;
            }
            return false;
        }

        static JsonValue parseValue(Lexer lexer) {
            return switch (lexer.current()) {
                case LBRACE -> parseObject(lexer);
                case LBRACKET -> parseArray(lexer);
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
                    error(lexer, "Unexpected token: " + lexer.current());
                    yield null;
                }
                case EOF -> {
                    error(lexer, "Unexpected end of input while expecting a value");
                    yield null;
                }
            };
        }

        static JsonObject parseObject(Lexer lexer) {
            expect(lexer, Token.LBRACE);
            Map<String, JsonValue> m = new LinkedHashMap<>();
            if (accept(lexer, Token.RBRACE)) return new JsonObject(m);
            while (true) {
                if (lexer.current() != Token.STRING) error(lexer, "Expected string key in object");
                String key = lexer.string();
                lexer.advance();
                expect(lexer, Token.COLON);
                m.put(key, parseValue(lexer));
                if (accept(lexer, Token.COMMA)) continue;
                else if (accept(lexer, Token.RBRACE)) break;
                else error(lexer, "Expected ',' or '}' in object");
            }
            return new JsonObject(m);
        }

        static JsonArray parseArray(Lexer lexer) {
            expect(lexer, Token.LBRACKET);
            List<JsonValue> list = new ArrayList<>();
            if (accept(lexer, Token.RBRACKET)) return new JsonArray(list);
            while (true) {
                list.add(parseValue(lexer));
                if (accept(lexer, Token.COMMA)) continue;
                else if (accept(lexer, Token.RBRACKET)) break;
                else error(lexer, "Expected ',' or ']' in array");
            }
            return new JsonArray(list);
        }

        static void error(Lexer lexer, String msg) {
            throw new SyntaxException(
                    msg + " (token: " + lexer.current() + ") at line " + lexer.line() + ", column " + lexer.col());
        }

        static Number parseNumber(String s) {
            BigDecimal b = new BigDecimal(s), n = b.stripTrailingZeros();
            if (n.scale() <= 0) {
                try {
                    long l = n.longValueExact();
                    if ((int) l == l) return (int) l; // Do NOT use Ternary Operator here!
                    return l;
                } catch (ArithmeticException e) {
                    return n.toBigIntegerExact();
                }
            } else {
                double d = b.doubleValue();
                return Double.isFinite(d) && b.compareTo(BigDecimal.valueOf(d)) == 0 ? d : b;
            }
        }

        static JsonObject expectObject(JsonValue jv) {
            if (jv instanceof JsonObject o) return o;
            throw new ConversionException(
                    "Expected JSON object, but got " + jv.getClass().getSimpleName());
        }

        private static Object toStringBasedType(JsonValue jv, Class<?> raw) {
            String value = Json.toString(jv);
            try {
                if (raw == UUID.class) return UUID.fromString(value);
                if (raw == Locale.class) return Locale.forLanguageTag(value);
                if (raw == Currency.class) return Currency.getInstance(value);
                if (raw == SimpleTimeZone.class) return SimpleTimeZone.getTimeZone(ZoneId.of(value));
                if (raw == TimeZone.class) return TimeZone.getTimeZone(value);
                if (raw == URI.class) return URI.create(value);
                if (raw == URL.class) return new URL(value);
                if (raw == Path.class) return Path.of(value);
                if (raw == Pattern.class) return Pattern.compile(value);
            } catch (java.lang.Exception e) {
                throw new ConversionException(
                        "Cannot parse " + raw.getSimpleName() + " from value: '" + value + "'", e);
            }
            throw new ConversionException("Cannot convert to " + raw.getSimpleName());
        }

        Object toMap(JsonObject jo, java.lang.reflect.Type type) {
            var keyType = mapKeyType(type);
            var valueType = mapValueType(type);
            var map = createMap(raw(type), jo.value().size());
            for (var en : jo.value().entrySet()) {
                Object key = parseJsonValue(new JsonString(en.getKey()), keyType);
                Object val = parseJsonValue(en.getValue(), valueType);
                map.put(key, val);
            }
            return map;
        }

        static java.lang.reflect.Type mapKeyType(java.lang.reflect.Type t) {
            if (t instanceof ParameterizedType p) return canonicalize(p.getActualTypeArguments()[0]);
            return String.class; // JSON keys are strings
        }

        static java.lang.reflect.Type mapValueType(java.lang.reflect.Type t) {
            if (t instanceof ParameterizedType p) return canonicalize(p.getActualTypeArguments()[1]);
            return Object.class;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        static Map<Object, Object> createMap(Class<?> raw, int size) {
            int cap = mapCap(size);
            if (typeBetween(raw, LinkedHashMap.class, Map.class)) return new LinkedHashMap<>(cap);
            if (typeBetween(raw, TreeMap.class, null)) return new TreeMap<>();
            if (typeBetween(raw, EnumMap.class, null)) {
                if (!typeBetween(raw, null, Enum.class))
                    throw new ConversionException("EnumMap requires Enum key type");
                return new EnumMap(raw.asSubclass(Enum.class));
            }
            if (typeBetween(raw, IdentityHashMap.class, null)) return new IdentityHashMap<>(cap);
            if (typeBetween(raw, ConcurrentHashMap.class, null)) return new ConcurrentHashMap<>(cap);
            if (typeBetween(raw, ConcurrentSkipListMap.class, null)) return new ConcurrentSkipListMap<>();
            try {
                return (Map<Object, Object>) raw.getDeclaredConstructor().newInstance();
            } catch (java.lang.Exception e) {
                throw new ConversionException(
                        "Cannot instantiate Map type " + raw.getName() + " (no accessible no-arg constructor)", e);
            }
        }

        Object toRecord(JsonObject jo, Class<?> raw) {
            try {
                var components = raw.getRecordComponents();
                Class<?>[] ctorTypes =
                        Arrays.stream(components).map(RecordComponent::getType).toArray(Class[]::new);
                Object[] args = new Object[components.length];
                for (int i = 0; i < components.length; i++) {
                    var c = components[i];
                    JsonValue v = findPropertyValue(jo, c.getName());
                    try {
                        args[i] = (v == null) ? defaultValue(c.getType()) : parseJsonValue(v, c.getGenericType());
                    } catch (Exception e) {
                        throw new ConversionException(
                                "Failed to convert component '" + c.getName() + "' of record " + raw.getName() + ": "
                                        + e.getMessage(),
                                e);
                    } catch (java.lang.Exception e) {
                        throw new ConversionException(
                                "Failed to convert component '" + c.getName() + "' of record " + raw.getName(), e);
                    }
                }
                var ctor = raw.getDeclaredConstructor(ctorTypes);
                makeAccessible(ctor, raw);
                return ctor.newInstance(args);
            } catch (Exception e) {
                throw e;
            } catch (java.lang.Exception e) {
                throw new ConversionException("Failed to construct record instance of type " + raw.getName(), e);
            }
        }

        Object toBean(JsonObject jo, Class<?> raw) {
            Object bean = constructBean(raw, jo);
            try {
                var bi = Introspector.getBeanInfo(raw);
                for (var pd : bi.getPropertyDescriptors()) {
                    if ("class".equals(pd.getName())) continue;
                    var write = pd.getWriteMethod();
                    if (write == null) continue;
                    var parameterType = write.getGenericParameterTypes()[0];
                    var parameterRawType = raw(parameterType);
                    JsonValue v = findPropertyValue(jo, pd.getName());
                    if (v == null) {
                        if (parameterRawType == Optional.class) write.invoke(bean, Optional.empty());
                        continue;
                    }
                    try {
                        Object tv = parseJsonValue(v, parameterType);
                        write.invoke(bean, tv);
                    } catch (Exception e) {
                        throw new ConversionException(
                                "Failed to set property '" + pd.getName() + "' of bean " + raw.getName() + ": "
                                        + e.getMessage(),
                                e);
                    } catch (java.lang.Exception e) {
                        throw new ConversionException(
                                "Failed to set property '" + pd.getName() + "' of bean " + raw.getName(), e);
                    }
                }
                return bean;
            } catch (Exception e) {
                throw e;
            } catch (java.lang.Exception e) {
                throw new ConversionException("Failed to introspect bean of type " + raw.getName());
            }
        }

        Object constructBean(Class<?> raw, JsonObject jo) {
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
                if (unique == null)
                    throw new ConversionException(
                            "No suitable constructor found: expected no-arg constructor or single constructor with parameters");
                makeAccessible(unique, raw);
                var params = unique.getParameters();
                Object[] args = new Object[params.length];
                for (int i = 0; i < params.length; i++) {
                    var p = params[i];
                    var v = findPropertyValue(jo, p.getName());
                    if (v == null) args[i] = defaultValue(p.getType());
                    else args[i] = parseJsonValue(v, p.getParameterizedType());
                }
                return unique.newInstance(args);
            } catch (Exception e) {
                throw e;
            } catch (java.lang.Exception e) {
                throw new ConversionException("Failed to instantiate bean of type " + raw.getName(), e);
            }
        }

        Object toArray(JsonArray ja, Class<?> component) {
            int len = ja.value().size();
            Object arr = Array.newInstance(component, len);
            for (int i = 0; i < len; i++) {
                Array.set(arr, i, parseJsonValue(ja.value().get(i), component));
            }
            return arr;
        }

        Object toCollection(JsonArray ja, java.lang.reflect.Type type) {
            var coll = createCollection(raw(type), ja.value().size());
            var elemType = collectionElementType(type);
            for (var v : ja.value()) coll.add(parseJsonValue(v, elemType));
            return coll;
        }

        static java.lang.reflect.Type collectionElementType(java.lang.reflect.Type t) {
            if (t instanceof ParameterizedType p) return canonicalize(p.getActualTypeArguments()[0]);
            if (t instanceof GenericArrayType ga) return canonicalize(ga.getGenericComponentType());
            if (t instanceof Class<?> c && c.isArray()) return c.getComponentType();
            return Object.class;
        }

        @SuppressWarnings("unchecked")
        static Collection<Object> createCollection(Class<?> raw, int size) {
            if (typeBetween(raw, ArrayList.class, Collection.class)) return new ArrayList<>(size);
            if (typeBetween(raw, LinkedList.class, null)) return new LinkedList<>();
            if (typeBetween(raw, LinkedHashSet.class, null)) return new LinkedHashSet<>(size);
            if (typeBetween(raw, TreeSet.class, null)) return new TreeSet<>();
            if (typeBetween(raw, ArrayDeque.class, null)) return new ArrayDeque<>(size);
            if (typeBetween(raw, PriorityQueue.class, null)) return new PriorityQueue<>(size);
            if (typeBetween(raw, Vector.class, null)) return new Vector<>(size);
            if (typeBetween(raw, Stack.class, null)) return new Stack<>();
            if (typeBetween(raw, ArrayBlockingQueue.class, null)) return new ArrayBlockingQueue<>(size);
            if (typeBetween(raw, LinkedBlockingQueue.class, null)) return new LinkedBlockingQueue<>();
            if (typeBetween(raw, ConcurrentLinkedQueue.class, null)) return new ConcurrentLinkedQueue<>();
            if (typeBetween(raw, ConcurrentSkipListSet.class, null)) return new ConcurrentSkipListSet<>();
            if (typeBetween(raw, CopyOnWriteArrayList.class, null)) return new CopyOnWriteArrayList<>();
            try {
                return (Collection<Object>) raw.getDeclaredConstructor().newInstance();
            } catch (java.lang.Exception e) {
                throw new ConversionException(
                        "Cannot instantiate Collection type " + raw.getName() + " (no accessible no-arg constructor)",
                        e);
            }
        }

        Object fromUntyped(JsonValue jv) {
            if (jv instanceof JsonNull) return null;
            if (jv instanceof JsonBoolean b) return b.value();
            if (jv instanceof JsonNumber n) return n.value();
            if (jv instanceof JsonString s) return s.value();
            if (jv instanceof JsonArray a) {
                List<Object> list = new ArrayList<>(a.value().size());
                for (var e : a.value()) list.add(parseJsonValue(e, Object.class));
                return list;
            }
            if (jv instanceof JsonObject o) {
                Map<String, Object> map = new LinkedHashMap<>();
                for (var en : o.value().entrySet()) map.put(en.getKey(), parseJsonValue(en.getValue(), Object.class));
                return map;
            }
            throw new ConversionException("Unknown JsonValue type: " + jv.getClass());
        }

        static Object toNull(Class<?> raw) {
            if (raw.isPrimitive()) throw new ConversionException("Cannot assign null to primitive type");
            if (raw == Optional.class) return Optional.empty();
            if (raw == AtomicReference.class) return new AtomicReference<>(null);
            return null;
        }

        Object toOptional(JsonValue jv, java.lang.reflect.Type targetType) {
            if (!(targetType instanceof ParameterizedType p)) {
                throw new ConversionException("Optional type must be parameterized (e.g., Optional<String>)");
            }
            if (jv instanceof JsonNull) return Optional.empty();
            return Optional.ofNullable(parseJsonValue(jv, p.getActualTypeArguments()[0]));
        }

        Object toStream(JsonArray ja, java.lang.reflect.Type targetType) {
            if (!(targetType instanceof ParameterizedType p)) {
                throw new ConversionException("Stream type must be parameterized (e.g., Stream<String>) for type "
                        + raw(targetType).getName());
            }
            var elemType = p.getActualTypeArguments()[0];
            var list = new ArrayList<>();
            for (var e : ja.value()) list.add(parseJsonValue(e, elemType));
            Class<?> raw = raw(targetType);
            if (typeBetween(raw, Stream.class, BaseStream.class)) return list.stream();
            if (typeBetween(raw, IntStream.class, null)) return list.stream().mapToInt(e -> ((Number) e).intValue());
            if (typeBetween(raw, LongStream.class, null)) return list.stream().mapToLong(e -> ((Number) e).longValue());
            if (typeBetween(raw, DoubleStream.class, null))
                return list.stream().mapToDouble(e -> ((Number) e).doubleValue());
            throw new ConversionException("Unsupported Stream type");
        }

        static JsonValue findPropertyValue(JsonObject jo, String propertyName) {
            var map = jo.value();
            JsonValue direct = map.get(propertyName);
            if (direct != null) return direct;

            String snake = toSnakeCase(propertyName);
            if (!snake.equals(propertyName)) {
                JsonValue alt = map.get(snake);
                if (alt != null) return alt;
            }

            String camel = toCamelCase(propertyName);
            if (!camel.equals(propertyName)) {
                JsonValue alt = map.get(camel);
                if (alt != null) return alt;
            }

            return null;
        }

        static Object asJsonValue(JsonValue jv, Class<?> raw) {
            if (raw.isInstance(jv)) return jv;
            if (raw == JsonValue.class) return jv;
            throw new ConversionException(
                    "Cannot convert " + jv.getClass().getSimpleName() + " to " + raw.getSimpleName());
        }
    }

    static int mapCap(int size) {
        int n = -1 >>> Integer.numberOfLeadingZeros(size - 1);
        return (n < 0) ? 1 : (n >= 1 << 30) ? 1 << 30 : n + 1;
    }

    static void makeAccessible(Constructor<?> c, Class<?> raw) {
        if (!Modifier.isPublic(c.getModifiers()) || !Modifier.isPublic(raw.getModifiers())) c.setAccessible(true);
    }

    static String toSnakeCase(String name) {
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

    static String toCamelCase(String name) {
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

    static Object defaultValue(Class<?> t) {
        if (t == boolean.class) return false;
        if (t == byte.class) return (byte) 0;
        if (t == short.class) return (short) 0;
        if (t == int.class) return 0;
        if (t == long.class) return 0L;
        if (t == float.class) return 0f;
        if (t == double.class) return 0d;
        if (t == char.class) return '\0';
        if (t == Optional.class) return Optional.empty(); // null Optional joke :)
        return null;
    }

    static Object toBoolean(JsonValue jv, Class<?> raw) {
        if (!(jv instanceof JsonBoolean) && !(jv instanceof JsonString) && !(jv instanceof JsonNumber)) {
            jv = new JsonString(toString(jv));
        }
        boolean bool;
        if (jv instanceof JsonBoolean b) bool = b.value();
        else if (jv instanceof JsonString s) {
            String v = s.value();
            if (v.equalsIgnoreCase("true")) bool = true;
            else if (v.equalsIgnoreCase("false")) bool = false;
            else
                throw new ConversionException(
                        "Cannot convert string to boolean: expected 'true' or 'false', got '" + v + "'");
        } else if (jv instanceof JsonNumber n) {
            int v = n.value().intValue();
            if (v == 0) bool = false;
            else if (v == 1) bool = true;
            else throw new ConversionException("Cannot convert number to boolean: expected 0 or 1, got " + v);
        } else throw new ConversionException("Cannot convert " + jv.getClass().getSimpleName() + " to boolean");

        if (raw == boolean.class || raw == Boolean.class) return bool;
        if (raw == AtomicBoolean.class) return new AtomicBoolean(bool);
        throw new ConversionException("Unsupported boolean target type");
    }

    static Object toNumber(JsonValue jv, Class<?> raw) {
        if (jv instanceof JsonBoolean b) {
            jv = new JsonNumber(b.value() ? BigDecimal.ONE : BigDecimal.ZERO);
        } else if (!(jv instanceof JsonNumber) && !(jv instanceof JsonString)) {
            // last resort: stringify non-number into string, then parse
            jv = new JsonString(toString(jv));
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
                throw new ConversionException(
                        "Cannot parse number from string: '" + s.value() + "' for type " + raw.getName(), e);
            }
        } else throw new ConversionException("Cannot convert " + jv.getClass().getSimpleName() + " to number");

        if (raw == BigDecimal.class) return bd;
        if (raw == BigInteger.class) return new BigInteger(bd.toPlainString());
        if (raw == Number.class) return bd;
        if (raw == double.class || raw == Double.class) return bd.doubleValue();
        if (raw == float.class || raw == Float.class) return bd.floatValue();
        if (raw == long.class || raw == Long.class) return bd.longValue();
        if (raw == int.class || raw == Integer.class) return bd.intValue();
        if (raw == short.class || raw == Short.class) return bd.shortValue();
        if (raw == byte.class || raw == Byte.class) return bd.byteValue();
        if (raw == AtomicInteger.class) return new AtomicInteger(bd.intValue());
        if (raw == AtomicLong.class) return new AtomicLong(bd.longValue());
        throw new ConversionException("Unsupported numeric target type");
    }

    static String toString(JsonValue jv) {
        if (jv instanceof JsonString s) return s.value();
        return stringify(jv);
    }

    static Object toEnum(JsonValue jv, Class<?> raw) {
        if (!(jv instanceof JsonString) && !(jv instanceof JsonNumber)) {
            jv = new JsonString(toString(jv)); // fallback to textual form
        }
        if (jv instanceof JsonString s) {
            for (Object ec : raw.getEnumConstants()) if (((Enum<?>) ec).name().equalsIgnoreCase(s.value())) return ec;
            throw new ConversionException(
                    "No enum constant found with name '" + s.value() + "' in " + raw.getSimpleName());
        }
        if (jv instanceof JsonNumber n) {
            int ord = n.value().intValue();
            Object[] cs = raw.getEnumConstants();
            if (ord < 0 || ord >= cs.length)
                throw new ConversionException(
                        "Enum ordinal out of range: expected 0-" + (cs.length - 1) + ", got " + ord);
            return cs[ord];
        }
        throw new ConversionException("Cannot convert " + jv.getClass().getSimpleName() + " to enum");
    }

    static boolean isTemporal(Class<?> raw) {
        return raw == Instant.class
                || raw == Date.class
                || raw == Timestamp.class
                || raw == LocalDate.class
                || raw == LocalTime.class
                || raw == LocalDateTime.class
                || raw == ZonedDateTime.class
                || raw == OffsetDateTime.class
                || raw == Duration.class
                || raw == Year.class
                || raw == YearMonth.class
                || raw == MonthDay.class
                || raw == Period.class
                || raw == ZoneOffset.class
                || raw == ZoneId.class;
    }

    static boolean isStringBasedType(Class<?> raw) {
        return raw == UUID.class
                || raw == Locale.class
                || raw == Currency.class
                || typeBetween(raw, SimpleTimeZone.class, TimeZone.class)
                || raw == URI.class
                || raw == URL.class
                || raw == Path.class
                || raw == Pattern.class;
    }

    static Object toTemporal(JsonValue jv, Class<?> raw) {
        if (!(jv instanceof JsonString) && !(jv instanceof JsonNumber)) {
            jv = new JsonString(toString(jv));
        }
        try {
            if (raw == Instant.class) {
                if (jv instanceof JsonString s) return Instant.parse(s.value());
                if (jv instanceof JsonNumber n)
                    return Instant.ofEpochMilli(n.value().longValue());
            } else if (raw == Date.class) {
                Instant i = (Instant) toTemporal(jv, Instant.class);
                return Date.from(i);
            } else if (raw == Timestamp.class) {
                Instant i = (Instant) toTemporal(jv, Instant.class);
                return Timestamp.from(i);
            } else if (raw == Duration.class) {
                if (jv instanceof JsonString s) return Duration.parse(s.value());
                if (jv instanceof JsonNumber n)
                    return Duration.ofMillis(n.value().longValue());
            } else if (raw == Period.class) {
                if (jv instanceof JsonString s) return Period.parse(s.value());
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
            } else if (raw == Year.class) {
                if (jv instanceof JsonString s) return Year.parse(s.value());
                if (jv instanceof JsonNumber n) return Year.of(n.value().intValue());
            } else if (raw == YearMonth.class) {
                if (jv instanceof JsonString s) return YearMonth.parse(s.value());
            } else if (raw == MonthDay.class) {
                if (jv instanceof JsonString s) return MonthDay.parse(s.value());
            } else if (raw == ZoneOffset.class) {
                if (jv instanceof JsonString s) return ZoneOffset.of(s.value());
            } else if (raw == ZoneId.class) {
                if (jv instanceof JsonString s) return ZoneId.of(s.value());
            }
        } catch (java.lang.Exception e) {
            String value = jv instanceof JsonString s ? s.value() : String.valueOf(jv);
            throw new ConversionException("Cannot parse " + raw.getSimpleName() + " from value: '" + value + "'", e);
        }
        throw new ConversionException("Cannot convert " + jv.getClass().getSimpleName() + " to " + raw.getSimpleName());
    }

    // ============================================================
    // Type utils
    // ============================================================

    static List<Codec> loadCodecs() {
        var codecs = new ArrayList<Codec>();
        for (var c : ServiceLoader.load(Codec.class)) codecs.add(c);
        return codecs;
    }

    static Class<?> raw(java.lang.reflect.Type t) {
        if (t instanceof Class<?> c) return c;
        if (t instanceof ParameterizedType p) return (Class<?>) p.getRawType();
        if (t instanceof GenericArrayType ga) {
            var comp = raw(ga.getGenericComponentType());
            return Array.newInstance(comp, 0).getClass();
        }
        if (t instanceof TypeVariable<?> tv) return raw(erasureOf(tv));
        if (t instanceof WildcardType w) return raw(erasureOf(w));
        throw new IllegalArgumentException("Unsupported type: " + t);
    }

    static boolean typeBetween(Class<?> raw, Class<?> lower, Class<?> upper) {
        return (lower == null || raw.isAssignableFrom(lower)) && (upper == null || upper.isAssignableFrom(raw));
    }

    static boolean isClassPresent(String name) {
        try {
            Class.forName(name);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    static java.lang.reflect.Type canonicalize(java.lang.reflect.Type t) {
        if (t instanceof WildcardType w) return erasureOf(w);
        if (t instanceof TypeVariable<?> tv) return erasureOf(tv);
        return t;
    }

    static java.lang.reflect.Type erasureOf(WildcardType w) {
        var uppers = w.getUpperBounds();
        return uppers.length == 0 ? Object.class : uppers[0];
    }

    static java.lang.reflect.Type erasureOf(TypeVariable<?> tv) {
        var uppers = tv.getBounds();
        return uppers.length == 0 ? Object.class : uppers[0];
    }

    /**
     * Exception thrown when JSON parsing, serialization, or type conversion fails.
     * This is the base exception for all json4j-related errors.
     *
     * @author Freeman
     * @since 0.3.0
     */
    public abstract static class Exception extends RuntimeException {
        public Exception(String message) {
            super(message);
        }

        public Exception(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Exception thrown during JSON serialization (write operations).
     *
     * <p> This exception is thrown when converting Java objects to JSON fails.
     *
     * @since 0.3.0
     */
    public static class WriteException extends Exception {
        public WriteException(String message) {
            super(message);
        }

        public WriteException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Exception thrown when JSON parsing fails due to malformed JSON syntax.
     *
     * @since 0.3.0
     */
    public static class SyntaxException extends Exception {
        public SyntaxException(String message) {
            super(message);
        }

        public SyntaxException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Exception thrown when converting {@link JsonValue} to Java objects fails during deserialization.
     *
     * <p> This includes type conversion errors, bean/record mapping errors, and other conversion failures.
     *
     * @since 0.3.0
     */
    public static class ConversionException extends Exception {
        public ConversionException(String message) {
            super(message);
        }

        public ConversionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
