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
import java.util.regex.Pattern;
import java.util.stream.BaseStream;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Minimal, standard-first JSON writer and parser.
 *
 * @author <a href="mailto:llw599502537@gmail.com">Freeman</a>
 */
public final class Json {

    private static final List<Codec> codecs = loadCodecs();

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
        new Writer(sb).write(o);
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

        void serialize(Writer writer, Object o);

        boolean canDeserialize(JsonValue jv, java.lang.reflect.Type targetType);

        Object deserialize(JsonValue jv, java.lang.reflect.Type targetType);
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

    static final class Parser {
        final Lexer lexer;

        Parser(Lexer l) {
            this.lexer = l;
        }

        JsonValue parse() {
            JsonValue v = parseValue();
            if (lexer.current() != Token.EOF) error("Trailing characters after top-level value");
            return v;
        }

        void expect(Token t) {
            if (lexer.current() != t) error("Expected " + t + " but found " + lexer.current());
            lexer.advance();
        }

        boolean accept(Token t) {
            if (lexer.current() == t) {
                lexer.advance();
                return true;
            }
            return false;
        }

        JsonValue parseValue() {
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

        JsonObject parseObject() {
            expect(Token.LBRACE);
            Map<String, JsonValue> m = new LinkedHashMap<>();
            if (accept(Token.RBRACE)) return new JsonObject(m);
            while (true) {
                if (lexer.current() != Token.STRING) error("Expected string key in object");
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

        JsonArray parseArray() {
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

        void error(String msg) {
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
    }

    // ============================================================
    // Writer (correct escaping, faster stringify)
    // ============================================================

    public static final class Writer {
        final StringBuilder out;

        public Writer(StringBuilder sb) {
            this.out = sb;
        }

        public void write(Object o) {
            if (o == null) {
                out.append("null");
                return;
            }
            if (o instanceof JsonValue jv) {
                writeJsonValue(jv);
                return;
            }
            for (var codec : codecs) {
                if (codec.canSerialize(o)) {
                    codec.serialize(this, o);
                    return;
                }
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
            if (o instanceof Optional<?> optional) {
                if (optional.isEmpty()) out.append("null");
                else write(optional.get());
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
            if (o instanceof BaseStream<?, ?> stream) {
                writeStream(stream);
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

            // temporal types -> string
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
            if (o instanceof MonthDay md) {
                writeString(md.toString());
                return;
            }
            if (o instanceof Period p) {
                writeString(p.toString());
                return;
            }
            if (o instanceof ZoneOffset zo) {
                writeString(zo.toString());
                return;
            }
            if (o instanceof ZoneId zi) {
                writeString(zi.toString());
                return;
            }

            // java.util types -> string
            if (o instanceof UUID uuid) {
                writeString(uuid.toString());
                return;
            }
            if (o instanceof Locale locale) {
                writeString(locale.toLanguageTag());
                return;
            }
            if (o instanceof Currency currency) {
                writeString(currency.getCurrencyCode());
                return;
            }
            if (o instanceof TimeZone tz) {
                writeString(tz.getID());
                return;
            }

            // java.net types -> string
            if (o instanceof URI uri) {
                writeString(uri.toString());
                return;
            }
            if (o instanceof URL url) {
                writeString(url.toString());
                return;
            }

            // java.nio.file.Path -> string
            if (o instanceof Path path) {
                writeString(path.toString());
                return;
            }

            // java.util.regex.Pattern -> string
            if (o instanceof Pattern pattern) {
                writeString(pattern.pattern());
                return;
            }

            // record / bean
            if (o instanceof Record) {
                writeRecord(o);
                return;
            }
            writeBean(o);
        }

        private void writeStream(BaseStream<?, ?> stream) {
            out.append('[');
            boolean first = true;
            try (var s = stream) {
                var it = Spliterators.iterator(s.spliterator());
                while (it.hasNext()) {
                    if (!first) out.append(',');
                    first = false;
                    write(it.next());
                }
            }
            out.append(']');
        }

        void writeJsonValue(JsonValue v) {
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
            throw new WriteException("Unknown JsonValue type: " + v.getClass());
        }

        void writeString(String s) {
            out.append('"');
            escapeTo(out, s);
            out.append('"');
        }

        void writeNumber(Number n) {
            if (n instanceof BigDecimal || n instanceof BigInteger) {
                out.append(n.toString());
                return;
            }
            // Avoid NaN/Infinity (not valid in JSON)
            double d = n.doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d))
                throw new WriteException("Cannot serialize NaN or Infinity as JSON number: " + n);
            out.append(n.toString());
        }

        private void writeArray(Object arr) {
            out.append('[');
            int len = Array.getLength(arr);
            for (int i = 0; i < len; i++) {
                if (i > 0) out.append(',');
                write(Array.get(arr, i));
            }
            out.append(']');
        }

        private void writeIterable(Iterable<?> it) {
            out.append('[');
            boolean first = true;
            for (Object e : it) {
                if (!first) out.append(',');
                first = false;
                write(e);
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
                write(en.getValue());
            }
            out.append('}');
        }

        private void writeRecord(Object r) {
            out.append('{');
            boolean first = true;
            for (var c : r.getClass().getRecordComponents()) {
                try {
                    Object v = c.getAccessor().invoke(r);
                    if (v instanceof Optional<?> optional) {
                        if (optional.isEmpty()) continue;
                        v = optional.get();
                    }
                    if (!first) out.append(',');
                    first = false;
                    writeString(c.getName());
                    out.append(':');
                    write(v);
                } catch (java.lang.Exception e) {
                    throw new WriteException(
                            "Failed to access record component '" + c.getName() + "' of type "
                                    + r.getClass().getName(),
                            e);
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
                    try {
                        Object v = read.invoke(bean);
                        if (v instanceof Optional<?> optional) {
                            if (optional.isEmpty()) continue;
                            v = optional.get();
                        }
                        if (!first) out.append(',');
                        first = false;
                        writeString(pd.getName());
                        out.append(':');
                        write(v);
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
    // JsonValue -> Object
    // ============================================================

    @SuppressWarnings("unchecked")
    static <T> T fromJsonValue(JsonValue jv, java.lang.reflect.Type targetType) {
        // Normalize reflective type, obtain raw class
        targetType = canonicalize(targetType);
        Class<?> raw = raw(targetType);

        // 0) Trivial/dynamic targets
        if (raw == Object.class) return (T) fromUntyped(jv); // best-effort untyped
        if (JsonValue.class.isAssignableFrom(raw)) return (T) asJsonValue(jv, raw); // return AST as-is

        // Custom codec
        for (var codec : codecs) {
            if (codec.canDeserialize(jv, targetType)) {
                return (T) codec.deserialize(jv, targetType);
            }
        }

        // 1) Null handling
        if (jv instanceof JsonNull) return (T) nullValueFor(raw);

        // 2) Scalar targets (LOOSE)

        // 2.1 boolean: accept JsonBoolean, "true"/"false", "1"/"0", numeric 1/0
        if (raw == boolean.class || raw == Boolean.class) {
            return (T) coerceBoolean(jv, raw);
        }

        // 2.2 number: accept JsonNumber, numeric strings, and booleans (true->1, false->0)
        if (Number.class.isAssignableFrom(raw) || (raw.isPrimitive() && raw != char.class)) {
            return (T) coerceNumber(jv, raw);
        }

        // 2.3 String/CharSequence: stringify any JsonValue (numbers/booleans become their textual form)
        if (raw == String.class || raw == CharSequence.class) {
            return (T) coerceString(jv); // json string should have quote, Java String should not have quote
        }

        // 2.4 char/Character: accept 1-char string (or stringify first)
        if (raw == char.class || raw == Character.class) {
            String s = coerceString(jv);
            if (s.length() != 1)
                throw new ConversionException(
                        "Cannot convert string to char: expected length 1, got " + s.length() + " (\"" + s + "\")");
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

        // 2.7 string-based types: UUID, Locale, Currency, TimeZone, URI, URL, Path, Pattern
        if (isStringBasedType(raw)) {
            return (T) coerceStringBasedType(jv, raw);
        }

        // 2.8 optional: wrap the inner type
        if (raw == Optional.class) {
            return (T) coerceOptional(jv, targetType);
        }

        // 3) Structured targets (LOOSE)

        // 3.1 arrays: if non-array provided, wrap single element
        if (raw.isArray()) {
            JsonArray ja = (jv instanceof JsonArray) ? (JsonArray) jv : new JsonArray(List.of(jv));
            return (T) coerceArray(ja, raw.getComponentType());
        }

        // 3.2 Collection: if non-array provided, wrap single element
        if (Iterable.class.isAssignableFrom(raw)) {
            JsonArray ja = (jv instanceof JsonArray) ? (JsonArray) jv : new JsonArray(List.of(jv));
            return (T) coerceIterable(ja, targetType);
        }

        // 3.3 Stream
        if (BaseStream.class.isAssignableFrom(raw)) {
            JsonArray ja = (jv instanceof JsonArray) ? (JsonArray) jv : new JsonArray(List.of(jv));
            return (T) coerceStream(ja, targetType);
        }

        // Must JsonObject here!
        JsonObject jo = expectObject(jv);

        // 3.4 Map
        if (Map.class.isAssignableFrom(raw)) {
            return (T) coerceMap(jo, targetType);
        }

        // 3.5 Record
        if (raw.isRecord()) return (T) coerceRecord(jo, raw);

        // 3.6 Java bean
        return (T) coerceBean(jo, raw);
    }

    static Object coerceMap(JsonObject jo, java.lang.reflect.Type type) {
        var keyType = mapKeyType(type);
        var valueType = mapValueType(type);
        var map = createMap(raw(type), jo.value().size());
        for (var en : jo.value().entrySet()) {
            Object key = fromJsonValue(new JsonString(en.getKey()), keyType);
            Object val = fromJsonValue(en.getValue(), valueType);
            map.put(key, val);
        }
        return map;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static Map<Object, Object> createMap(Class<?> raw, int size) {
        int cap = mapCap(size);
        if (typeBetween(raw, LinkedHashMap.class, Map.class)) return new LinkedHashMap<>(cap);
        if (typeBetween(raw, TreeMap.class, null)) return new TreeMap<>();
        if (typeBetween(raw, EnumMap.class, null)) {
            if (!typeBetween(raw, null, Enum.class)) throw new ConversionException("EnumMap requires Enum key type");
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

    static int mapCap(int size) {
        int n = -1 >>> Integer.numberOfLeadingZeros(size - 1);
        return (n < 0) ? 1 : (n >= 1 << 30) ? 1 << 30 : n + 1;
    }

    static Object coerceRecord(JsonObject jo, Class<?> raw) {
        try {
            var components = raw.getRecordComponents();
            Class<?>[] ctorTypes =
                    Arrays.stream(components).map(RecordComponent::getType).toArray(Class[]::new);
            Object[] args = new Object[components.length];
            for (int i = 0; i < components.length; i++) {
                var c = components[i];
                JsonValue v = findPropertyValue(jo, c.getName());
                try {
                    args[i] = (v == null) ? defaultValue(c.getType()) : fromJsonValue(v, c.getGenericType());
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

    static Object coerceBean(JsonObject jo, Class<?> raw) {
        Object bean = constructBean(raw);
        try {
            var bi = Introspector.getBeanInfo(raw);
            for (var pd : bi.getPropertyDescriptors()) {
                if ("class".equals(pd.getName())) continue;
                var write = pd.getWriteMethod();
                if (write == null) continue;
                JsonValue v = findPropertyValue(jo, pd.getName());
                if (v == null) continue;
                try {
                    Object tv = fromJsonValue(v, write.getGenericParameterTypes()[0]);
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

    static Object constructBean(Class<?> raw) {
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
            // fill defaults; will be overwritten via setters if present
            for (int i = 0; i < params.length; i++) args[i] = defaultValue(params[i].getType());
            return unique.newInstance(args);
        } catch (Exception e) {
            throw e;
        } catch (java.lang.Exception e) {
            throw new ConversionException("Failed to instantiate bean of type " + raw.getName());
        }
    }

    static void makeAccessible(Constructor<?> c, Class<?> raw) {
        if (!Modifier.isPublic(c.getModifiers()) || !Modifier.isPublic(raw.getModifiers())) c.setAccessible(true);
    }

    static JsonValue findPropertyValue(JsonObject jo, String propertyName) {
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

    static Object coerceArray(JsonArray ja, Class<?> component) {
        int len = ja.value().size();
        Object arr = Array.newInstance(component, len);
        for (int i = 0; i < len; i++) {
            Array.set(arr, i, fromJsonValue(ja.value().get(i), component));
        }
        return arr;
    }

    static Object coerceIterable(JsonArray ja, java.lang.reflect.Type type) {
        var coll = createCollection(raw(type), ja.value().size());
        var elemType = collectionElementType(type);
        for (var v : ja.value()) coll.add(fromJsonValue(v, elemType));
        return coll;
    }

    @SuppressWarnings("unchecked")
    static Collection<Object> createCollection(Class<?> raw, int size) {
        if (typeBetween(raw, ArrayList.class, Iterable.class)) return new ArrayList<>(size);
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
                    "Cannot instantiate Collection type " + raw.getName() + " (no accessible no-arg constructor)", e);
        }
    }

    static JsonObject expectObject(JsonValue jv) {
        if (jv instanceof JsonObject o) return o;
        throw new ConversionException(
                "Expected JSON object, but got " + jv.getClass().getSimpleName());
    }

    static Object asJsonValue(JsonValue jv, Class<?> raw) {
        if (raw.isInstance(jv)) return jv;
        if (raw == JsonValue.class) return jv;
        throw new ConversionException("Cannot convert " + jv.getClass().getSimpleName() + " to " + raw.getSimpleName());
    }

    static Object fromUntyped(JsonValue jv) {
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
        throw new ConversionException("Unknown JsonValue type: " + jv.getClass());
    }

    static Object nullValueFor(Class<?> raw) {
        if (raw.isPrimitive()) throw new ConversionException("Cannot assign null to primitive type");
        return null;
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

    static Object coerceBoolean(JsonValue jv, Class<?> raw) {
        if (!(jv instanceof JsonBoolean) && !(jv instanceof JsonString) && !(jv instanceof JsonNumber)) {
            jv = new JsonString(coerceString(jv));
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
        throw new ConversionException("Unsupported boolean target type");
    }

    static Object coerceNumber(JsonValue jv, Class<?> raw) {
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
        throw new ConversionException("Unsupported numeric target type");
    }

    static String coerceString(JsonValue jv) {
        if (jv instanceof JsonString s) return s.value();
        return stringify(jv);
    }

    static Object coerceEnum(JsonValue jv, Class<?> raw) {
        if (!(jv instanceof JsonString) && !(jv instanceof JsonNumber)) {
            jv = new JsonString(coerceString(jv)); // fallback to textual form
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
                || raw == Duration.class
                || raw == Year.class
                || raw == YearMonth.class
                || raw == MonthDay.class
                || raw == Period.class
                || raw == ZoneOffset.class
                || raw == ZoneId.class;
    }

    private static boolean isStringBasedType(Class<?> raw) {
        return raw == UUID.class
                || raw == Locale.class
                || raw == Currency.class
                || raw == TimeZone.class
                || raw == URI.class
                || raw == URL.class
                || raw == Path.class
                || raw == Pattern.class;
    }

    private static Object coerceTemporal(JsonValue jv, Class<?> raw) {
        if (!(jv instanceof JsonString) && !(jv instanceof JsonNumber)) {
            jv = new JsonString(coerceString(jv));
        }
        try {
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

    private static Object coerceStringBasedType(JsonValue jv, Class<?> raw) {
        String value = coerceString(jv);
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
            throw new ConversionException("Cannot parse " + raw.getSimpleName() + " from value: '" + value + "'", e);
        }
        throw new ConversionException("Cannot convert to " + raw.getSimpleName());
    }

    static Object coerceOptional(JsonValue jv, java.lang.reflect.Type targetType) {
        if (!(targetType instanceof ParameterizedType p)) {
            throw new ConversionException("Optional type must be parameterized (e.g., Optional<String>)");
        }
        if (jv instanceof JsonNull) return Optional.empty();
        return Optional.ofNullable(fromJsonValue(jv, p.getActualTypeArguments()[0]));
    }

    static Object coerceStream(JsonArray ja, java.lang.reflect.Type targetType) {
        if (!(targetType instanceof ParameterizedType p)) {
            throw new ConversionException("Stream type must be parameterized (e.g., Stream<String>) for type "
                    + raw(targetType).getName());
        }
        var elemType = p.getActualTypeArguments()[0];
        var list = new ArrayList<>();
        for (var e : ja.value()) list.add(fromJsonValue(e, elemType));
        Class<?> raw = raw(targetType);
        if (typeBetween(raw, Stream.class, BaseStream.class)) return list.stream();
        if (typeBetween(raw, IntStream.class, null)) return list.stream().mapToInt(e -> ((Number) e).intValue());
        if (typeBetween(raw, LongStream.class, null)) return list.stream().mapToLong(e -> ((Number) e).longValue());
        if (typeBetween(raw, DoubleStream.class, null))
            return list.stream().mapToDouble(e -> ((Number) e).doubleValue());
        throw new ConversionException("Unsupported Stream type");
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

    static java.lang.reflect.Type collectionElementType(java.lang.reflect.Type t) {
        if (t instanceof ParameterizedType p) return canonicalize(p.getActualTypeArguments()[0]);
        if (t instanceof GenericArrayType ga) return canonicalize(ga.getGenericComponentType());
        if (t instanceof Class<?> c && c.isArray()) return c.getComponentType();
        return Object.class;
    }

    static boolean isClassPresent(String name) {
        try {
            Class.forName(name);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    static java.lang.reflect.Type mapKeyType(java.lang.reflect.Type t) {
        if (t instanceof ParameterizedType p) return canonicalize(p.getActualTypeArguments()[0]);
        return String.class; // JSON keys are strings
    }

    static java.lang.reflect.Type mapValueType(java.lang.reflect.Type t) {
        if (t instanceof ParameterizedType p) return canonicalize(p.getActualTypeArguments()[1]);
        return Object.class;
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
