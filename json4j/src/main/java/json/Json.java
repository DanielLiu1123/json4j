package json;

import java.beans.Introspector;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
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
import java.util.stream.Collectors;

public final class Json {

    private Json() {
        throw new UnsupportedOperationException();
    }

    // ------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------

    /**
     * Convert any Java object to JSON string.
     * @param o object to convert, can be null
     * @return JSON string representation of the object, never null
     */
    public static String stringify(Object o) {
        return toJsonValue(o).toString();
    }

    /**
     * Parse JSON string into the target Type reference.
     *
     * @param json JSON string to parse, must not be null
     * @param type target type reference, must not be null
     * @return parsed object, can be null if json is "null"
     * @param <T> target type
     */
    public static <T> T parse(String json, Type<T> type) {
        Objects.requireNonNull(json);
        Objects.requireNonNull(type);
        var jsonValue = toJsonValue(json);
        return fromJsonValue(jsonValue, type.getType());
    }

    /**
     * Parse JSON string into a target Class.
     * @param json JSON string to parse, must not be null
     * @param type target class, must not be null
     * @return parsed object, can be null if json is "null"
     * @param <T> target type
     */
    public static <T> T parse(String json, Class<T> type) {
        Objects.requireNonNull(type);
        return parse(json, Type.of(type));
    }

    // ------------------------------------------------------------
    // JSON AST
    // ------------------------------------------------------------

    public sealed interface JsonValue permits JsonArray, JsonBoolean, JsonNull, JsonNumber, JsonObject, JsonString {
        @Override
        String toString();
    }

    public record JsonArray(List<JsonValue> value) implements JsonValue {
        @Override
        public String toString() {
            return value.stream().map(JsonValue::toString).collect(Collectors.joining(", ", "[", "]"));
        }
    }

    public record JsonBoolean(boolean value) implements JsonValue {
        @Override
        public String toString() {
            return value ? "true" : "false";
        }
    }

    public record JsonNull() implements JsonValue {
        @Override
        public String toString() {
            return "null";
        }
    }

    public record JsonNumber(Number value) implements JsonValue {
        @Override
        public String toString() {
            return value.toString();
        }
    }

    public record JsonObject(Map<String, JsonValue> value) implements JsonValue {
        @Override
        public String toString() {
            return value.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(e -> "\"%s\": %s".formatted(e.getKey(), e.getValue()))
                    .collect(Collectors.joining(", ", "{", "}"));
        }
    }

    public record JsonString(String value) implements JsonValue {
        @Override
        public String toString() {
            return "\"%s\"".formatted(value);
        }
    }

    // ------------------------------------------------------------
    // Type token
    // ------------------------------------------------------------

    public abstract static class Type<T> {
        private final java.lang.reflect.Type type;

        protected Type() {
            var typeSubclass = findTypeSubclass(getClass());
            var parameterizedType = (ParameterizedType) typeSubclass.getGenericSuperclass();
            var actualTypeArguments = parameterizedType.getActualTypeArguments();
            this.type = actualTypeArguments[0];
        }

        private Type(java.lang.reflect.Type type) {
            this.type = type;
        }

        public static <T> Type<T> of(Class<T> clazz) {
            return new Type<>(clazz) {};
        }

        public java.lang.reflect.Type getType() {
            return type;
        }

        private static Class<?> findTypeSubclass(Class<?> child) {
            Class<?> parent = child.getSuperclass();
            if (Object.class == parent) throw new IllegalStateException("Expected Json.Type superclass");
            if (Type.class == parent) return child;
            return findTypeSubclass(parent);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            return Objects.equals(type, ((Type<?>) o).type);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(type);
        }

        @Override
        public String toString() {
            return "Type{" + "type=" + type + '}';
        }
    }

    // ------------------------------------------------------------
    // Lexer / Parser
    // ------------------------------------------------------------

    public enum Token {
        LBRACE, // {
        RBRACE, // }
        LBRACKET, // [
        RBRACKET, // ]
        COLON, // :
        COMMA, // ,
        STRING, // "..."
        NUMBER, // 123
        TRUE, // true
        FALSE, // false
        NULL, // null
        EOF
    }

    public static final class Lexer {
        private final String s;
        private int i = 0;
        private int line = 1;
        private int col = 1;
        private Token current;
        private String stringValue;
        private String numberLexeme;

        public Lexer(String s) {
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
                        return;
                    }
                    error("Unexpected character: '" + c + "'");
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
            consume(); // opening quote
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
                            } else if (Character.isLowSurrogate((char) cp)) error("Unexpected low surrogate");
                            else sb.append((char) cp);
                        }
                        default -> throw new IllegalStateException("Invalid escape: \\" + e);
                    }
                } else {
                    if (c < 0x20) error("Control char in string");
                    sb.append(c);
                }
            }
            error("Unterminated string");
            return null; // unreachable
        }

        private int readHex4() {
            int cp = 0;
            for (int k = 0; k < 4; k++) {
                if (eof()) error("Unexpected end in \\u escape");
                char h = consume();
                int v = hexVal(h);
                if (v < 0) error("Invalid hex in \\u escape: " + h);
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

    public static final class Parser {
        private final Lexer lexer;

        Parser(Lexer lexer) {
            this.lexer = lexer;
        }

        public JsonValue parse() {
            JsonValue v = parseValue();
            if (lexer.current() != Token.EOF) error("Trailing characters after top-level value");
            return v;
        }

        private boolean accept(Token t) {
            if (lexer.current() == t) {
                lexer.advance();
                return true;
            }
            return false;
        }

        private void expect(Token t) {
            if (lexer.current() != t) error("Expected " + t + " but found " + lexer.current());
            lexer.advance();
        }

        private JsonValue parseValue() {
            switch (lexer.current()) {
                case LBRACE:
                    return parseObject();
                case LBRACKET:
                    return parseArray();
                case STRING: {
                    String s = lexer.string();
                    lexer.advance();
                    return new JsonString(s);
                }
                case NUMBER: {
                    String n = lexer.number();
                    lexer.advance();
                    return new JsonNumber(new BigDecimal(n));
                }
                case TRUE:
                    lexer.advance();
                    return new JsonBoolean(true);
                case FALSE:
                    lexer.advance();
                    return new JsonBoolean(false);
                case NULL:
                    lexer.advance();
                    return new JsonNull();
                case EOF:
                    error("Unexpected end of input while expecting a value");
                    return null; // unreachable
                default:
                    error("Unexpected token: " + lexer.current());
                    return null; // unreachable
            }
        }

        private JsonObject parseObject() {
            expect(Token.LBRACE);
            Map<String, JsonValue> map = new LinkedHashMap<>();
            if (accept(Token.RBRACE)) return new JsonObject(map);
            while (true) {
                if (lexer.current() != Token.STRING) error("Expected string as object key");
                String key = lexer.string();
                lexer.advance();
                expect(Token.COLON);
                JsonValue val = parseValue();
                map.put(key, val);
                if (accept(Token.COMMA)) continue;
                else if (accept(Token.RBRACE)) break;
                else error("Expected ',' or '}' in object");
            }
            return new JsonObject(map);
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
    }

    // ------------------------------------------------------------
    // Object -> JsonValue
    // ------------------------------------------------------------

    private static JsonValue toJsonValue(String json) {
        return new Parser(new Lexer(json)).parse();
    }

    private static JsonValue toJsonValue(Object o) {
        if (o instanceof JsonValue jv) return jv;
        if (isNullLike(o)) return toJsonNull(o);
        if (isBooleanLike(o)) return toJsonBoolean(o);
        if (isNumberLike(o)) return toJsonNumber(o);
        if (isStringLike(o)) return toJsonString(o);
        if (isArrayLike(o)) return toJsonArray(o);
        return toJsonObject(o);
    }

    private static boolean isNullLike(Object o) {
        return o == null || isNullLikeType(o.getClass());
    }

    private static boolean isNullLikeType(Class<?> type) {
        return type == JsonNull.class;
    }

    private static boolean isBooleanLike(Object o) {
        return isBooleanLikeType(o.getClass());
    }

    private static boolean isBooleanLikeType(Class<?> type) {
        return type == JsonBoolean.class || type == Boolean.class || type == boolean.class;
    }

    private static boolean isStringLike(Object o) {
        return isStringLikeType(o.getClass());
    }

    private static boolean isStringLikeType(Class<?> type) {
        return type == JsonString.class
                || CharSequence.class.isAssignableFrom(type)
                || type == char.class
                || type == Character.class
                || type.isEnum()
                || Date.class.isAssignableFrom(type)
                || type == LocalDate.class
                || type == LocalTime.class
                || type == LocalDateTime.class
                || type == ZonedDateTime.class
                || type == OffsetDateTime.class
                || type == Instant.class
                || type == Duration.class;
    }

    private static boolean isNumberLike(Object o) {
        return isNumberLikeType(o.getClass());
    }

    private static boolean isNumberLikeType(Class<?> type) {
        return type == JsonNumber.class
                || Number.class.isAssignableFrom(type)
                || type == byte.class
                || type == short.class
                || type == int.class
                || type == long.class
                || type == float.class
                || type == double.class;
    }

    private static boolean isArrayLike(Object o) {
        return o.getClass() == JsonArray.class || o.getClass().isArray() || o instanceof Iterable<?>;
    }

    private static String mapKeyToString(Object key) {
        if (key == null) return "null";
        if (isStringLikeType(key.getClass())) return toJsonString(key).value(); // no quotes
        if (isNullLikeType(key.getClass())) return toJsonNull(key).toString();
        if (isBooleanLikeType(key.getClass())) return toJsonBoolean(key).toString();
        if (isNumberLikeType(key.getClass())) return toJsonNumber(key).toString();
        throw new IllegalStateException("Unsupported map key type: " + key.getClass());
    }

    private static JsonString toJsonString(Object o) {
        if (o instanceof JsonString js) return js;
        if (o instanceof CharSequence s) return new JsonString(s.toString());
        if (o instanceof Character c) return new JsonString(c.toString());
        if (o instanceof Enum<?> e) return new JsonString(e.name());
        if (o instanceof Date d) return new JsonString(d.toInstant().toString());
        if (o instanceof LocalDate ld) return new JsonString(ld.toString());
        if (o instanceof LocalTime lt) return new JsonString(lt.toString());
        if (o instanceof LocalDateTime ldt) return new JsonString(ldt.toString());
        if (o instanceof Instant i) return new JsonString(i.toString());
        if (o instanceof ZonedDateTime zdt) return new JsonString(zdt.toString());
        if (o instanceof OffsetDateTime odt) return new JsonString(odt.toString());
        if (o instanceof Duration du) return new JsonString(du.toString());
        throw new IllegalStateException("Unsupported string type: " + o.getClass());
    }

    private static JsonNull toJsonNull(Object o) {
        if (o instanceof JsonNull jn) return jn;
        if (o == null) return new JsonNull();
        if (isStringLike(o)) {
            var v = toJsonString(o).value();
            if (v.equalsIgnoreCase("null")) return new JsonNull();
            throw new IllegalStateException("Cannot convert string to null: " + o);
        }
        throw new IllegalStateException("Unsupported null type: " + o.getClass());
    }

    private static JsonBoolean toJsonBoolean(Object o) {
        if (o instanceof JsonBoolean jb) return jb;
        if (o instanceof Boolean b) return new JsonBoolean(b);
        if (isStringLike(o)) {
            var v = toJsonString(o).value();
            if (v.equalsIgnoreCase("true")) return new JsonBoolean(true);
            if (v.equalsIgnoreCase("false")) return new JsonBoolean(false);
            throw new IllegalStateException("Cannot convert string to boolean: " + o);
        }
        throw new IllegalStateException("Unsupported boolean type: " + o.getClass());
    }

    private static JsonNumber toJsonNumber(Object o) {
        if (o instanceof JsonNumber jn) return jn;
        if (o instanceof Number n) return new JsonNumber(n);
        if (o == byte.class) return new JsonNumber((byte) o);
        if (o == short.class) return new JsonNumber((short) o);
        if (o == int.class) return new JsonNumber((int) o);
        if (o == long.class) return new JsonNumber((long) o);
        if (o == float.class) return new JsonNumber((float) o);
        if (o == double.class) return new JsonNumber((double) o);
        if (isStringLike(o)) {
            var v = toJsonString(o).value();
            try {
                return new JsonNumber(new BigDecimal(v));
            } catch (NumberFormatException e) {
                throw new IllegalStateException("Cannot convert string to number: " + o, e);
            }
        }
        throw new IllegalStateException("Unsupported number type: " + o.getClass());
    }

    private static JsonArray toJsonArray(Object o) {
        if (o instanceof JsonArray ja) return ja;
        var values = new ArrayList<JsonValue>();
        if (o instanceof Iterable<?> it) {
            for (var e : it) values.add(toJsonValue(e));
        } else if (o.getClass().isArray()) {
            int len = Array.getLength(o);
            for (int i = 0; i < len; i++) values.add(toJsonValue(Array.get(o, i)));
        } else throw new IllegalStateException("Unsupported array type: " + o.getClass());
        return new JsonArray(values);
    }

    private static JsonObject toJsonObject(Object o) {
        if (o instanceof JsonObject jo) return jo;
        var values = new LinkedHashMap<String, JsonValue>();
        if (o instanceof Map<?, ?> map) {
            for (var en : map.entrySet()) values.put(mapKeyToString(en.getKey()), toJsonValue(en.getValue()));
            return new JsonObject(values);
        }
        if (o instanceof Record r) {
            Class<?> rc = r.getClass();
            var components = rc.getRecordComponents();
            for (var c : components) {
                try {
                    values.put(c.getName(), toJsonValue(c.getAccessor().invoke(r)));
                } catch (Exception ex) {
                    throw new IllegalStateException("Failed to get record component: " + c.getName(), ex);
                }
            }
            return new JsonObject(values);
        }
        try {
            var beanInfo = Introspector.getBeanInfo(o.getClass());
            for (var pd : beanInfo.getPropertyDescriptors()) {
                if (Objects.equals(pd.getName(), "class")) continue;
                var read = pd.getReadMethod();
                if (read == null) continue;
                Object v = read.invoke(o);
                values.put(pd.getName(), toJsonValue(v));
            }
            return new JsonObject(values);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to convert object to JSON: " + o.getClass(), e);
        }
    }

    // ------------------------------------------------------------
    // JsonValue -> Object
    // ------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static <T> T fromJsonValue(JsonValue jv, java.lang.reflect.Type type) {
        Class<?> raw = raw(type);
        if (raw == Object.class) return (T) fromJsonUntyped(jv);
        if (JsonValue.class.isAssignableFrom(raw)) return (T) asJsonValue(jv, raw, type);
        // null
        if (jv instanceof JsonNull || isNullLikeType(raw)) return (T) nullFromJsonValue(jv, type); // careful!
        // boolean
        if (isBooleanLikeType(raw)) return (T) booleanFromJsonValue(jv, type);
        // number
        if (isNumberLikeType(raw)) return (T) numberFromJsonValue(jv, type);
        // string
        if (isStringLikeType(raw)) return (T) stringFromJsonValue(jv, type);
        // array
        if (raw.isArray()) return (T) arrayFromJson(expectJsonArray(jv, type), raw.getComponentType());
        if (Iterable.class.isAssignableFrom(raw)) return (T) collectionFromJson(expectJsonArray(jv, type), type, raw);
        // object
        return (T) objectFromJson(jv, type);
    }

    private static Object mapFromJson(JsonObject jo, java.lang.reflect.Type type, Class<?> raw) {
        var keyType = mapKeyType(type);
        var valueType = mapValueType(type);
        Map<Object, Object> map = createMap(raw);
        for (var en : jo.value().entrySet()) {
            Object key = mapKeyFromString(en.getKey(), keyType);
            Object value = fromJsonValue(en.getValue(), valueType);
            map.put(key, value);
        }
        return map;
    }

    private static Object mapKeyFromString(String key, java.lang.reflect.Type keyType) {
        if (keyType == null) return key;
        Class<?> raw = raw(keyType);
        if (raw == Object.class) return key;
        if (isStringLikeType(raw)) return stringFromJsonValue(toJsonString(key), keyType);
        if (isNullLikeType(raw)) return nullFromJsonValue(toJsonNull(key), keyType);
        if (isBooleanLikeType(raw)) return booleanFromJsonValue(toJsonBoolean(key), keyType);
        if (isNumberLikeType(raw)) return numberFromJsonValue(toJsonNumber(key), keyType);
        return key;
    }

    @SuppressWarnings("unchecked")
    private static Map<Object, Object> createMap(Class<?> raw) {
        if (raw == LinkedHashMap.class || raw == Map.class) return new LinkedHashMap<>();
        if (raw == HashMap.class) return new HashMap<>();
        if (raw == ConcurrentMap.class || raw == ConcurrentHashMap.class) return new ConcurrentHashMap<>();
        try {
            return (Map<Object, Object>) raw.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("Unsupported Map type: " + raw.getName(), e);
        }
    }

    private static Object recordFromJson(JsonObject jo, Class<?> raw) {
        try {
            var components = raw.getRecordComponents();
            Class<?>[] argTypes =
                    Arrays.stream(components).map(RecordComponent::getType).toArray(Class[]::new);
            Object[] args = new Object[components.length];
            for (int i = 0; i < components.length; i++) {
                var c = components[i];
                var jv = jo.value().get(c.getName());
                args[i] = (jv == null)
                        ? (c.getType().isPrimitive() ? defaultPrimitive(c.getType()) : null)
                        : fromJsonValue(jv, c.getGenericType());
            }
            var ctor = raw.getDeclaredConstructor(argTypes);
            makeAccessible(ctor, raw);
            return ctor.newInstance(args);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create record: " + raw.getName(), e);
        }
    }

    private static Object beanFromJson(JsonObject jo, Class<?> raw) {
        Object bean;
        var ctors = raw.getDeclaredConstructors();
        Constructor<?> noArg = null;
        Constructor<?> unique = null;
        for (var c : ctors)
            if (c.getParameterCount() == 0) {
                noArg = c;
                break;
            }
        if (noArg != null) {
            makeAccessible(noArg, raw);
            try {
                bean = noArg.newInstance();
            } catch (Exception e) {
                throw new IllegalStateException("Failed no-arg ctor: " + noArg, e);
            }
        } else {
            for (var c : ctors) {
                if (c.getParameterCount() == 0) continue;
                if (unique != null) throw new IllegalStateException("Multiple non no-arg ctors for " + raw.getName());
                unique = c;
            }
            if (unique == null) throw new IllegalStateException("No accessible constructor for " + raw.getName());
            makeAccessible(unique, raw);
            var params = unique.getParameters();
            Object[] args = new Object[params.length];
            for (int i = 0; i < params.length; i++) {
                var p = params[i];
                var jv = jo.value().get(p.getName());
                args[i] = (jv == null)
                        ? (p.getType().isPrimitive() ? defaultPrimitive(p.getType()) : null)
                        : fromJsonValue(jv, p.getParameterizedType());
            }
            try {
                bean = unique.newInstance(args);
            } catch (Exception e) {
                throw new IllegalStateException("Failed ctor: " + unique, e);
            }
        }
        try {
            var beanInfo = Introspector.getBeanInfo(raw);
            for (var pd : beanInfo.getPropertyDescriptors()) {
                if (Objects.equals(pd.getName(), "class")) continue;
                var write = pd.getWriteMethod();
                if (write == null) continue;
                var jv = jo.value().get(pd.getName());
                if (jv == null) continue;
                var v = fromJsonValue(jv, write.getGenericParameterTypes()[0]);
                write.invoke(bean, v);
            }
            return bean;
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to populate bean properties: " + raw.getName(), e);
        }
    }

    private static void makeAccessible(Constructor<?> c, Class<?> raw) {
        if (!Modifier.isPublic(c.getModifiers()) || !Modifier.isPublic(raw.getModifiers())) c.setAccessible(true);
    }

    private static Object arrayFromJson(JsonArray ja, Class<?> componentType) {
        int len = ja.value().size();
        Object arr = Array.newInstance(componentType, len);
        for (int i = 0; i < len; i++) {
            Object elem = fromJsonValue(ja.value().get(i), componentType);
            Array.set(arr, i, elem);
        }
        return arr;
    }

    private static Object collectionFromJson(JsonArray ja, java.lang.reflect.Type type, Class<?> raw) {
        var coll = createCollection(raw);
        var elementType = collectionElementType(type);
        for (var jv : ja.value()) coll.add(fromJsonValue(jv, elementType));
        return coll;
    }

    private static Object objectFromJson(JsonValue jv, java.lang.reflect.Type type) {
        var raw = raw(type);
        if (JsonValue.class.isAssignableFrom(raw)) {
            if (raw == JsonObject.class) return jv;
            else throw new IllegalStateException("Cannot convert JsonObject to " + type);
        }
        var jo = expectJsonObject(jv, type);
        if (Map.class.isAssignableFrom(raw)) return mapFromJson(jo, type, raw);
        if (raw.isRecord()) return recordFromJson(jo, raw);
        return beanFromJson(jo, raw);
    }

    private static JsonArray expectJsonArray(JsonValue jv, java.lang.reflect.Type type) {
        if (jv instanceof JsonArray ja) return ja;
        throw new IllegalStateException("Cannot convert " + jv.getClass() + " to " + type);
    }

    private static JsonObject expectJsonObject(JsonValue jv, java.lang.reflect.Type type) {
        if (jv instanceof JsonObject jo) return jo;
        throw new IllegalStateException("Cannot convert " + jv.getClass() + " to " + type);
    }

    private static Object asJsonValue(JsonValue jv, Class<?> raw, java.lang.reflect.Type type) {
        if (raw.isInstance(jv)) return jv;
        if (raw == JsonValue.class) return jv;
        throw new IllegalStateException("Cannot convert " + jv.getClass() + " to " + type);
    }

    private static Object fromJsonUntyped(JsonValue jv) {
        if (jv instanceof JsonNull) return null;
        if (jv instanceof JsonString js) return js.value();
        if (jv instanceof JsonNumber jn) return jn.value();
        if (jv instanceof JsonBoolean jb) return jb.value();
        if (jv instanceof JsonArray ja) {
            List<Object> list = new ArrayList<>(ja.value().size());
            for (var elem : ja.value()) list.add(fromJsonValue(elem, Object.class));
            return list;
        }
        if (jv instanceof JsonObject jo) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (var en : jo.value().entrySet()) map.put(en.getKey(), fromJsonValue(en.getValue(), Object.class));
            return map;
        }
        throw new IllegalStateException("Unknown JsonValue: " + jv.getClass());
    }

    private static Object nullFromJsonValue(JsonValue jv, java.lang.reflect.Type type) {
        Class<?> raw = raw(type);
        if (jv instanceof JsonString js) {
            String value = js.value();
            if (value.equalsIgnoreCase("null")) return nullFromJsonValue(new JsonNull(), type);
            else throw new IllegalStateException("Cannot convert string to null: " + value);
        }
        if (jv instanceof JsonNull jn) {
            if (JsonValue.class.isAssignableFrom(raw)) {
                if (raw == JsonNull.class) return jn;
                throw new IllegalStateException("Cannot convert JsonNull to " + type);
            }
            return null;
        }
        throw new IllegalStateException("Cannot convert " + jv.getClass() + " to " + type);
    }

    private static Object booleanFromJsonValue(JsonValue jv, java.lang.reflect.Type type) {
        Class<?> raw = raw(type);
        if (jv instanceof JsonString js) {
            String value = js.value();
            if (value.equalsIgnoreCase("true")) return booleanFromJsonValue(new JsonBoolean(true), type);
            if (value.equalsIgnoreCase("false")) return booleanFromJsonValue(new JsonBoolean(false), type);
            else throw new IllegalStateException("Cannot convert string to boolean: " + value);
        }
        if (jv instanceof JsonNumber jn) {
            int v = jn.value().intValue();
            if (v == 0) return booleanFromJsonValue(new JsonBoolean(false), type);
            if (v == 1) return booleanFromJsonValue(new JsonBoolean(true), type);
            throw new IllegalStateException("Cannot convert number to boolean: " + jn.value());
        }
        if (jv instanceof JsonBoolean jb) {
            if (JsonValue.class.isAssignableFrom(raw)) {
                if (raw == JsonBoolean.class) return jb;
                else throw new IllegalStateException("Cannot convert JsonBoolean to " + type);
            }
            if (raw == boolean.class || raw == Boolean.class || raw == Object.class) return jb.value();
            throw new IllegalStateException("Unsupported boolean target type: " + type);
        }
        throw new IllegalStateException("Cannot convert " + jv.getClass() + " to " + type);
    }

    private static Object numberFromJsonValue(JsonValue jv, java.lang.reflect.Type type) {
        Class<?> raw = raw(type);
        if (JsonValue.class.isAssignableFrom(raw)) {
            if (raw == JsonNumber.class) return jv;
            else throw new IllegalStateException("Cannot convert JsonNumber to " + type);
        }
        if (jv instanceof JsonString js) {
            try {
                return numberFromJsonValue(new JsonNumber(new BigDecimal(js.value())), type);
            } catch (NumberFormatException e) {
                throw new IllegalStateException("Cannot convert string to number: " + js.value(), e);
            }
        }
        if (jv instanceof JsonNumber jn) {
            Number n = jn.value();
            if (raw == Number.class || raw == Object.class) return n;
            if (raw == int.class || raw == Integer.class) return n.intValue();
            if (raw == long.class || raw == Long.class) return n.longValue();
            if (raw == double.class || raw == Double.class) return n.doubleValue();
            if (raw == float.class || raw == Float.class) return n.floatValue();
            if (raw == short.class || raw == Short.class) return n.shortValue();
            if (raw == byte.class || raw == Byte.class) return n.byteValue();
            if (raw == BigDecimal.class) return new BigDecimal(n.toString());
            if (raw == BigInteger.class) return new BigInteger(n.toString());
            throw new IllegalStateException("Unsupported number target type: " + type);
        }
        throw new IllegalStateException("Cannot convert " + jv.getClass() + " to " + type);
    }

    private static Object stringFromJsonValue(JsonValue jv, java.lang.reflect.Type type) {
        Class<?> raw = raw(type);
        if (JsonValue.class.isAssignableFrom(raw)) {
            if (raw == JsonString.class) return jv;
            else throw new IllegalStateException("Cannot convert JsonString to " + type);
        }
        if (raw == char.class || raw == Character.class) {
            var s = coerceToString(jv);
            if (s.length() != 1) throw new IllegalStateException("Cannot convert string to char: " + s);
            return s.charAt(0);
        }
        if (raw == String.class || raw == CharSequence.class) return coerceToString(jv);
        if (raw == StringBuilder.class) return new StringBuilder(coerceToString(jv));
        if (raw == StringBuffer.class) return new StringBuffer(coerceToString(jv));
        if (raw.isEnum()) return enumFromJsonValue(jv, raw);
        if (raw == Instant.class) return instantFromJsonValue(jv, type);
        if (raw == Date.class) return Date.from(instantFromJsonValue(jv, type));
        if (raw == Timestamp.class) return Timestamp.from(instantFromJsonValue(jv, type));
        if (raw == Duration.class) return durationFromJsonValue(jv, type);
        if (raw == LocalDateTime.class) return localDateTimeFromJsonValue(jv, type);
        if (raw == ZonedDateTime.class) return zonedDateTimeFromJsonValue(jv, type);
        if (raw == OffsetDateTime.class) return offsetDateTimeFromJsonValue(jv, type);
        if (raw == LocalDate.class) return LocalDate.parse(requireStringValue(jv, type));
        if (raw == LocalTime.class) return LocalTime.parse(requireStringValue(jv, type));
        throw new IllegalStateException("Unsupported temporal target type: " + type);
    }

    private static Object enumFromJsonValue(JsonValue jv, Class<?> raw) {
        if (jv instanceof JsonString js) return toEnum(raw, js.value());
        if (jv instanceof JsonNumber jn) {
            int ordinal = jn.value().intValue();
            var constants = raw.getEnumConstants();
            if (constants == null) throw new IllegalStateException("Not an enum type: " + raw.getName());
            if (ordinal < 0 || ordinal >= constants.length)
                throw new IllegalStateException("Enum ordinal out of range for " + raw.getName() + ": " + ordinal);
            return constants[ordinal];
        }
        throw new IllegalStateException("Cannot convert " + jv.getClass() + " to " + raw.getName());
    }

    private static Instant instantFromJsonValue(JsonValue jv, java.lang.reflect.Type type) {
        if (jv instanceof JsonString js) return Instant.parse(js.value());
        if (jv instanceof JsonNumber jn) return Instant.ofEpochMilli(numberToLong(jn.value()));
        throw new IllegalStateException("Cannot convert " + jv.getClass() + " to " + type);
    }

    private static Duration durationFromJsonValue(JsonValue jv, java.lang.reflect.Type type) {
        if (jv instanceof JsonString js) return Duration.parse(js.value());
        if (jv instanceof JsonNumber jn) return Duration.ofMillis(numberToLong(jn.value()));
        throw new IllegalStateException("Cannot convert " + jv.getClass() + " to " + type);
    }

    private static LocalDateTime localDateTimeFromJsonValue(JsonValue jv, java.lang.reflect.Type type) {
        if (jv instanceof JsonString js) return LocalDateTime.parse(js.value());
        if (jv instanceof JsonNumber jn)
            return instantFromJsonValue(jn, type).atZone(ZoneId.systemDefault()).toLocalDateTime();
        throw new IllegalStateException("Cannot convert " + jv.getClass() + " to " + type);
    }

    private static ZonedDateTime zonedDateTimeFromJsonValue(JsonValue jv, java.lang.reflect.Type type) {
        if (jv instanceof JsonString js) return ZonedDateTime.parse(js.value());
        if (jv instanceof JsonNumber jn) return instantFromJsonValue(jn, type).atZone(ZoneId.systemDefault());
        throw new IllegalStateException("Cannot convert " + jv.getClass() + " to " + type);
    }

    private static OffsetDateTime offsetDateTimeFromJsonValue(JsonValue jv, java.lang.reflect.Type type) {
        if (jv instanceof JsonString js) return OffsetDateTime.parse(js.value());
        if (jv instanceof JsonNumber jn)
            return zonedDateTimeFromJsonValue(jn, type).toOffsetDateTime();
        throw new IllegalStateException("Cannot convert " + jv.getClass() + " to " + type);
    }

    private static long numberToLong(Number number) {
        if (number instanceof BigDecimal bd) return bd.longValueExact();
        return number.longValue();
    }

    private static String requireStringValue(JsonValue jv, java.lang.reflect.Type type) {
        if (jv instanceof JsonString js) return js.value();
        throw new IllegalStateException("Cannot convert " + jv.getClass() + " to " + type);
    }

    private static String coerceToString(JsonValue jv) {
        return jv instanceof JsonString js ? js.value() /* no quote */ : jv.toString();
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

    private static Enum<?> toEnum(Class<?> enumType, String value) {
        Object[] constants = enumType.getEnumConstants();
        if (constants == null) throw new IllegalStateException(enumType + " is not an enum type");
        for (var c : constants) {
            if (((Enum<?>) c).name().equalsIgnoreCase(value)) return (Enum<?>) c;
        }
        throw new IllegalStateException(enumType + " has no enum constant " + value);
    }

    private static Class<?> raw(java.lang.reflect.Type type) {
        if (type instanceof Class<?> c) return c;
        if (type instanceof ParameterizedType p) return (Class<?>) p.getRawType();
        throw new IllegalStateException("Unsupported type: " + type);
    }

    private static java.lang.reflect.Type mapKeyType(java.lang.reflect.Type type) {
        if (type instanceof ParameterizedType p) return p.getActualTypeArguments()[0];
        return Object.class;
    }

    private static java.lang.reflect.Type mapValueType(java.lang.reflect.Type type) {
        if (type instanceof ParameterizedType p) return p.getActualTypeArguments()[1];
        return Object.class;
    }

    private static java.lang.reflect.Type collectionElementType(java.lang.reflect.Type type) {
        if (type instanceof ParameterizedType p) return p.getActualTypeArguments()[0];
        if (type instanceof GenericArrayType ga) return ga.getGenericComponentType();
        if (type instanceof Class<?> c && c.isArray()) return c.getComponentType();
        return Object.class;
    }

    private static Object defaultPrimitive(Class<?> t) {
        if (t == boolean.class) return false;
        if (t == byte.class) return (byte) 0;
        if (t == short.class) return (short) 0;
        if (t == int.class) return 0;
        if (t == long.class) return 0L;
        if (t == float.class) return 0f;
        if (t == double.class) return 0d;
        if (t == char.class) return '\0';
        throw new IllegalStateException("Unsupported primitive: " + t);
    }
}
