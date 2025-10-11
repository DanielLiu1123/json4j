package json;

import java.beans.Introspector;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;

public final class Json {

    /**
     * Convert any java object to JSON string
     *
     * @param o any java object
     * @return JSON string
     */
    public static String stringify(@Nullable Object o) {
        return toJsonValue(o).toString();
    }

    /**
     * Parse JSON string to object.
     *
     * @param json JSON string
     * @param type Type reference
     * @param <T>  Target type
     * @return Parsed object
     */
    @Nullable
    public static <T> T parse(String json, Type<T> type) {
        var jsonValue = toJsonValue(json);
        return fromJsonValue(jsonValue, type.getType());
    }

    /**
     * Parse JSON string to object.
     *
     * @param json JSON string
     * @param type Target class
     * @param <T>  Target type
     * @return Parsed object
     */
    @Nullable
    public static <T> T parse(String json, Class<T> type) {
        return parse(json, Type.of(type));
    }

    private static JsonValue toJsonValue(String json) {
        var parser = new Parser(new Lexer(json));
        return parser.parse();
    }

    public static JsonValue toJsonValue(@Nullable Object o) {
        if (o instanceof JsonValue jsonValue) {
            return jsonValue;
        }
        if (isJsonNull(o)) {
            return toJsonNull();
        }
        if (isJsonNumber(o)) {
            return toJsonNumber(o);
        }
        if (isJsonString(o)) {
            return toJsonString(o);
        }
        if (isJsonBoolean(o)) {
            return toJsonBoolean(o);
        }
        if (isJsonArray(o)) {
            return toJsonArray(o);
        }
        return toJsonObject(o);
    }

    private static JsonNull toJsonNull() {
        return new JsonNull();
    }

    private static JsonNumber toJsonNumber(Object o) {
        return new JsonNumber((Number) o);
    }

    private static JsonString toJsonString(Object o) {
        if (o instanceof CharSequence s) {
            return new JsonString(s.toString());
        } else if (o instanceof Character c) {
            return new JsonString(c.toString());
        } else if (o instanceof Enum<?> e) {
            return new JsonString(e.name());
        } else if (o instanceof Date d) {
            return new JsonString(d.toInstant().toString());
        } else if (o instanceof LocalDate ld) {
            return new JsonString(ld.toString());
        } else if (o instanceof LocalTime lt) {
            return new JsonString(lt.toString());
        } else if (o instanceof LocalDateTime ldt) {
            return new JsonString(ldt.atZone(ZoneId.systemDefault()).toInstant().toString());
        } else if (o instanceof Instant i) {
            return new JsonString(i.toString());
        } else if (o instanceof Duration du) {
            return new JsonString(du.toString());
        } else {
            throw new IllegalStateException("Unsupported string type: " + o.getClass());
        }
    }

    private static JsonBoolean toJsonBoolean(Object o) {
        return new JsonBoolean((Boolean) o);
    }

    private static JsonObject toJsonObject(Object o) {
        // Json object
        var values = new LinkedHashMap<String, JsonValue>();
        if (o instanceof Map<?, ?> map) {
            for (var en : map.entrySet()) {
                values.put(en.getKey().toString(), toJsonValue(en.getValue()));
            }
        } else if (o instanceof Record record) {
            for (var e : record.getClass().getRecordComponents()) {
                try {
                    var v = e.getAccessor().invoke(record);
                    values.put(e.getName(), toJsonValue(v));
                } catch (Exception ex) {
                    throw new IllegalStateException("Failed to get record component value: " + e.getName(), ex);
                }
            }
        } else {
            try {
                var beanInfo = Introspector.getBeanInfo(o.getClass());
                for (var property : beanInfo.getPropertyDescriptors()) {
                    if (Objects.equals(property.getName(), "class")) continue;
                    var readMethod = property.getReadMethod();
                    if (readMethod == null) continue;
                    var v = readMethod.invoke(o);
                    values.put(property.getName(), toJsonValue(v));
                }
            } catch (Exception e) {
                throw new IllegalStateException("Failed to convert object to JSON: " + o.getClass(), e);
            }
        }
        return new JsonObject(values);
    }

    private static JsonArray toJsonArray(Object o) {
        var values = new ArrayList<JsonValue>();
        if (o instanceof Iterable<?> it) {
            for (var e : it) {
                values.add(toJsonValue(e));
            }
        } else if (o.getClass().isArray()) {
            var array = (Object[]) o;
            for (var e : array) {
                values.add(toJsonValue(e));
            }
        } else {
            throw new IllegalStateException("Unsupported array type: " + o.getClass());
        }
        return new JsonArray(values);
    }

    private static boolean isJsonArray(Object obj) {
        return obj instanceof Iterable<?> || obj.getClass().isArray();
    }

    private static boolean isJsonNull(@Nullable Object obj) {
        return obj == null;
    }

    private static boolean isJsonString(Object obj) {
        return obj instanceof CharSequence
                || obj instanceof Character
                || obj instanceof Enum<?>
                || obj instanceof Date
                || obj instanceof LocalDate
                || obj instanceof LocalTime
                || obj instanceof LocalDateTime
                || obj instanceof Instant
                || obj instanceof Duration;
    }

    private static boolean isJsonNumber(Object obj) {
        return obj instanceof Number;
    }

    private static boolean isJsonBoolean(Object obj) {
        return obj instanceof Boolean;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    private static <T> T fromJsonValue(JsonValue jsonValue, java.lang.reflect.Type type) {
        if (jsonValue instanceof JsonNull jsonNull) {
            return (T) fromJsonNull(jsonNull, type);
        }
        if (jsonValue instanceof JsonObject jsonObject) {
            return (T) fromJsonObject(jsonObject, type);
        }
        if (jsonValue instanceof JsonArray jsonArray) {
            return (T) fromJsonArray(jsonArray, type);
        }
        if (jsonValue instanceof JsonString jsonString) {
            return (T) fromJsonString(jsonString, type);
        }
        if (jsonValue instanceof JsonNumber jsonNumber) {
            return (T) fromJsonNumber(jsonNumber, type);
        }
        if (jsonValue instanceof JsonBoolean jsonBoolean) {
            return (T) fromJsonBoolean(jsonBoolean, type);
        }
        throw new IllegalStateException("Unknown JsonValue type: " + jsonValue.getClass());
    }

    @Nullable
    private static Object fromJsonNull(JsonNull jsonNull, java.lang.reflect.Type type) {
        Class<?> rawType = getRawType(type);
        if (JsonValue.class.isAssignableFrom(rawType)) {
            if (rawType == JsonNull.class) {
                return jsonNull;
            } else {
                throw new IllegalStateException("Cannot convert JsonNull to " + type);
            }
        }
        return null;
    }

    private static Object fromJsonObject(JsonObject jsonObject, java.lang.reflect.Type type) {
        Class<?> rawType = getRawType(type);
        if (JsonValue.class.isAssignableFrom(rawType)) {
            if (rawType == JsonObject.class) {
                return jsonObject;
            } else {
                throw new IllegalStateException("Cannot convert JsonObject to " + type);
            }
        }
        if (rawType == Map.class || rawType == LinkedHashMap.class) {
            var map = new LinkedHashMap<String, @Nullable Object>();
            var valueType = getMapValueType(type);
            for (var en : jsonObject.value.entrySet()) {
                // TODO(Freeman): handle key type, like Enum, Integer, LocalDate, etc.
                map.put(en.getKey(), fromJsonValue(en.getValue(), valueType));
            }
            return map;
        }
        if (rawType == HashMap.class) {
            var map = new HashMap<String, @Nullable Object>();
            var valueType = getMapValueType(type);
            for (var en : jsonObject.value.entrySet()) {
                // TODO(Freeman): handle key type, like Enum, Integer, LocalDate, etc.
                map.put(en.getKey(), fromJsonValue(en.getValue(), valueType));
            }
            return map;
        }
        if (rawType == ConcurrentMap.class || rawType == ConcurrentHashMap.class) {
            var map = new ConcurrentHashMap<String, Object>();
            var valueType = getMapValueType(type);
            for (var en : jsonObject.value.entrySet()) {
                // TODO(Freeman): handle key type, like Enum, Integer, LocalDate, etc.
                var e = fromJsonValue(en.getValue(), valueType);
                if (e == null) throw new IllegalStateException(type + " does not support null values");
                map.put(en.getKey(), e);
            }
            return map;
        }
        if (rawType.isRecord()) {
            var recordComponents = rawType.getRecordComponents();
            var args = new Object[recordComponents.length];
            var argTypes = new Class<?>[recordComponents.length];
            for (int i = 0; i < recordComponents.length; i++) {
                var component = recordComponents[i];
                argTypes[i] = component.getType();
                var jsonValue = jsonObject.value.get(component.getName());
                args[i] = fromJsonValue(jsonValue, component.getGenericType());
            }
            try {
                Constructor<?> c = rawType.getDeclaredConstructor(argTypes);
                makeAccessible(c, rawType);
                return c.newInstance(args);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to create record instance: " + rawType.getName(), e);
            }
        }
        Object bean;
        var constructors = rawType.getDeclaredConstructors();
        Constructor<?> noArgConstructor = null;
        Constructor<?> candidateConstructor = null;
        for (var constructor : constructors) {
            if (constructor.getParameterCount() == 0) {
                noArgConstructor = constructor;
                break;
            }
        }
        if (noArgConstructor != null) {
            makeAccessible(noArgConstructor, rawType);
            try {
                bean = noArgConstructor.newInstance();
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to create instance using no-arg constructor: " + noArgConstructor, e);
            }
        } else {
            for (var constructor : constructors) {
                if (constructor.getParameterCount() == 0) continue;
                if (candidateConstructor != null) {
                    throw new IllegalStateException("Multiple non no-arg constructors found for " + rawType.getName());
                }
                candidateConstructor = constructor;
            }
            if (candidateConstructor == null) {
                throw new IllegalStateException("No accessible constructor found for " + rawType.getName());
            }
            makeAccessible(candidateConstructor, rawType);
            var parameters = candidateConstructor.getParameters();
            var args = new Object[parameters.length];
            for (int i = 0; i < parameters.length; i++) {
                var parameter = parameters[i];
                var jsonValue = jsonObject.value.get(parameter.getName());
                if (jsonValue == null) {
                    args[i] = parameter.getType().isPrimitive() ? defaultValueForPrimitive(parameter.getType()) : null;
                } else {
                    args[i] = fromJsonValue(jsonValue, parameter.getParameterizedType());
                }
            }
            try {
                bean = candidateConstructor.newInstance(args);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to create instance using constructor: " + candidateConstructor, e);
            }
        }
        try {
            var beanInfo = Introspector.getBeanInfo(rawType);
            for (var property : beanInfo.getPropertyDescriptors()) {
                if (Objects.equals(property.getName(), "class")) continue;
                var writeMethod = property.getWriteMethod();
                if (writeMethod == null) continue;
                var jsonValue = jsonObject.value.get(property.getName());
                if (jsonValue == null) continue;
                var value = fromJsonValue(jsonValue, writeMethod.getGenericParameterTypes()[0]);
                writeMethod.invoke(bean, value);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to populate bean properties: " + rawType.getName(), e);
        }
        return bean;
    }

    private static void makeAccessible(Constructor<?> c, Class<?> rawType) {
        if (!Modifier.isPublic(c.getModifiers()) || !Modifier.isPublic(rawType.getModifiers())) {
            c.setAccessible(true);
        }
    }

    private static Object fromJsonArray(JsonArray jsonArray, java.lang.reflect.Type type) {
        var rawType = getRawType(type);
        if (JsonValue.class.isAssignableFrom(rawType)) {
            if (rawType == JsonArray.class) {
                return jsonArray;
            } else {
                throw new IllegalStateException("Cannot convert JsonArray to " + type);
            }
        }
        if (rawType == List.class
                || rawType == ArrayList.class
                || rawType == Collection.class
                || rawType == Iterable.class) {
            var list = new ArrayList<>();
            var elementType = getCollectionElementType(type);
            for (var jsonValue : jsonArray.value()) {
                list.add(fromJsonValue(jsonValue, elementType));
            }
            return list;
        }
        if (rawType == LinkedList.class) {
            var list = new LinkedList<>();
            var elementType = getCollectionElementType(type);
            for (var jsonValue : jsonArray.value()) {
                list.add(fromJsonValue(jsonValue, elementType));
            }
            return list;
        }
        if (rawType == Set.class || rawType == LinkedHashSet.class) {
            var set = new LinkedHashSet<>();
            var elementType = getCollectionElementType(type);
            for (var jsonValue : jsonArray.value()) {
                set.add(fromJsonValue(jsonValue, elementType));
            }
            return set;
        }
        if (rawType == HashSet.class) {
            var set = new HashSet<>();
            var elementType = getCollectionElementType(type);
            for (var jsonValue : jsonArray.value()) {
                set.add(fromJsonValue(jsonValue, elementType));
            }
            return set;
        }
        if (rawType == Queue.class || rawType == ArrayBlockingQueue.class) {
            var queue = new ArrayBlockingQueue<>(jsonArray.value().size());
            var elementType = getCollectionElementType(type);
            for (var jsonValue : jsonArray.value()) {
                var e = fromJsonValue(jsonValue, elementType);
                if (e == null) throw new IllegalStateException("Queue does not support null elements");
                queue.add(e);
            }
            return queue;
        }
        if (rawType == Deque.class || rawType == ArrayDeque.class) {
            var deque = new ArrayDeque<>(jsonArray.value().size());
            var elementType = getCollectionElementType(type);
            for (var jsonValue : jsonArray.value()) {
                var e = fromJsonValue(jsonValue, elementType);
                if (e == null) throw new IllegalStateException("Deque does not support null elements");
                deque.add(e);
            }
            return deque;
        }
        if (rawType.isArray()) {
            var array = (Object[]) Array.newInstance(
                    rawType.getComponentType(), jsonArray.value().size());
            var elementType = rawType.getComponentType();
            for (int i = 0; i < jsonArray.value().size(); i++) {
                array[i] = fromJsonValue(jsonArray.value().get(i), elementType);
            }
            return array;
        }
        throw new IllegalStateException("Unsupported array type: " + type);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object fromJsonString(JsonString jsonString, java.lang.reflect.Type type) {
        var rawType = getRawType(type);
        if (JsonValue.class.isAssignableFrom(rawType)) {
            if (rawType == JsonString.class) {
                return jsonString;
            } else {
                throw new IllegalStateException("Cannot convert JsonString to " + type);
            }
        }
        if (rawType == String.class || rawType == CharSequence.class || rawType == Object.class) {
            return jsonString.value;
        }
        if (rawType == StringBuilder.class) {
            return new StringBuilder(jsonString.value);
        }
        if (rawType == StringBuffer.class) {
            return new StringBuffer(jsonString.value);
        }
        if (rawType == char.class || rawType == Character.class) {
            if (jsonString.value.length() != 1) {
                throw new IllegalStateException("Cannot convert string to char: " + jsonString.value);
            }
            return jsonString.value.charAt(0);
        }
        if (rawType.isEnum()) {
            return Enum.valueOf((Class<Enum>) rawType, jsonString.value);
        }
        if (rawType == Date.class) {
            return Date.from(Instant.parse(jsonString.value));
        }
        if (rawType == LocalDate.class) {
            return LocalDate.parse(jsonString.value);
        }
        if (rawType == LocalTime.class) {
            return LocalTime.parse(jsonString.value);
        }
        if (rawType == LocalDateTime.class) {
            return LocalDateTime.ofInstant(Instant.parse(jsonString.value), ZoneId.systemDefault());
        }
        if (rawType == Instant.class) {
            return Instant.parse(jsonString.value);
        }
        if (rawType == Duration.class) {
            return Duration.parse(jsonString.value);
        }
        throw new IllegalStateException("Unsupported string type: " + type);
    }

    private static Object fromJsonNumber(JsonNumber jsonNumber, java.lang.reflect.Type type) {
        var rawType = getRawType(type);
        if (JsonValue.class.isAssignableFrom(rawType)) {
            if (rawType == JsonNumber.class) {
                return jsonNumber;
            } else {
                throw new IllegalStateException("Cannot convert JsonNumber to " + type);
            }
        }
        if (rawType == Number.class || rawType == Object.class) {
            return jsonNumber.value;
        }
        if (rawType == int.class || rawType == Integer.class) {
            return jsonNumber.value.intValue();
        }
        if (rawType == long.class || rawType == Long.class) {
            return jsonNumber.value.longValue();
        }
        if (rawType == double.class || rawType == Double.class) {
            return jsonNumber.value.doubleValue();
        }
        if (rawType == float.class || rawType == Float.class) {
            return jsonNumber.value.floatValue();
        }
        if (rawType == short.class || rawType == Short.class) {
            return jsonNumber.value.shortValue();
        }
        if (rawType == byte.class || rawType == Byte.class) {
            return jsonNumber.value.byteValue();
        }
        if (rawType == BigDecimal.class) {
            return new BigDecimal(jsonNumber.value.toString());
        }
        if (rawType == BigInteger.class) {
            return new BigInteger(jsonNumber.value.toString());
        }
        throw new IllegalStateException("Unsupported number type: " + type);
    }

    private static Object fromJsonBoolean(JsonBoolean jsonBoolean, java.lang.reflect.Type type) {
        var rawType = getRawType(type);
        if (JsonValue.class.isAssignableFrom(rawType)) {
            if (rawType == JsonBoolean.class) {
                return jsonBoolean;
            } else {
                throw new IllegalStateException("Cannot convert JsonBoolean to " + type);
            }
        }
        if (rawType == boolean.class || rawType == Boolean.class || rawType == Object.class) {
            return jsonBoolean.value();
        }
        throw new IllegalStateException("Unsupported boolean type: " + type);
    }

    private static Class<?> getRawType(java.lang.reflect.Type type) {
        if (type instanceof Class<?> clazz) {
            return clazz;
        }
        if (type instanceof ParameterizedType parameterizedType) {
            return (Class<?>) parameterizedType.getRawType();
        }
        throw new IllegalStateException("Unsupported type: " + type);
    }

    private static java.lang.reflect.Type getMapValueType(java.lang.reflect.Type type) {
        if (type instanceof ParameterizedType parameterizedType) {
            return parameterizedType.getActualTypeArguments()[1];
        }
        return Object.class;
    }

    private static java.lang.reflect.Type getCollectionElementType(java.lang.reflect.Type type) {
        if (type instanceof ParameterizedType parameterizedType) {
            return parameterizedType.getActualTypeArguments()[0];
        }
        return Object.class;
    }

    private static Object defaultValueForPrimitive(Class<?> type) {
        if (type == boolean.class) return false;
        if (type == byte.class) return (byte) 0;
        if (type == short.class) return (short) 0;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == float.class) return 0f;
        if (type == double.class) return 0d;
        if (type == char.class) return '\0';
        throw new IllegalStateException("Unsupported primitive type: " + type);
    }

    public enum Token {
        LBRACE, // {
        RBRACE, // }
        LBRACKET, // [
        RBRACKET, // ]
        COLON, // :
        COMMA, // ,
        STRING, // "..."
        NUMBER, // -?(0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?
        TRUE, // true
        FALSE, // false
        NULL, // null
        EOF
    }

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
                    .map(e -> "\"%s\": %s".formatted(e.getKey(), e.getValue().toString()))
                    .collect(Collectors.joining(", ", "{", "}"));
        }
    }

    public record JsonString(String value) implements JsonValue {

        @Override
        public String toString() {
            return "\"%s\"".formatted(value);
        }
    }

    public abstract static class Type<T> {

        private final java.lang.reflect.Type type;

        protected Type() {
            var typeSubclass = findTypeSubclass(getClass());
            var parameterizedType = (ParameterizedType) typeSubclass.getGenericSuperclass();
            var actualTypeArguments = parameterizedType.getActualTypeArguments();
            assert actualTypeArguments.length == 1;
            this.type = actualTypeArguments[0];
        }

        private Type(java.lang.reflect.Type type) {
            this.type = type;
        }

        public static <T> Type<T> of(Class<T> clazz) {
            return new Type<>(clazz) {};
        }

        private static Class<?> findTypeSubclass(Class<?> child) {
            Class<?> parent = child.getSuperclass();
            if (Object.class == parent) {
                throw new IllegalStateException("Expected Json.Type superclass");
            } else if (Type.class == parent) {
                return child;
            } else {
                return findTypeSubclass(parent);
            }
        }

        public java.lang.reflect.Type getType() {
            return type;
        }

        @Override
        public boolean equals(Object object) {
            if (object == null || getClass() != object.getClass()) return false;
            Type<?> type1 = (Type<?>) object;
            return Objects.equals(type, type1.type);
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

    public static final class Lexer {
        private final String s;
        private int i = 0;
        private int line = 1;
        private int col = 1;

        private Token current;
        private String stringValue; // for STRING
        private String numberLexeme; // for NUMBER

        public Lexer(String s) {
            this.s = Objects.requireNonNull(s, "json must not be null");
            advance(); // load first token
        }

        private static int hexVal(char c) {
            if (c >= '0' && c <= '9') return c - '0';
            if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
            if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
            return -1;
        }

        private static String hex4(int x) {
            String s = Integer.toHexString(x);
            return "0000".substring(Math.min(4, s.length())) + s;
        }

        private static boolean isDigit(char c) {
            return c >= '0' && c <= '9';
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
            skipWhitespace();
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

        private void skipWhitespace() {
            while (!eof()) {
                char c = peek();
                if (c == ' ' || c == '\t' || c == '\r') {
                    consume();
                } else if (c == '\n') {
                    consume();
                    line++;
                    col = 1;
                } else {
                    break;
                }
            }
        }

        private String readString() {
            // current char is '"'
            consume(); // skip opening quote
            StringBuilder sb = new StringBuilder();
            while (!eof()) {
                char c = consume();
                if (c == '"') {
                    return sb.toString();
                } else if (c == '\\') {
                    if (eof()) error("Unterminated escape");
                    char e = consume();
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
                        case 'f':
                            sb.append('\f');
                            break;
                        case 'n':
                            sb.append('\n');
                            break;
                        case 'r':
                            sb.append('\r');
                            break;
                        case 't':
                            sb.append('\t');
                            break;
                        case 'u': {
                            int cp = readHex4();
                            // Handle surrogate pairs for proper Unicode
                            if (Character.isHighSurrogate((char) cp)) {
                                // expect "\\uXXXX" for low surrogate
                                if (peek() == '\\' && peekNext() == 'u') {
                                    consume(); // '\'
                                    consume(); // 'u'
                                    int low = readHex4();
                                    if (!Character.isLowSurrogate((char) low)) {
                                        error("Invalid low surrogate: \\u" + hex4(low));
                                    }
                                    int codePoint = Character.toCodePoint((char) cp, (char) low);
                                    sb.appendCodePoint(codePoint);
                                } else {
                                    // stand-alone high surrogate is invalid in JSON strings
                                    error("High surrogate not followed by low surrogate");
                                }
                            } else if (Character.isLowSurrogate((char) cp)) {
                                error("Unexpected low surrogate without preceding high surrogate");
                            } else {
                                sb.append((char) cp);
                            }
                            break;
                        }
                        default:
                            error("Invalid escape: \\" + e);
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

        private void readKeyword(String kw) {
            for (int k = 0; k < kw.length(); k++) {
                if (eof() || peek() != kw.charAt(k)) {
                    error("Invalid literal, expected '" + kw + "'");
                }
                consume();
            }
        }

        private String readNumber() {
            int start = i;
            // sign
            if (peek() == '-') consume();
            // int part
            if (eof()) error("Unexpected end in number");
            if (peek() == '0') {
                consume();
            } else if (isDigit(peek())) {
                while (!eof() && isDigit(peek())) consume();
            } else {
                error("Invalid number (int part)");
            }
            // frac
            if (!eof() && peek() == '.') {
                consume();
                if (eof() || !isDigit(peek())) error("Invalid number (frac part)");
                while (!eof() && isDigit(peek())) consume();
            }
            // exp
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
            if (lexer.current() != Token.EOF) {
                error("Trailing characters after top-level value");
            }
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
            if (lexer.current() != t) {
                error("Expected " + t + " but found " + lexer.current());
            }
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

            if (accept(Token.RBRACE)) {
                return new JsonObject(map);
            }

            while (true) {
                if (lexer.current() != Token.STRING) {
                    error("Expected string as object key");
                }
                String key = lexer.string();
                lexer.advance();

                expect(Token.COLON);

                JsonValue val = parseValue();
                map.put(key, val);

                if (accept(Token.COMMA)) {
                    // continue with next member
                    continue;
                } else if (accept(Token.RBRACE)) {
                    break;
                } else {
                    error("Expected ',' or '}' in object");
                }
            }
            return new JsonObject(map);
        }

        private JsonArray parseArray() {
            expect(Token.LBRACKET);
            List<JsonValue> list = new ArrayList<>();

            if (accept(Token.RBRACKET)) {
                return new JsonArray(list);
            }

            while (true) {
                list.add(parseValue());

                if (accept(Token.COMMA)) {
                    // continue with next element
                    continue;
                } else if (accept(Token.RBRACKET)) {
                    break;
                } else {
                    error("Expected ',' or ']' in array");
                }
            }
            return new JsonArray(list);
        }

        private void error(String msg) {
            throw new IllegalStateException(
                    msg + " at line " + lexer.line() + ", col " + lexer.col() + " (token " + lexer.current() + ")");
        }
    }
}
