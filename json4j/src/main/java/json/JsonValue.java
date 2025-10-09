package json;

import lombok.SneakyThrows;
import org.jspecify.annotations.Nullable;

import java.beans.Introspector;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 *
 *
 * @author Freeman
 * @since 2025/10/9
 */
public sealed interface JsonValue permits JsonArray, JsonBoolean, JsonNull, JsonNumber, JsonObject, JsonString {
    String stringify();

    @SneakyThrows
    static JsonValue fromJavaObject(@Nullable Object o) {
        return switch (o) {
            case JsonValue jsonValue -> jsonValue;
            // json null
            case null -> new JsonNull();
            // json number
            case Number number -> new JsonNumber(number);
            // json string
            case String string -> new JsonString(string);
            case Date string -> new JsonString("");
            case LocalDateTime string -> new JsonString("");
            case LocalDate string -> new JsonString(string.toString());
            // json boolean
            case Boolean bool -> new JsonBoolean(bool);
            // json array
            case Object[] array -> {
                var values = new ArrayList<JsonValue>();
                for (var e : array) {
                    values.add(fromJavaObject(e));
                }
                yield new JsonArray(values);
            }
            case Iterable<?> array -> {
                var values = new ArrayList<JsonValue>();
                for (var e : array) {
                    values.add(fromJavaObject(e));
                }
                yield new JsonArray(values);
            }
            // json object
            case Map<?, ?> object -> {
                var values = new LinkedHashMap<String, JsonValue>();
                for (var en : object.entrySet()) {
                    values.put(en.getKey().toString(), fromJavaObject(en.getValue()));
                }
                yield new JsonObject(values);
            }
            case Record object -> {
                var values = new LinkedHashMap<String, JsonValue>();
                for (var e : object.getClass().getRecordComponents()) {
                    var v = e.getAccessor().invoke(object);
                    values.put(e.getName(), fromJavaObject(v));
                }
                yield new JsonObject(values);
            }
            default -> {
                var values = new LinkedHashMap<String, JsonValue>();
                var beanInfo = Introspector.getBeanInfo(o.getClass());
                for (var property : beanInfo.getPropertyDescriptors()) {
                    if (Objects.equals(property.getName(), "class")) continue;
                    var readMethod = property.getReadMethod();
                    if (readMethod == null) continue;
                    var v = readMethod.invoke(o);
                    values.put(property.getName(), fromJavaObject(v));
                }
                yield new JsonObject(values);
            }
        };
    }
}
