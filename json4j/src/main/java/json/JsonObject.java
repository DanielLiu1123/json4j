package json;

import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 *
 * @author Freeman
 * @since 2025/10/9
 */
public record JsonObject(Map<String, JsonValue> value) implements JsonValue {
    @Override
    public String stringify() {
        return value.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> "\"%s\": %s".formatted(e.getKey(), e.getValue().stringify()))
                .collect(Collectors.joining(", ", "{", "}"));
    }
}
