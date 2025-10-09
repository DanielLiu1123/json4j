package json;

import java.util.List;
import java.util.stream.Collectors;

/**
 *
 *
 * @author Freeman
 * @since 2025/10/9
 */
public record JsonArray(List<JsonValue> value) implements JsonValue {
    @Override
    public String stringify() {
        return value.stream()
                .map(JsonValue::stringify)
                .collect(Collectors.joining(", ", "[", "]"));
    }
}
