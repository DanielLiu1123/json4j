package json;

/**
 *
 *
 * @author Freeman
 * @since 2025/10/9
 */
public record JsonString(String value) implements JsonValue {

    @Override
    public String stringify() {
        return "\"%s\"".formatted(value);
    }
}
