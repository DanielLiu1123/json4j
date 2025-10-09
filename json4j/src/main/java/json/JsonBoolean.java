package json;

/**
 *
 *
 * @author Freeman
 * @since 2025/10/9
 */
public record JsonBoolean(boolean value) implements JsonValue {
    @Override
    public String stringify() {
        return value ? "true" : "false";
    }
}
