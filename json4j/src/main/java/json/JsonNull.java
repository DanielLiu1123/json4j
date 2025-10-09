package json;

/**
 *
 *
 * @author Freeman
 * @since 2025/10/9
 */
public record JsonNull() implements JsonValue {
    @Override
    public String stringify() {
        return "null";
    }
}
