package json;

public record JsonNumber(Number value) implements JsonValue {
    @Override
    public String stringify() {
        return value.toString();
    }
}
