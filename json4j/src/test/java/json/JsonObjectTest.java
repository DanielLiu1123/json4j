package json;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 *
 *
 * @author Freeman
 * @since 2025/10/9
 */
class JsonObjectTest {

    @Test
    void ok() {
        Json.JsonObject jsonObject = new Json.JsonObject(Map.of(
                "null", new Json.JsonNull(),
                "number", new Json.JsonNumber(100),
                "string", new Json.JsonString("OK"),
                "object", new Json.JsonObject(Map.of()),
                "boolean", new Json.JsonBoolean(true),
                "array",
                        new Json.JsonArray(
                                List.of(new Json.JsonNumber(1), new Json.JsonNull(), new Json.JsonString("Cool")))));

        System.out.println(jsonObject.toString());
    }
}
