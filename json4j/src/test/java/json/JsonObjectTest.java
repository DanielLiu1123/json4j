package json;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 *
 *
 * @author Freeman
 * @since 2025/10/9
 */
class JsonObjectTest {

    @Test
    void ok(){
        JsonObject jsonObject = new JsonObject(Map.of(
                "null", new JsonNull(),
                "number", new JsonNumber(100),
                "string", new JsonString("OK"),
                "object", new JsonObject(Map.of()),
                "boolean", new JsonBoolean(true),
                "array", new JsonArray(List.of(new JsonNumber(1), new JsonNull(), new JsonString("Cool")))
        ));

        System.out.println(jsonObject.stringify());
    }
}
