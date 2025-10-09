package json;

import lombok.Data;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 *
 *
 * @author Freeman
 * @since 2025/10/9
 */
class JsonValueTest {

    @Nested
    class FromJavaObject {

        @Data
        static class ClassUser {
            private final String name;
            private final Integer age;
        }

        record RecordUser(String name, Integer age) {}

        @Test
        void test() {
            System.out.println(JsonValue.fromJavaObject(new ClassUser("Freeman", 25)).stringify());
            System.out.println(JsonValue.fromJavaObject(new RecordUser("Freeman", 25)).stringify());
        }
    }
}
