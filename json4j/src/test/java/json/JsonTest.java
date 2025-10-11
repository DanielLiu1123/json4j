package json;

import java.lang.reflect.Type;
import lombok.Data;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 *
 *
 * @author Freeman
 * @since 2025/10/9
 */
class JsonTest {

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
            System.out.println(Json.stringify(new ClassUser("Freeman", 25)));
            System.out.println(Json.stringify(new RecordUser("Freeman", 25)));

            var json = "{\"name\":\"Freeman\",\"age\":25}";
            System.out.println(Json.parse(json, new Json.Type<ClassUser>() {
                @Override
                public Type getType() {
                    return ClassUser.class;
                }
            }));
            System.out.println(Json.parse(json, RecordUser.class));
        }
    }
}
