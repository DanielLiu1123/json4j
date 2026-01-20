package issues.issue15;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import cr.Classpath;
import java.util.Map;
import json.Json;
import org.junit.jupiter.api.Test;

/**
 * Verify this library works correctly when Protobuf is not present in the classpath.
 *
 * @see <a href="https://github.com/danielliu1123/json4j/issues/15">Issue 15</a>
 */
class NoProtobufTest {

    @Test
    void protobufInClasspath_shouldWork() {
        assertThat(Json.parse("{\"name\":\"Alice\"}", Object.class)).isEqualTo(Map.of("name", "Alice"));
        assertThat(Json.stringify(Map.of("name", "Alice"))).isEqualTo("{\"name\":\"Alice\"}");
    }

    @Test
    @Classpath(exclude = "com.google.protobuf:protobuf-java")
    void noProtobufInClasspath_shouldWork() {
        assertThatCode(() -> Json.parse("""
                {"name":"Alice"}
                """, Object.class)).doesNotThrowAnyException();
        assertThatCode(() -> Json.stringify(Map.of("name", "Alice"))).doesNotThrowAnyException();
    }
}
