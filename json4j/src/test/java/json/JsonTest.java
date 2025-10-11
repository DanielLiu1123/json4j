package json;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.Data;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class JsonTest {

    private record RecordPerson(String name, int age, LocalDate birthDate) {}

    @Data
    static class ClassPerson {
        private String name;
        private int age;
        private LocalDate birthDate;
    }

    @Nested
    class StringifyTests {

        private static Stream<Arguments> stringifyArgs() {
            // @spotless:off
            return Stream.of(
                    Arguments.of(42, "42"),
                    Arguments.of(true, "true"),
                    Arguments.of(false, "false"),
                    Arguments.of(3.14, "3.14"),
                    Arguments.of(-0.01, "-0.01"),
                    Arguments.of(null, "null"),
                    Arguments.of("hello", "\"hello\""),
                    Arguments.of("", "\"\""),
                    Arguments.of(LocalDate.parse("2023-01-01"), "\"2023-01-01\""),
                    Arguments.of(List.of(1, 2, 3), "[1, 2, 3]"),
                    Arguments.of(Map.of("key", "value"), "{\"key\": \"value\"}"),
                    Arguments.of(new LinkedHashMap<>(Map.of("a", 1, "b", 2)), "{\"a\": 1, \"b\": 2}"),
                    Arguments.of(new RecordPerson("Alice", 30, LocalDate.parse("1993-05-15")), "{\"age\": 30, \"birthDate\": \"1993-05-15\", \"name\": \"Alice\"}"),
                    Arguments.of(new ClassPerson() {{ setName("Bob"); setAge(25); setBirthDate(LocalDate.parse("1998-10-20")); }}, "{\"age\": 25, \"birthDate\": \"1998-10-20\", \"name\": \"Bob\"}")
            );
            // @spotless:on
        }

        @ParameterizedTest
        @MethodSource("stringifyArgs")
        void showStringifyOK(Object value, String expectedJson) {
            // Arrange
            var input = value;

            // Act
            var actual = Json.stringify(input);

            // Assert
            var expected = expectedJson;
            assertThat(actual).isEqualTo(expected);
        }

        @Test
        void timestampShouldUseSameFormat() {
            // Date/Instant/Timestamp use same ISO-8601 format
            var instant = Instant.parse("2024-03-15T10:15:30Z");
            var date = Date.from(instant);
            var timestamp = Timestamp.from(instant);

            var actual = Json.stringify(Map.of(
                    "date", date,
                    "instant", instant,
                    "timestamp", timestamp));

            var expected =
                    "{\"date\": \"%1$s\", \"instant\": \"%1$s\", \"timestamp\": \"%1s\"}".formatted(instant.toString());
            assertThat(actual).isEqualTo(expected);
        }

        @Test
        void dateTimeWithTimeZone() {
            // Arrange
            var zdt = LocalDateTime.parse("2024-01-01T09:00:00").atZone(ZoneId.of("Asia/Shanghai"));
            var odt = OffsetDateTime.ofInstant(zdt.toInstant(), zdt.getZone());

            // Act
            var actual = Json.stringify(Map.of(
                    "zoneDateTime", zdt,
                    "offsetDateTime", odt));

            // Assert
            var expected =
                    "{\"offsetDateTime\": \"2024-01-01T09:00+08:00\", \"zoneDateTime\": \"2024-01-01T09:00+08:00[Asia/Shanghai]\"}";
            assertThat(actual).isEqualTo(expected);
        }
    }

    @Nested
    class ParseTests {

        private static Stream<Arguments> parseArgs() {
            // @spotless:off
            return Stream.of(
                    Arguments.of("\"json\"", Json.Type.of(String.class), "json"),
                    Arguments.of("7", Json.Type.of(Integer.class), 7),
                    Arguments.of("false", Json.Type.of(Boolean.class), false),
                    Arguments.of("null", new Json.Type<RecordPerson>() {}, null),
                    Arguments.of("[]", new Json.Type<List<RecordPerson>>() {}, List.of()),
                    Arguments.of("{}", new Json.Type<Map<String, RecordPerson>>() {}, Map.of()),
                    Arguments.of("\"2025-10-10\"", new Json.Type<LocalDate>() {}, LocalDate.parse("2025-10-10")),
                    Arguments.of("[1,2,3]", new Json.Type<List<Integer>>() {}, List.of(1, 2, 3)),
                    Arguments.of("{\"left\":1,\"right\":2}", new Json.Type<Map<String, Integer>>() {}, Map.of("left", 1, "right", 2)),
                    Arguments.of("{\"name\":\"Alice\",\"age\":30}", new Json.Type<RecordPerson>() {}, new RecordPerson("Alice", 30, null)),
                    Arguments.of("[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25,\"birthDate\":\"2025-10-10\"}]", new Json.Type<List<RecordPerson>>() {}, List.of(new RecordPerson("Alice", 30, null), new RecordPerson("Bob", 25, LocalDate.parse("2025-10-10")))),
                    Arguments.of("[[1,2],[3,4]]", new Json.Type<List<List<Integer>>>() {}, List.of(List.of(1, 2), List.of(3, 4))),
                    Arguments.of("{\"group1\":[{\"name\":\"Alice\",\"age\":30}],\"group2\":[{\"name\":\"Bob\",\"age\":25}]}", new Json.Type<Map<String, List<RecordPerson>>>() {}, Map.of("group1", List.of(new RecordPerson("Alice", 30, null)), "group2", List.of(new RecordPerson("Bob", 25, null)))),
                    Arguments.of("\"2024-01-01T09:00+08:00\"", new Json.Type<OffsetDateTime>() {}, OffsetDateTime.parse("2024-01-01T09:00+08:00")),
                    Arguments.of("\"2024-01-01T09:00+08:00[Asia/Shanghai]\"", new Json.Type<ZonedDateTime>() {}, ZonedDateTime.parse("2024-01-01T09:00+08:00[Asia/Shanghai]"))
            );
            // @spotless:on
        }

        @ParameterizedTest
        @MethodSource("parseArgs")
        <T> void shouldParseOK(String jsonInput, Json.Type<T> type, T expectedValue) {
            // Arrange
            var json = jsonInput;
            var typeRef = type;

            // Act
            var actual = Json.parse(json, typeRef);

            // Assert
            var expected = expectedValue;
            assertThat(actual).isEqualTo(expected);
        }
    }
}
