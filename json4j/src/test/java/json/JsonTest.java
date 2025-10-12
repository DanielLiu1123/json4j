package json;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.Data;
import org.junit.jupiter.api.Nested;
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
                    Arguments.of(new ClassPerson() {{ setName("Bob"); setAge(25); setBirthDate(LocalDate.parse("1998-10-20")); }}, "{\"age\": 25, \"birthDate\": \"1998-10-20\", \"name\": \"Bob\"}"),
                    // timestamp
                    Arguments.of(Date.from(Instant.parse("2024-03-15T10:15:30Z")), "\"2024-03-15T10:15:30Z\""),
                    Arguments.of(Instant.parse("2024-03-15T10:15:30Z"), "\"2024-03-15T10:15:30Z\""),
                    Arguments.of(Timestamp.from(Instant.parse("2024-03-15T10:15:30Z")), "\"2024-03-15T10:15:30Z\""),
                    // date time with timezone
                    Arguments.of(LocalDateTime.parse("2024-01-01T09:00:30"), "\"2024-01-01T09:00:30\""),
                    Arguments.of(OffsetDateTime.parse("2024-01-01T09:00:00+08:00"), "\"2024-01-01T09:00+08:00\""),
                    Arguments.of(ZonedDateTime.parse("2024-01-01T09:00:00+08:00[Asia/Shanghai]"), "\"2024-01-01T09:00+08:00[Asia/Shanghai]\""),
                    // special map keys
                    Arguments.of(Map.of(1, "one"), "{\"1\": \"one\"}"),
                    Arguments.of(Map.of(true, "yes"), "{\"true\": \"yes\"}"),
                    Arguments.of(new HashMap<>() {{ put(null, "null"); }}, "{\"null\": \"null\"}"),
                    Arguments.of(Map.of(3.14, "pi"), "{\"3.14\": \"pi\"}"),
                    Arguments.of(Map.of(LocalDate.parse("2024-01-01"), "New Year"), "{\"2024-01-01\": \"New Year\"}")
            );
            // @spotless:on
        }

        @ParameterizedTest
        @MethodSource("stringifyArgs")
        void stringify(Object input, String expected) {
            // Act
            var actual = Json.stringify(input);

            // Assert
            assertThat(actual).isEqualTo(expected);
        }
    }

    @Nested
    class ParseTests {

        private static Stream<Arguments> parseArgs() {
            // @spotless:off
            return Stream.of(
                    Arguments.of("\"str\"", Json.Type.of(String.class), "str"),
                    Arguments.of("7", Json.Type.of(Integer.class), 7),
                    Arguments.of("7", Json.Type.of(String.class), "7"),
                    Arguments.of("false", Json.Type.of(Boolean.class), false),
                    Arguments.of("null", new Json.Type<RecordPerson>() {}, null),
                    Arguments.of("null", new Json.Type<String>() {}, null),
                    Arguments.of("[]", new Json.Type<List<RecordPerson>>() {}, List.of()),
                    Arguments.of("{}", new Json.Type<Map<String, RecordPerson>>() {}, Map.of()),
                    Arguments.of("\"2025-10-10\"", new Json.Type<LocalDate>() {}, LocalDate.parse("2025-10-10")),
                    Arguments.of("[1,2,3]", new Json.Type<List<Integer>>() {}, List.of(1, 2, 3)),
                    Arguments.of("{\"left\":1,\"right\":2}", new Json.Type<Map<String, Integer>>() {}, Map.of("left", 1, "right", 2)),
                    Arguments.of("{\"name\":\"Alice\",\"age\":30}", new Json.Type<RecordPerson>() {}, new RecordPerson("Alice", 30, null)),
                    Arguments.of("[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25,\"birthDate\":\"2025-10-10\"}]", new Json.Type<List<RecordPerson>>() {}, List.of(new RecordPerson("Alice", 30, null), new RecordPerson("Bob", 25, LocalDate.parse("2025-10-10")))),
                    Arguments.of("[[1,2],[3,4]]", new Json.Type<List<List<Integer>>>() {}, List.of(List.of(1, 2), List.of(3, 4))),
                    Arguments.of("{\"group1\":[{\"name\":\"Alice\",\"age\":30}],\"group2\":[{\"name\":\"Bob\",\"age\":25}]}", new Json.Type<Map<String, List<RecordPerson>>>() {}, Map.of("group1", List.of(new RecordPerson("Alice", 30, null)), "group2", List.of(new RecordPerson("Bob", 25, null)))),
                    // Number parsing
                    Arguments.of("42", new Json.Type<>() {}, 42),
                    Arguments.of("10000000000", new Json.Type<>() {}, 10000000000L),
                    Arguments.of("9999999999999999999999999", new Json.Type<>() {}, new BigInteger("9999999999999999999999999")),
                    Arguments.of("3.14", new Json.Type<>() {}, 3.14),
                    Arguments.of("1.0000000000000001", new Json.Type<>() {}, new BigDecimal("1.0000000000000001")),
                    // timestamp
                    Arguments.of("\"2024-03-15T10:15:30Z\"", new Json.Type<Date>() {}, Date.from(Instant.parse("2024-03-15T10:15:30Z"))),
                    Arguments.of("\"2024-03-15T10:15:30Z\"", new Json.Type<Instant>() {}, Instant.parse("2024-03-15T10:15:30Z")),
                    Arguments.of("\"2024-03-15T10:15:30Z\"", new Json.Type<Timestamp>() {}, Timestamp.from(Instant.parse("2024-03-15T10:15:30Z"))),
                    // date time with timezone
                    Arguments.of("\"2024-01-01T09:00+08:00\"", new Json.Type<OffsetDateTime>() {}, OffsetDateTime.parse("2024-01-01T09:00+08:00")),
                    Arguments.of("\"2024-01-01T09:00+08:00[Asia/Shanghai]\"", new Json.Type<ZonedDateTime>() {}, ZonedDateTime.parse("2024-01-01T09:00+08:00[Asia/Shanghai]")),
                    // special map keys
                    Arguments.of("{\"1\":\"one\"}", new Json.Type<Map<Integer, String>>() {}, Map.of(1, "one")),
                    Arguments.of("{\"true\":\"yes\"}", new Json.Type<Map<Boolean, String>>() {}, Map.of(true, "yes")),
                    Arguments.of("{\"3.14\":\"pi\"}", new Json.Type<Map<Double, String>>() {}, Map.of(3.14, "pi")),
                    Arguments.of("{\"2024-01-01\":\"New Year\"}", new Json.Type<Map<LocalDate, String>>() {}, Map.of(LocalDate.parse("2024-01-01"), "New Year")),
                    // Loose parsing
                    Arguments.of("{\"key1\":\"value1\",\"key2\":[1,true,1.01]}", new Json.Type<Object>() {}, Map.of("key1", "value1", "key2", List.of(1, true, 1.01))),
                    Arguments.of("\"123.4\"", new Json.Type<Double>() {}, 123.4),
                    Arguments.of("\"10000000000\"", new Json.Type<Long>() {}, 10000000000L)
            );
            // @spotless:on
        }

        @ParameterizedTest
        @MethodSource("parseArgs")
        void parse(String input, Json.Type<?> type, Object expected) {
            // Act
            var actual = Json.parse(input, type);

            // Assert
            assertThat(actual).isEqualTo(expected);
        }
    }
}
