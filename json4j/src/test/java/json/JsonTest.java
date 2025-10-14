package json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
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
                    Arguments.of(List.of(1, 2, 3), "[1,2,3]"),
                    Arguments.of(Map.of("key", "value"), "{\"key\":\"value\"}"),
                    Arguments.of(new RecordPerson("Alice", 30, LocalDate.parse("1993-05-15")), "{\"name\":\"Alice\",\"age\":30,\"birthDate\":\"1993-05-15\"}"),
                    Arguments.of(new ClassPerson() {{ setName("Bob"); setAge(25); setBirthDate(LocalDate.parse("1998-10-20")); }}, "{\"age\":25,\"birthDate\":\"1998-10-20\",\"name\":\"Bob\"}"),
                    // timestamp
                    Arguments.of(Date.from(Instant.parse("2024-03-15T10:15:30Z")), "\"2024-03-15T10:15:30Z\""),
                    Arguments.of(Instant.parse("2024-03-15T10:15:30Z"), "\"2024-03-15T10:15:30Z\""),
                    Arguments.of(Timestamp.from(Instant.parse("2024-03-15T10:15:30Z")), "\"2024-03-15T10:15:30Z\""),
                    // date time with timezone
                    Arguments.of(LocalDateTime.parse("2024-01-01T09:00:30"), "\"2024-01-01T09:00:30\""),
                    Arguments.of(OffsetDateTime.parse("2024-01-01T09:00:00+08:00"), "\"2024-01-01T09:00+08:00\""),
                    Arguments.of(ZonedDateTime.parse("2024-01-01T09:00:00+08:00[Asia/Shanghai]"), "\"2024-01-01T09:00+08:00[Asia/Shanghai]\""),
                    // special map keys
                    Arguments.of(Map.of(1, "one"), "{\"1\":\"one\"}"),
                    Arguments.of(Map.of(true, "yes"), "{\"true\":\"yes\"}"),
                    Arguments.of(new HashMap<>() {{ put(null, "null"); }}, "{\"null\":\"null\"}"),
                    Arguments.of(Map.of(3.14, "pi"), "{\"3.14\":\"pi\"}"),
                    Arguments.of(Map.of(LocalDate.parse("2024-01-01"), "New Year"), "{\"2024-01-01\":\"New Year\"}")
            );
            // @spotless:on
        }

        @ParameterizedTest
        @MethodSource("stringifyArgs")
        void stringify(Object input, String expected) {
            assertThat(Json.stringify(input)).isEqualTo(expected);
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
                    Arguments.of("1", new Json.Type<String>() {}, "1"),
                    Arguments.of("1", new Json.Type<Boolean>() {}, true),
                    Arguments.of("0", new Json.Type<Boolean>() {}, false),
                    Arguments.of("0", new Json.Type<Character>() {}, '0'),
                    Arguments.of("\"MONDAY\"", new Json.Type<DayOfWeek>() {}, DayOfWeek.MONDAY), // exact match
                    Arguments.of("\"monday\"", new Json.Type<DayOfWeek>() {}, DayOfWeek.MONDAY), // case-insensitive
                    Arguments.of("0", new Json.Type<DayOfWeek>() {}, DayOfWeek.MONDAY), // ordinal
                    Arguments.of("\"true\"", new Json.Type<Boolean>() {}, true),
                    Arguments.of("\"false\"", new Json.Type<Boolean>() {}, false),
                    Arguments.of("\"123.4\"", new Json.Type<Double>() {}, 123.4),
                    Arguments.of("{\"key1\":\"value1\",\"key2\":[1,true,1.01]}", new Json.Type<Object>() {}, Map.of("key1", "value1", "key2", List.of(1, true, 1.01))),
                    Arguments.of("\"10000000000\"", new Json.Type<Long>() {}, 10000000000L),
                    Arguments.of("\"2025-01-01\"", new Json.Type<Iterable<LocalDate>>() {}, List.of(LocalDate.parse("2025-01-01"))),
                    Arguments.of("\"2025-01-01\"", new Json.Type<LocalDate[]>() {}, new LocalDate[] {LocalDate.parse("2025-01-01")}),
                    // Loose parsing snake_case to camelCase
                    Arguments.of("{\"name\":\"Alice\",\"birth_date\":\"1993-05-15\"}", new Json.Type<RecordPerson>() {}, new RecordPerson("Alice", 0, LocalDate.parse("1993-05-15"))),
                    Arguments.of("{\"name\":\"Bob\",\"birth_date\":\"1998-10-20\"}", new Json.Type<ClassPerson>() {}, new ClassPerson() {{ setName("Bob"); setBirthDate(LocalDate.parse("1998-10-20")); }})
            );
            // @spotless:on
        }

        @ParameterizedTest
        @MethodSource("parseArgs")
        void parse(String input, Json.Type<?> type, Object expected) {
            assertThat(Json.parse(input, type)).isEqualTo(expected);
        }

        private static Stream<Arguments> nullCannotParseToPrimitiveArgs() {
            // @spotless:off
            return Stream.of(
                    Arguments.of("null", Json.Type.of(int.class)),
                    Arguments.of("{\"age\":null,\"birth_date\":\"1998-10-20\"}", new Json.Type<ClassPerson>() {}),
                    Arguments.of("{\"age\":null,\"birthDate\":\"1998-10-20\"}", new Json.Type<RecordPerson>() {})
            );
            // @spotless:on
        }

        @ParameterizedTest
        @MethodSource("nullCannotParseToPrimitiveArgs")
        void nullCannotParseToPrimitive(String input, Json.Type<?> type) {
            assertThatCode(() -> Json.parse(input, type))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Cannot assign null to primitive");
        }

        private static Stream<Arguments> invalidJsonArgs() {
            // @spotless:off
            return Stream.of(
                    Arguments.of("\"1.5", "Unterminated string at line 1, col 5"),
                    Arguments.of("nul", "Invalid literal, expected 'null' at line 1, col 4"),
                    Arguments.of("tru", "Invalid literal, expected 'true' at line 1, col 4"),
                    Arguments.of("fals", "Invalid literal, expected 'false' at line 1, col 5"),
                    Arguments.of("false,", "Trailing characters after top-level value at line 1, col 7 (token COMMA)"),
                    Arguments.of("1,2", "Trailing characters after top-level value at line 1, col 3 (token COMMA)"),
                    Arguments.of("{x:1}", "Unexpected character: 'x' at line 1, col 2"),
                    Arguments.of("{1:2}", "Expected string as object key at line 1, col 3 (token NUMBER)"),
                    Arguments.of("{\"x\":1", "Expected ',' or '}' in object at line 1, col 7 (token EOF)"),
                    Arguments.of("[1,2", "Expected ',' or ']' in array at line 1, col 5 (token EOF)")
            );
            // @spotless:on
        }

        @ParameterizedTest
        @MethodSource("invalidJsonArgs")
        void invalidJson(String input, String containsMessage) {
            assertThatCode(() -> Json.parse(input, Object.class))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(containsMessage);
        }
    }
}
