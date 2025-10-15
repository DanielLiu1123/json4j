package json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.google.protobuf.BoolValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import json4j.user.Hobby;
import json4j.user.User;
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
                    Arguments.of(Stream.of(Optional.empty(), Optional.of(1), Optional.of("str"), Optional.of(true), Optional.of(new RecordPerson("Alice", 30, null))), "[null,1,\"str\",true,{\"name\":\"Alice\",\"age\":30,\"birthDate\":null}]"),
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
                    Arguments.of(Map.of(LocalDate.parse("2024-01-01"), "New Year"), "{\"2024-01-01\":\"New Year\"}"),
                    // Protobuf support
                    Arguments.of(User.newBuilder().setId(1).setName("Freeman").setStatus(User.Status.ACTIVE).addAllHobbyList(List.of(Hobby.newBuilder().setId(1).setName("gaming").build())).putAllHobbyMap(Map.of(1L, Hobby.newBuilder().setId(1).setName("gaming").build())).build(), "{\"id\":1,\"name\":\"Freeman\",\"status\":\"ACTIVE\",\"hobbyList\":[{\"id\":1,\"name\":\"gaming\"}],\"hobbyMap\":{\"1\":{\"id\":1,\"name\":\"gaming\"}}}"),
                    Arguments.of(User.newBuilder().build(), "{\"id\":0,\"name\":\"\",\"status\":\"STATUS_UNSPECIFIED\",\"hobbyList\":[],\"hobbyMap\":{}}")
            );
            // @spotless:on
        }

        @ParameterizedTest
        @MethodSource("stringifyArgs")
        void stringify(Object input, String expected) {
            assertThat(Json.stringify(input)).isEqualTo(expected);
        }

        @Test
        void stringifyOptional() {
            record Foo(Optional<String> name, Optional<Integer> age) {}

            var table = new Object[][] {
                {new Foo(Optional.of("Alice"), Optional.of(30)), "{\"name\":\"Alice\",\"age\":30}"},
                {new Foo(Optional.of("Bob"), Optional.empty()), "{\"name\":\"Bob\"}"},
                {new Foo(Optional.empty(), Optional.of(25)), "{\"age\":25}"},
                {new Foo(Optional.empty(), Optional.empty()), "{}"},
                {new Foo(null, Optional.empty()), "{\"name\":null}"},
                {new Foo(Optional.empty(), null), "{\"age\":null}"}
            };

            for (var row : table) {
                var input = (Foo) row[0];
                var expected = (String) row[1];
                var actual = Json.stringify(input);
                assertThat(actual).isEqualTo(expected);
            }
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
                    Arguments.of("null", new Json.Type<Optional<?>>() {}, null),
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
                    Arguments.of("{\"name\":\"Bob\",\"birth_date\":\"1998-10-20\"}", new Json.Type<ClassPerson>() {}, new ClassPerson() {{ setName("Bob"); setBirthDate(LocalDate.parse("1998-10-20")); }}),
                    // Protobuf support
                    Arguments.of("{\"id\":1,\"name\":\"Freeman\",\"status\":\"ACTIVE\",\"hobbyList\":[{\"id\":1,\"name\":\"gaming\"}],\"hobbyMap\":{\"1\":{\"id\":1,\"name\":\"gaming\"}}}", new Json.Type<User>() {}, User.newBuilder().setId(1).setName("Freeman").setStatus(User.Status.ACTIVE).addAllHobbyList(List.of(Hobby.newBuilder().setId(1).setName("gaming").build())).putAllHobbyMap(Map.of(1L, Hobby.newBuilder().setId(1).setName("gaming").build())).build()),
                    Arguments.of("{}", new Json.Type<User>() {}, User.newBuilder().build()),
                    Arguments.of("{\"id\":0,\"hobby_list\":[]}", new Json.Type<User>() {}, User.newBuilder().build()),
                    Arguments.of("1", new Json.Type<Int32Value>() {}, Int32Value.newBuilder().setValue(1).build()),
                    Arguments.of("1", new Json.Type<Value>() {}, Value.newBuilder().setNumberValue(1).build()),
                    Arguments.of("null", new Json.Type<NullValue>() {}, NullValue.NULL_VALUE),
                    Arguments.of("0", new Json.Type<NullValue>() {}, NullValue.NULL_VALUE),
                    Arguments.of("\"NULL_VALUE\"", new Json.Type<NullValue>() {}, NullValue.NULL_VALUE),
                    Arguments.of("2", new Json.Type<com.google.type.DayOfWeek>() {}, com.google.type.DayOfWeek.TUESDAY),
                    Arguments.of("\"UNRECOGNIZED\"", new Json.Type<com.google.type.DayOfWeek>() {}, com.google.type.DayOfWeek.UNRECOGNIZED),
                    Arguments.of("null", new Json.Type<Value>() {}, Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build()),
                    Arguments.of("null", new Json.Type<ListValue>() {}, ListValue.newBuilder().addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build()).build()),
                    Arguments.of("\"NULL_VALUE\"", new Json.Type<Value>() {}, Value.newBuilder().setStringValue("NULL_VALUE").build()),
                    Arguments.of("[]", new Json.Type<ListValue>() {}, ListValue.newBuilder().build()),
                    Arguments.of("[]", new Json.Type<Value>() {}, Value.newBuilder().setListValue(ListValue.newBuilder().build()).build()),
                    Arguments.of("\"str\"", new Json.Type<Value>() {}, Value.newBuilder().setStringValue("str").build()),
                    Arguments.of("true", new Json.Type<Value>() {}, Value.newBuilder().setBoolValue(true).build(),
                    Arguments.of("\"true\"", new Json.Type<BoolValue>() {}, Value.newBuilder().setBoolValue(true).build())),
                    Arguments.of("{\"value\":true}", new Json.Type<Struct>() {}, Struct.newBuilder().putFields("value", Value.newBuilder().setBoolValue(true).build()).build()),
                    Arguments.of("{\"value\":true}", new Json.Type<Value>() {}, Value.newBuilder().setStructValue(Struct.newBuilder().putFields("value", Value.newBuilder().setBoolValue(true).build()).build()).build()),
                    Arguments.of("[1,true,\"str\"]", new Json.Type<ListValue>() {}, ListValue.newBuilder().addValues(Value.newBuilder().setNumberValue(1).build()).addValues(Value.newBuilder().setBoolValue(true).build()).addValues(Value.newBuilder().setStringValue("str").build()).build()),
                    Arguments.of("[{\"id\":1,\"name\":\"Freeman\"}]", new Json.Type<List<User>>() {}, List.of(User.newBuilder().setId(1).setName("Freeman").build())),
                    Arguments.of("[{\"id\":1,\"name\":\"Freeman\"}]", new Json.Type<User[]>() {}, new User[] {User.newBuilder().setId(1).setName("Freeman").build()}),
                    Arguments.of("[{\"id\":1,\"name\":\"Freeman\"}]", new Json.Type<ListValue>() {}, ListValue.newBuilder().addValues(Value.newBuilder().setStructValue(Struct.newBuilder().putFields("id", Value.newBuilder().setNumberValue(1).build()).putFields("name", Value.newBuilder().setStringValue("Freeman").build()).build()).build()).build())
            );
            // @spotless:on
        }

        @ParameterizedTest
        @MethodSource("parseArgs")
        void parse(String input, Json.Type<?> type, Object expected) {
            assertThat(Json.parse(input, type)).isEqualTo(expected);
        }

        @Test
        void parseStream() {
            var input = "[null,1,\"str\",true,{\"name\":\"Alice\",\"age\":30,\"birthDate\":null}]";
            var expected = new ArrayList<>() {
                {
                    add(null);
                    add(1);
                    add("str");
                    add(true);
                    add(new LinkedHashMap<>() {
                        {
                            put("name", "Alice");
                            put("age", 30);
                            put("birthDate", null);
                        }
                    });
                }
            };

            var actual = Json.parse(input, new Json.Type<Stream<Object>>() {}).toList();
            assertThat(actual).isEqualTo(expected);
        }

        @Test
        void parseOptional() {
            record Foo(Optional<String> name, Optional<Integer> age) {}

            var table = new Object[][] {
                {"{\"name\":\"Alice\",\"age\":30}", new Foo(Optional.of("Alice"), Optional.of(30))},
                {"{\"name\":\"Bob\"}", new Foo(Optional.of("Bob"), Optional.empty())},
                {"{\"age\":25}", new Foo(Optional.empty(), Optional.of(25))},
                {"{}", new Foo(Optional.empty(), Optional.empty())},
                {"{\"name\":null,\"age\":null}", new Foo(null, null)}
            };

            for (var row : table) {
                var input = (String) row[0];
                var expected = (Foo) row[1];
                var actual = Json.parse(input, new Json.Type<Foo>() {});
                assertThat(actual).isEqualTo(expected);
            }
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
