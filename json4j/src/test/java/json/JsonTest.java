package json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertAll;

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
import java.util.stream.IntStream;
import java.util.stream.Stream;
import json4j.user.Hobby;
import json4j.user.User;
import lombok.Data;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

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

        @Test
        void stringify() {
            // @spotless:off
            var table = new Object[][] {
                    {42, "42"},
                    {true, "true"},
                    {false, "false"},
                    {3.14, "3.14"},
                    {-0.01, "-0.01"},
                    {null, "null"},
                    {"hello", "\"hello\""},
                    {"", "\"\""},
                    {Stream.of(Optional.empty(), Optional.of(1), Optional.of("str"), Optional.of(true), Optional.of(new RecordPerson("Alice", 30, null))), "[null,1,\"str\",true,{\"name\":\"Alice\",\"age\":30,\"birthDate\":null}]"},
                    {LocalDate.parse("2023-01-01"), "\"2023-01-01\""},
                    {List.of(1, 2, 3), "[1,2,3]"},
                    {Map.of("key", "value"), "{\"key\":\"value\"}"},
                    {new RecordPerson("Alice", 30, LocalDate.parse("1993-05-15")), "{\"name\":\"Alice\",\"age\":30,\"birthDate\":\"1993-05-15\"}"},
                    {new ClassPerson() {{ setName("Bob"); setAge(25); setBirthDate(LocalDate.parse("1998-10-20")); }}, "{\"age\":25,\"birthDate\":\"1998-10-20\",\"name\":\"Bob\"}"},
                    // timestamp
                    {Date.from(Instant.parse("2024-03-15T10:15:30Z")), "\"2024-03-15T10:15:30Z\""},
                    {Instant.parse("2024-03-15T10:15:30Z"), "\"2024-03-15T10:15:30Z\""},
                    {Timestamp.from(Instant.parse("2024-03-15T10:15:30Z")), "\"2024-03-15T10:15:30Z\""},
                    // date time with timezone
                    {LocalDateTime.parse("2024-01-01T09:00:30"), "\"2024-01-01T09:00:30\""},
                    {OffsetDateTime.parse("2024-01-01T09:00:00+08:00"), "\"2024-01-01T09:00+08:00\""},
                    {ZonedDateTime.parse("2024-01-01T09:00:00+08:00[Asia/Shanghai]"), "\"2024-01-01T09:00+08:00[Asia/Shanghai]\""},
                    // special map keys
                    {Map.of(1, "one"), "{\"1\":\"one\"}"},
                    {Map.of(true, "yes"), "{\"true\":\"yes\"}"},
                    {new HashMap<>() {{ put(null, "null"); }}, "{\"null\":\"null\"}"},
                    {Map.of(3.14, "pi"), "{\"3.14\":\"pi\"}"},
                    {Map.of(LocalDate.parse("2024-01-01"), "New Year"), "{\"2024-01-01\":\"New Year\"}"},
                    // Protobuf support
                    {User.newBuilder().setId(1).setName("Freeman").setStatus(User.Status.ACTIVE).addAllHobbyList(List.of(Hobby.newBuilder().setId(1).setName("gaming").build())).putAllHobbyMap(Map.of(1L, Hobby.newBuilder().setId(1).setName("gaming").build())).build(), "{\"id\":1,\"name\":\"Freeman\",\"status\":\"ACTIVE\",\"hobbyList\":[{\"id\":1,\"name\":\"gaming\"}],\"hobbyMap\":{\"1\":{\"id\":1,\"name\":\"gaming\"}}}"},
                    {User.newBuilder().build(), "{\"id\":0,\"name\":\"\",\"status\":\"STATUS_UNSPECIFIED\",\"hobbyList\":[],\"hobbyMap\":{}}"}
            };
            // @spotless:on

            assertAll(IntStream.range(0, table.length).mapToObj(i -> () -> {
                var row = table[i];
                var input = row[0];
                var expected = (String) row[1];
                var actual = Json.stringify(input);
                assertThat(actual).as("Case %d: input=%s", i, input).isEqualTo(expected);
            }));
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

            assertAll(IntStream.range(0, table.length).mapToObj(i -> () -> {
                var row = table[i];
                var input = (Foo) row[0];
                var expected = (String) row[1];
                var actual = Json.stringify(input);
                assertThat(actual).as("Case %d: input=%s", i, input).isEqualTo(expected);
            }));
        }
    }

    @Nested
    class ParseTests {

        @Test
        void parse() {
            // @spotless:off
            var table = new Object[][] {
                    {"\"str\"", Json.Type.of(String.class), "str"},
                    {"7", Json.Type.of(Integer.class), 7},
                    {"7", Json.Type.of(String.class), "7"},
                    {"false", Json.Type.of(Boolean.class), false},
                    {"null", new Json.Type<RecordPerson>() {}, null},
                    {"null", new Json.Type<String>() {}, null},
                    {"null", new Json.Type<Optional<?>>() {}, null},
                    {"[]", new Json.Type<List<RecordPerson>>() {}, List.of()},
                    {"{}", new Json.Type<Map<String, RecordPerson>>() {}, Map.of()},
                    {"\"2025-10-10\"", new Json.Type<LocalDate>() {}, LocalDate.parse("2025-10-10")},
                    {"[1,2,3]", new Json.Type<List<Integer>>() {}, List.of(1, 2, 3)},
                    {"{\"left\":1,\"right\":2}", new Json.Type<Map<String, Integer>>() {}, Map.of("left", 1, "right", 2)},
                    {"{\"name\":\"Alice\",\"age\":30}", new Json.Type<RecordPerson>() {}, new RecordPerson("Alice", 30, null)},
                    {"[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25,\"birthDate\":\"2025-10-10\"}]", new Json.Type<List<RecordPerson>>() {}, List.of(new RecordPerson("Alice", 30, null), new RecordPerson("Bob", 25, LocalDate.parse("2025-10-10")))},
                    {"[[1,2],[3,4]]", new Json.Type<List<List<Integer>>>() {}, List.of(List.of(1, 2), List.of(3, 4))},
                    {"{\"group1\":[{\"name\":\"Alice\",\"age\":30}],\"group2\":[{\"name\":\"Bob\",\"age\":25}]}", new Json.Type<Map<String, List<RecordPerson>>>() {}, Map.of("group1", List.of(new RecordPerson("Alice", 30, null)), "group2", List.of(new RecordPerson("Bob", 25, null)))},
                    // Number parsing
                    {"42", new Json.Type<>() {}, 42},
                    {"10000000000", new Json.Type<>() {}, 10000000000L},
                    {"9999999999999999999999999", new Json.Type<>() {}, new BigInteger("9999999999999999999999999")},
                    {"3.14", new Json.Type<>() {}, 3.14},
                    {"1.0000000000000001", new Json.Type<>() {}, new BigDecimal("1.0000000000000001")},
                    // timestamp
                    {"\"2024-03-15T10:15:30Z\"", new Json.Type<Date>() {}, Date.from(Instant.parse("2024-03-15T10:15:30Z"))},
                    {"\"2024-03-15T10:15:30Z\"", new Json.Type<Instant>() {}, Instant.parse("2024-03-15T10:15:30Z")},
                    {"\"2024-03-15T10:15:30Z\"", new Json.Type<Timestamp>() {}, Timestamp.from(Instant.parse("2024-03-15T10:15:30Z"))},
                    // date time with timezone
                    {"\"2024-01-01T09:00+08:00\"", new Json.Type<OffsetDateTime>() {}, OffsetDateTime.parse("2024-01-01T09:00+08:00")},
                    {"\"2024-01-01T09:00+08:00[Asia/Shanghai]\"", new Json.Type<ZonedDateTime>() {}, ZonedDateTime.parse("2024-01-01T09:00+08:00[Asia/Shanghai]")},
                    // special map keys
                    {"{\"1\":\"one\"}", new Json.Type<Map<Integer, String>>() {}, Map.of(1, "one")},
                    {"{\"true\":\"yes\"}", new Json.Type<Map<Boolean, String>>() {}, Map.of(true, "yes")},
                    {"{\"3.14\":\"pi\"}", new Json.Type<Map<Double, String>>() {}, Map.of(3.14, "pi")},
                    {"{\"2024-01-01\":\"New Year\"}", new Json.Type<Map<LocalDate, String>>() {}, Map.of(LocalDate.parse("2024-01-01"), "New Year")},
                    // Loose parsing
                    {"1", new Json.Type<String>() {}, "1"},
                    {"1", new Json.Type<Boolean>() {}, true},
                    {"0", new Json.Type<Boolean>() {}, false},
                    {"0", new Json.Type<Character>() {}, '0'},
                    {"\"MONDAY\"", new Json.Type<DayOfWeek>() {}, DayOfWeek.MONDAY}, // exact match
                    {"\"monday\"", new Json.Type<DayOfWeek>() {}, DayOfWeek.MONDAY}, // case-insensitive
                    {"0", new Json.Type<DayOfWeek>() {}, DayOfWeek.MONDAY}, // ordinal
                    {"\"true\"", new Json.Type<Boolean>() {}, true},
                    {"\"false\"", new Json.Type<Boolean>() {}, false},
                    {"\"123.4\"", new Json.Type<Double>() {}, 123.4},
                    {"{\"key1\":\"value1\",\"key2\":[1,true,1.01]}", new Json.Type<Object>() {}, Map.of("key1", "value1", "key2", List.of(1, true, 1.01))},
                    {"\"10000000000\"", new Json.Type<Long>() {}, 10000000000L},
                    {"\"2025-01-01\"", new Json.Type<Iterable<LocalDate>>() {}, List.of(LocalDate.parse("2025-01-01"))},
                    {"\"2025-01-01\"", new Json.Type<LocalDate[]>() {}, new LocalDate[] {LocalDate.parse("2025-01-01")}},
                    // Loose parsing snake_case to camelCase
                    {"{\"name\":\"Alice\",\"birth_date\":\"1993-05-15\"}", new Json.Type<RecordPerson>() {}, new RecordPerson("Alice", 0, LocalDate.parse("1993-05-15"))},
                    {"{\"name\":\"Bob\",\"birth_date\":\"1998-10-20\"}", new Json.Type<ClassPerson>() {}, new ClassPerson() {{ setName("Bob"); setBirthDate(LocalDate.parse("1998-10-20")); }}},
                    // Protobuf support
                    {"{\"id\":1,\"name\":\"Freeman\",\"status\":\"ACTIVE\",\"hobbyList\":[{\"id\":1,\"name\":\"gaming\"}],\"hobbyMap\":{\"1\":{\"id\":1,\"name\":\"gaming\"}}}", new Json.Type<User>() {}, User.newBuilder().setId(1).setName("Freeman").setStatus(User.Status.ACTIVE).addAllHobbyList(List.of(Hobby.newBuilder().setId(1).setName("gaming").build())).putAllHobbyMap(Map.of(1L, Hobby.newBuilder().setId(1).setName("gaming").build())).build()},
                    {"{}", new Json.Type<User>() {}, User.newBuilder().build()},
                    {"{\"id\":0,\"hobby_list\":[]}", new Json.Type<User>() {}, User.newBuilder().build()},
                    {"1", new Json.Type<Int32Value>() {}, Int32Value.newBuilder().setValue(1).build()},
                    {"1", new Json.Type<Value>() {}, Value.newBuilder().setNumberValue(1).build()},
                    {"null", new Json.Type<NullValue>() {}, NullValue.NULL_VALUE},
                    {"0", new Json.Type<NullValue>() {}, NullValue.NULL_VALUE},
                    {"\"NULL_VALUE\"", new Json.Type<NullValue>() {}, NullValue.NULL_VALUE},
                    {"2", new Json.Type<com.google.type.DayOfWeek>() {}, com.google.type.DayOfWeek.TUESDAY},
                    {"\"UNRECOGNIZED\"", new Json.Type<com.google.type.DayOfWeek>() {}, com.google.type.DayOfWeek.UNRECOGNIZED},
                    {"null", new Json.Type<Value>() {}, Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build()},
                    {"null", new Json.Type<ListValue>() {}, ListValue.newBuilder().addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build()).build()},
                    {"\"NULL_VALUE\"", new Json.Type<Value>() {}, Value.newBuilder().setStringValue("NULL_VALUE").build()},
                    {"[]", new Json.Type<ListValue>() {}, ListValue.newBuilder().build()},
                    {"[]", new Json.Type<Value>() {}, Value.newBuilder().setListValue(ListValue.newBuilder().build()).build()},
                    {"\"str\"", new Json.Type<Value>() {}, Value.newBuilder().setStringValue("str").build()},
                    {"true", new Json.Type<Value>() {}, Value.newBuilder().setBoolValue(true).build()},
                    {"\"true\"", new Json.Type<BoolValue>() {}, BoolValue.newBuilder().setValue(true).build()},
                    {"{\"value\":true}", new Json.Type<Struct>() {}, Struct.newBuilder().putFields("value", Value.newBuilder().setBoolValue(true).build()).build()},
                    {"{\"value\":true}", new Json.Type<Value>() {}, Value.newBuilder().setStructValue(Struct.newBuilder().putFields("value", Value.newBuilder().setBoolValue(true).build()).build()).build()},
                    {"[1,true,\"str\"]", new Json.Type<ListValue>() {}, ListValue.newBuilder().addValues(Value.newBuilder().setNumberValue(1).build()).addValues(Value.newBuilder().setBoolValue(true).build()).addValues(Value.newBuilder().setStringValue("str").build()).build()},
                    {"[{\"id\":1,\"name\":\"Freeman\"}]", new Json.Type<List<User>>() {}, List.of(User.newBuilder().setId(1).setName("Freeman").build())},
                    {"[{\"id\":1,\"name\":\"Freeman\"}]", new Json.Type<User[]>() {}, new User[] {User.newBuilder().setId(1).setName("Freeman").build()}},
                    {"[{\"id\":1,\"name\":\"Freeman\"}]", new Json.Type<ListValue>() {}, ListValue.newBuilder().addValues(Value.newBuilder().setStructValue(Struct.newBuilder().putFields("id", Value.newBuilder().setNumberValue(1).build()).putFields("name", Value.newBuilder().setStringValue("Freeman").build()).build()).build()).build()}
            };
            // @spotless:on

            assertAll(IntStream.range(0, table.length).mapToObj(i -> () -> {
                var row = table[i];
                var input = (String) row[0];
                var type = (Json.Type<?>) row[1];
                var expected = row[2];
                var actual = Json.parse(input, type);
                assertThat(actual)
                        .as("Case %d: input=%s, type=%s", i, input, type)
                        .isEqualTo(expected);
            }));
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

            assertAll(IntStream.range(0, table.length).mapToObj(i -> () -> {
                var row = table[i];
                var input = (String) row[0];
                var expected = (Foo) row[1];
                var actual = Json.parse(input, new Json.Type<Foo>() {});
                assertThat(actual).as("Case %d: input=%s", i, input).isEqualTo(expected);
            }));
        }

        @Test
        void nullCannotParseToPrimitive() {
            // @spotless:off
            var table = new Object[][] {
                    {"null", Json.Type.of(int.class)},
                    {"{\"age\":null,\"birth_date\":\"1998-10-20\"}", new Json.Type<ClassPerson>() {}},
                    {"{\"age\":null,\"birthDate\":\"1998-10-20\"}", new Json.Type<RecordPerson>() {}}
            };
            // @spotless:on

            assertAll(IntStream.range(0, table.length).mapToObj(i -> () -> {
                var row = table[i];
                var input = (String) row[0];
                var type = (Json.Type<?>) row[1];
                assertThatCode(() -> Json.parse(input, type))
                        .as("Case %d: input=%s, type=%s", i, input, type)
                        .isInstanceOf(Json.ConversionException.class)
                        .hasMessageContaining("Cannot assign null to primitive");
            }));
        }

        @Test
        void syntaxException() {
            // @spotless:off
            var table = new Object[][] {
                    {"\"1.5", "Unterminated string literal at line 1, column 5"},
                    {"nul", "Invalid literal, expected 'null' at line 1, column 4"},
                    {"tru", "Invalid literal, expected 'true' at line 1, column 4"},
                    {"fals", "Invalid literal, expected 'false' at line 1, column 5"},
                    {"false,", "Trailing characters after top-level value (token: COMMA) at line 1, column 7"},
                    {"1,2", "Trailing characters after top-level value (token: COMMA) at line 1, column 3"},
                    {"{x:1}", "Unexpected character: 'x' at line 1, column 2"},
                    {"{1:2}", "Expected string key in object (token: NUMBER) at line 1, column 3"},
                    {"{\"x\":1", "Expected ',' or '}' in object (token: EOF) at line 1, column 7"},
                    {"[1,2", "Expected ',' or ']' in array (token: EOF) at line 1, column 5"}
            };
            // @spotless:on

            assertAll(IntStream.range(0, table.length).mapToObj(i -> () -> {
                var row = table[i];
                var input = (String) row[0];
                var containsMessage = (String) row[1];
                assertThatCode(() -> Json.parse(input, Object.class))
                        .as("Case %d: input=%s", i, input)
                        .isInstanceOf(Json.SyntaxException.class)
                        .hasMessageContaining(containsMessage);
            }));
        }
    }
}
