package json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertAll;

import com.google.protobuf.Any;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.Timestamps;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Currency;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
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
        void stringify() throws Exception {
            // @spotless:off
            var table = new Object[][] {
                    // null
                    {null, "null"},

                    // String/CharSequence
                    {"hello", "\"hello\""},
                    {"", "\"\""},

                    // Character (handled as String)
                    {'A', "\"A\""},

                    // Boolean
                    {true, "true"},
                    {false, "false"},

                    // Number
                    {42, "42"},
                    {42L, "\"42\""},
                    {BigDecimal.valueOf(1), "\"1\""},
                    {BigInteger.valueOf(1), "\"1\""},
                    {3.14, "3.14"},
                    {-0.01, "-0.01"},

                    // Optional
                    {Optional.of(1), "1"},
                    {Optional.of("str"), "\"str\""},
                    {Optional.empty(), "null"},

                    // Atomic types
                    {new AtomicInteger(42), "42"},
                    {new AtomicLong(10000000000L), "\"10000000000\""},
                    {new AtomicBoolean(true), "true"},
                    {new AtomicReference<>("hello"), "\"hello\""},
                    {new AtomicReference<>(null), "null"},

                    // Array (handled in Collection/List)

                    // Collection
                    {List.of(1, 2, 3), "[1,2,3]"},
                    {new HashSet<>(List.of(1, 2, 3)), "[1,2,3]"},
                    {new TreeSet<>(List.of(3, 1, 2)), "[1,2,3]"},

                    // Stream
                    {Stream.of(Optional.empty(), Optional.of(1), Optional.of("str"), Optional.of(true), Optional.of(new RecordPerson("Alice", 30, null))), "[null,1,\"str\",true,{\"name\":\"Alice\",\"age\":30}]"},

                    // Map
                    {Map.of("key", "value"), "{\"key\":\"value\"}"},
                    // special map keys
                    {Map.of(1, "one"), "{\"1\":\"one\"}"},
                    {Map.of(true, "yes"), "{\"true\":\"yes\"}"},
                    {new HashMap<>() {{ put(null, "null"); }}, "{\"null\":\"null\"}"},
                    {Map.of(3.14, "pi"), "{\"3.14\":\"pi\"}"},
                    {Map.of(LocalDate.parse("2024-01-01"), "New Year"), "{\"2024-01-01\":\"New Year\"}"},

                    // Enum (tested in other tests)

                    // Temporal types
                    // Date, Instant, Timestamp
                    {Date.from(Instant.parse("2024-03-15T10:15:30Z")), "\"2024-03-15T10:15:30Z\""},
                    {Instant.parse("2024-03-15T10:15:30Z"), "\"2024-03-15T10:15:30Z\""},
                    {Timestamp.from(Instant.parse("2024-03-15T10:15:30Z")), "\"2024-03-15T10:15:30Z\""},
                    // LocalDate, LocalTime, LocalDateTime
                    {LocalDate.parse("2023-01-01"), "\"2023-01-01\""},
                    {LocalTime.parse("09:00:30"), "\"09:00:30\""},
                    {LocalDateTime.parse("2024-01-01T09:00:30"), "\"2024-01-01T09:00:30\""},
                    // ZonedDateTime, OffsetDateTime
                    {ZonedDateTime.parse("2024-01-01T09:00:00+08:00[Asia/Shanghai]"), "\"2024-01-01T09:00+08:00[Asia/Shanghai]\""},
                    {OffsetDateTime.parse("2024-01-01T09:00:00+08:00"), "\"2024-01-01T09:00+08:00\""},
                    // Duration, Year, YearMonth, MonthDay, Period, ZoneOffset, ZoneId
                    {Duration.ofHours(2), "\"PT2H\""},
                    {Year.of(2024), "\"2024\""},
                    {YearMonth.of(2024, 3), "\"2024-03\""},
                    {MonthDay.of(12, 25), "\"--12-25\""},
                    {Period.ofDays(5), "\"P5D\""},
                    {ZoneOffset.ofHours(8), "\"+08:00\""},
                    {ZoneId.of("Asia/Shanghai"), "\"Asia/Shanghai\""},

                    // String-based types
                    // java.util types
                    {UUID.fromString("550e8400-e29b-41d4-a716-446655440000"), "\"550e8400-e29b-41d4-a716-446655440000\""},
                    {Locale.US, "\"en-US\""},
                    {Currency.getInstance("USD"), "\"USD\""},
                    {TimeZone.getTimeZone("Asia/Shanghai"), "\"Asia/Shanghai\""},
                    // java.net types
                    {URI.create("https://example.com"), "\"https://example.com\""},
                    {URI.create("https://example.com").toURL(), "\"https://example.com\""},
                    // java.util.regex.Pattern
                    {Pattern.compile("[a-z]+"), "\"[a-z]+\""},

                    // Record/Bean
                    {new RecordPerson("Alice", 30, LocalDate.parse("1993-05-15")), "{\"name\":\"Alice\",\"age\":30,\"birthDate\":\"1993-05-15\"}"},
                    {new ClassPerson() {{ setName("Bob"); setAge(25); setBirthDate(LocalDate.parse("1998-10-20")); }}, "{\"age\":25,\"birthDate\":\"1998-10-20\",\"name\":\"Bob\"}"},
                    {new Object(), "{}"},

                    // Protobuf support (codec)
                    {User.newBuilder().setId(1).setName("Freeman").setStatus(User.Status.ACTIVE).addAllHobbyList(List.of(Hobby.newBuilder().setId(1).setName("gaming").build())).putAllHobbyMap(Map.of(1L, Hobby.newBuilder().setId(1).setName("gaming").build())).build(), "{\"id\":1,\"name\":\"Freeman\",\"status\":\"ACTIVE\",\"hobbyList\":[{\"id\":1,\"name\":\"gaming\"}],\"hobbyMap\":{\"1\":{\"id\":1,\"name\":\"gaming\"}}}"},
                    {User.newBuilder().build(), "{\"id\":0,\"name\":\"\",\"status\":\"STATUS_UNSPECIFIED\",\"hobbyList\":[],\"hobbyMap\":{}}"},
                    {Any.pack(Timestamps.parse("2025-10-19T03:29:19.500Z")), "{\"@type\":\"type.googleapis.com/google.protobuf.Timestamp\",\"value\":\"2025-10-19T03:29:19.500Z\"}"},
                    {Any.pack(User.newBuilder().setId(1).setName("Freeman").build()), "{\"@type\":\"type.googleapis.com/json4j.user.User\",\"id\":1,\"name\":\"Freeman\",\"status\":\"STATUS_UNSPECIFIED\",\"hobbyList\":[],\"hobbyMap\":{}}"}
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

            // @spotless:off
            var table = new Object[][] {
                {new Foo(Optional.of("Alice"), Optional.of(30)), "{\"name\":\"Alice\",\"age\":30}"},
                {new Foo(Optional.of("Bob"), Optional.empty()), "{\"name\":\"Bob\"}"},
                {new Foo(Optional.empty(), Optional.of(25)), "{\"age\":25}"},
                {new Foo(Optional.empty(), Optional.empty()), "{}"},
                {new Foo(null, Optional.empty()), "{}"}, // null Optional treated as empty
            };
            // @spotless:on

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
        void parse() throws Exception {
            // @spotless:off
            var table = new Object[][] {
                    // Object type (dynamic/untyped)
                    {"{\"key1\":\"value1\",\"key2\":[1,true,1.01]}", new Json.Type<Object>() {}, Map.of("key1", "value1", "key2", List.of(1, true, 1.01))},

                    // null handling
                    {"null", new Json.Type<RecordPerson>() {}, null},
                    {"null", new Json.Type<String>() {}, null},
                    {"null", new Json.Type<Optional<?>>() {}, Optional.empty()},

                    // boolean (LOOSE)
                    {"false", Json.Type.of(Boolean.class), false},
                    {"true", Json.Type.of(Boolean.class), true},
                    // Loose: from number
                    {"1", new Json.Type<Boolean>() {}, true},
                    {"0", new Json.Type<Boolean>() {}, false},
                    // Loose: from string
                    {"\"true\"", new Json.Type<Boolean>() {}, true},
                    {"\"false\"", new Json.Type<Boolean>() {}, false},

                    // number (LOOSE)
                    {"7", Json.Type.of(Integer.class), 7},
                    {"42", new Json.Type<>() {}, 42},
                    {"10000000000", new Json.Type<>() {}, 10000000000L},
                    {"9999999999999999999999999", new Json.Type<>() {}, new BigInteger("9999999999999999999999999")},
                    {"3.14", new Json.Type<>() {}, 3.14},
                    {"1.0000000000000001", new Json.Type<>() {}, new BigDecimal("1.0000000000000001")},
                    // Loose: from string
                    {"\"123.4\"", new Json.Type<Double>() {}, 123.4},
                    {"\"10000000000\"", new Json.Type<Long>() {}, 10000000000L},

                    // String/CharSequence (LOOSE - can stringify any JsonValue)
                    {"\"str\"", Json.Type.of(String.class), "str"},
                    // Loose: from number
                    {"7", Json.Type.of(String.class), "7"},
                    // Loose: from number/boolean
                    {"1", new Json.Type<String>() {}, "1"},

                    // char/Character (LOOSE)
                    {"0", new Json.Type<Character>() {}, '0'},

                    // enum (case-insensitive name or ordinal)
                    {"\"MONDAY\"", new Json.Type<DayOfWeek>() {}, DayOfWeek.MONDAY}, // exact match
                    {"\"monday\"", new Json.Type<DayOfWeek>() {}, DayOfWeek.MONDAY}, // case-insensitive
                    {"0", new Json.Type<DayOfWeek>() {}, DayOfWeek.MONDAY}, // ordinal

                    // temporal (ISO-8601 string or epoch millis)
                    // Date, Instant, Timestamp
                    {"\"2024-03-15T10:15:30Z\"", new Json.Type<Date>() {}, Date.from(Instant.parse("2024-03-15T10:15:30Z"))},
                    {"\"2024-03-15T10:15:30Z\"", new Json.Type<Instant>() {}, Instant.parse("2024-03-15T10:15:30Z")},
                    {"\"2024-03-15T10:15:30Z\"", new Json.Type<Timestamp>() {}, Timestamp.from(Instant.parse("2024-03-15T10:15:30Z"))},
                    // Duration, Period
                    {"\"PT2H\"", new Json.Type<Duration>() {}, Duration.ofHours(2)},
                    {"\"P5D\"", new Json.Type<Period>() {}, Period.ofDays(5)},
                    // LocalDate, LocalTime, LocalDateTime
                    {"\"2025-10-10\"", new Json.Type<LocalDate>() {}, LocalDate.parse("2025-10-10")},
                    {"\"09:00:30\"", new Json.Type<LocalTime>() {}, LocalTime.parse("09:00:30")},
                    {"\"2024-01-01T09:00:30\"", new Json.Type<LocalDateTime>() {}, LocalDateTime.parse("2024-01-01T09:00:30")},
                    // ZonedDateTime, OffsetDateTime
                    {"\"2024-01-01T09:00+08:00[Asia/Shanghai]\"", new Json.Type<ZonedDateTime>() {}, ZonedDateTime.parse("2024-01-01T09:00+08:00[Asia/Shanghai]")},
                    {"\"2024-01-01T09:00+08:00\"", new Json.Type<OffsetDateTime>() {}, OffsetDateTime.parse("2024-01-01T09:00+08:00")},
                    // Year, YearMonth, MonthDay
                    {"\"2024\"", new Json.Type<Year>() {}, Year.of(2024)},
                    {"2024", new Json.Type<Year>() {}, Year.of(2024)}, // number support for Year
                    {"\"2024-03\"", new Json.Type<YearMonth>() {}, YearMonth.of(2024, 3)},
                    {"\"--12-25\"", new Json.Type<MonthDay>() {}, MonthDay.of(12, 25)},
                    // ZoneOffset, ZoneId
                    {"\"+08:00\"", new Json.Type<ZoneOffset>() {}, ZoneOffset.ofHours(8)},
                    {"\"Asia/Shanghai\"", new Json.Type<ZoneId>() {}, ZoneId.of("Asia/Shanghai")},

                    // string-based types (UUID, Locale, Currency, TimeZone, URI, URL, Path, Pattern)
                    // java.util types
                    {"\"550e8400-e29b-41d4-a716-446655440000\"", new Json.Type<UUID>() {}, UUID.fromString("550e8400-e29b-41d4-a716-446655440000")},
                    {"\"en-US\"", new Json.Type<Locale>() {}, Locale.US},
                    {"\"USD\"", new Json.Type<Currency>() {}, Currency.getInstance("USD")},
                    {"\"Asia/Shanghai\"", new Json.Type<TimeZone>() {}, TimeZone.getTimeZone("Asia/Shanghai")},
                    // java.net types
                    {"\"https://example.com\"", new Json.Type<URI>() {}, URI.create("https://example.com")},
                    {"\"https://example.com\"", new Json.Type<URL>() {}, URI.create("https://example.com").toURL()},
                    // java.util.regex.Pattern - removed due to Pattern not implementing equals()

                    // Optional
                    {"\"str\"", new Json.Type<Optional<String>>() {}, Optional.of("str")},
                    {"1", new Json.Type<Optional<String>>() {}, Optional.of("1")},
                    {"1", new Json.Type<Optional<Boolean>>() {}, Optional.of(true)},
                    {"null", new Json.Type<Optional<String>>() {}, Optional.empty()},

                    // Atomic types (note: will need special handling in test since atomic types don't implement equals)

                    // arrays (if non-array provided, wrap single element)
                    {"\"2025-01-01\"", new Json.Type<LocalDate[]>() {}, new LocalDate[] {LocalDate.parse("2025-01-01")}},

                    // Collection (if non-array provided, wrap single element)
                    {"[]", new Json.Type<List<RecordPerson>>() {}, List.of()},
                    {"[1,2,3]", new Json.Type<Iterable<Integer>>() {}, List.of(1, 2, 3)},
                    {"[[1,2],[3,4]]", new Json.Type<List<List<Integer>>>() {}, List.of(List.of(1, 2), List.of(3, 4))},
                    {"[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25,\"birthDate\":\"2025-10-10\"}]", new Json.Type<List<RecordPerson>>() {}, List.of(new RecordPerson("Alice", 30, null), new RecordPerson("Bob", 25, LocalDate.parse("2025-10-10")))},
                    // Set types
                    {"[1,2,3]", new Json.Type<HashSet<Integer>>() {}, new HashSet<>(List.of(1, 2, 3))},
                    {"[3,1,2]", new Json.Type<TreeSet<Integer>>() {}, new TreeSet<>(List.of(3, 1, 2))},
                    {"[1,2,3]", new Json.Type<Set<Integer>>() {}, Set.of(1, 2, 3)},
                    // Loose: wrap single element
                    {"\"2025-01-01\"", new Json.Type<List<LocalDate>>() {}, List.of(LocalDate.parse("2025-01-01"))},

                    // Map
                    {"{}", new Json.Type<Map<String, RecordPerson>>() {}, Map.of()},
                    {"{\"left\":1,\"right\":2}", new Json.Type<Map<String, Integer>>() {}, Map.of("left", 1, "right", 2)},
                    {"{\"group1\":[{\"name\":\"Alice\",\"age\":30}],\"group2\":[{\"name\":\"Bob\",\"age\":25}]}", new Json.Type<Map<String, List<RecordPerson>>>() {}, Map.of("group1", List.of(new RecordPerson("Alice", 30, null)), "group2", List.of(new RecordPerson("Bob", 25, null)))},
                    // special map keys (coercion)
                    {"{\"1\":\"one\"}", new Json.Type<Map<Integer, String>>() {}, Map.of(1, "one")},
                    {"{\"true\":\"yes\"}", new Json.Type<Map<Boolean, String>>() {}, Map.of(true, "yes")},
                    {"{\"3.14\":\"pi\"}", new Json.Type<Map<Double, String>>() {}, Map.of(3.14, "pi")},
                    {"{\"2024-01-01\":\"New Year\"}", new Json.Type<Map<LocalDate, String>>() {}, Map.of(LocalDate.parse("2024-01-01"), "New Year")},

                    // Record
                    {"{\"name\":\"Alice\",\"age\":30}", new Json.Type<RecordPerson>() {}, new RecordPerson("Alice", 30, null)},
                    // Loose parsing snake_case to camelCase
                    {"{\"name\":\"Alice\",\"birth_date\":\"1993-05-15\"}", new Json.Type<RecordPerson>() {}, new RecordPerson("Alice", 0, LocalDate.parse("1993-05-15"))},

                    // Bean
                    // Loose parsing snake_case to camelCase
                    {"{}", new Json.Type<Object>(){}, new LinkedHashMap<>()},
                    {"{\"name\":\"Bob\",\"birth_date\":\"1998-10-20\"}", new Json.Type<ClassPerson>() {}, new ClassPerson() {{ setName("Bob"); setBirthDate(LocalDate.parse("1998-10-20")); }}},

                    // Protobuf support (codec)
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
                    {"[{\"id\":1,\"name\":\"Freeman\"}]", new Json.Type<ListValue>() {}, ListValue.newBuilder().addValues(Value.newBuilder().setStructValue(Struct.newBuilder().putFields("id", Value.newBuilder().setNumberValue(1).build()).putFields("name", Value.newBuilder().setStringValue("Freeman").build()).build()).build()).build()},
                    {"{\"@type\":\"type.googleapis.com/google.protobuf.Timestamp\",\"value\":\"2025-10-19T03:29:19.500Z\"}", new Json.Type<Any>() {}, Any.pack(Timestamps.parse("2025-10-19T03:29:19.500Z"))},
                    {"{\"@type\":\"type.googleapis.com/json4j.user.User\",\"id\":1,\"name\":\"Freeman\",\"status\":\"STATUS_UNSPECIFIED\",\"hobbyList\":[],\"hobbyMap\":{}}", new Json.Type<Any>() {}, Any.pack(User.newBuilder().setId(1).setName("Freeman").build())}
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
        void parseAtomicTypes() {
            var table = new Object[][] {
                {"42", new Json.Type<AtomicInteger>() {}, 42},
                {"10000000000", new Json.Type<AtomicLong>() {}, 10000000000L},
                {"true", new Json.Type<AtomicBoolean>() {}, true},
                {"\"hello\"", new Json.Type<AtomicReference<String>>() {}, "hello"},
                {"null", new Json.Type<AtomicReference<String>>() {}, null}
            };

            assertAll(IntStream.range(0, table.length).mapToObj(i -> () -> {
                var row = table[i];
                var input = (String) row[0];
                var type = (Json.Type<?>) row[1];
                var expected = row[2];
                var actual = invoke(Json.parse(input, type), "get");
                assertThat(actual)
                        .as("Case %d: input=%s, type=%s", i, input, type)
                        .isEqualTo(expected);
            }));
        }

        record Foo(Optional<String> name, Optional<Integer> age) {}

        @Test
        void parseRecordOptionalProperties() {
            // @spotless:off
            var table = new Object[][] {
                {"{\"name\":\"Alice\",\"age\":30}", new Foo(Optional.of("Alice"), Optional.of(30))},
                {"{\"name\":\"Bob\"}", new Foo(Optional.of("Bob"), Optional.empty())},
                {"{\"age\":25}", new Foo(Optional.empty(), Optional.of(25))},
                {"{}", new Foo(Optional.empty(), Optional.empty())},
                {"{\"name\":null,\"age\":null}", new Foo(Optional.empty(), Optional.empty())}
            };
            // @spotless:on

            assertAll(IntStream.range(0, table.length).mapToObj(i -> () -> {
                var row = table[i];
                var input = (String) row[0];
                var expected = (Foo) row[1];
                var actual = Json.parse(input, new Json.Type<Foo>() {});
                assertThat(actual).as("Case %d: input=%s", i, input).isEqualTo(expected);
            }));
        }

        @Data
        static class Bar {
            private final Optional<String> name;
            private final Optional<Integer> age;
        }

        @Test
        void parseBeanOptionalProperties() {
            // @spotless:off
            var table2 = new Object[][] {
                    {"{\"name\":\"Alice\",\"age\":30}", new Bar(Optional.of("Alice"), Optional.of(30))},
                    {"{\"name\":\"Bob\"}", new Bar(Optional.of("Bob"), Optional.empty())},
                    {"{\"age\":25}", new Bar(Optional.empty(), Optional.of(25))},
                    {"{}", new Bar(Optional.empty(), Optional.empty())},
                    {"{\"name\":null,\"age\":null}", new Bar(Optional.empty(), Optional.empty())}
            };
            // @spotless:on

            assertAll(IntStream.range(0, table2.length).mapToObj(i -> () -> {
                var row = table2[i];
                var input = (String) row[0];
                var expected = (Bar) row[1];
                var actual = Json.parse(input, new Json.Type<Bar>() {});
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

    @SuppressWarnings("unchecked")
    private static <T> T invoke(Object o, String methodName, Object... args) {
        var paramTypes = Stream.of(args).map(Object::getClass).toArray(Class[]::new);
        try {
            var method = o.getClass().getDeclaredMethod(methodName, paramTypes);
            method.setAccessible(true);
            return (T) method.invoke(o, args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
